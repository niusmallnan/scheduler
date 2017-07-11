package scheduler

import (
	"fmt"
	"sync"

	"time"

	"reflect"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/go-rancher/v2"
)

const (
	instancePool            = "instanceReservation"
	memoryPool              = "memoryReservation"
	cpuPool                 = "cpuReservation"
	storageSize             = "storageSize"
	portPool                = "portReservation"
	totalAvailableInstances = 1000000
	hostLabels              = "hostLabels"
	computePool             = "computePool"
	portPoolType            = "portPool"
	instanceReservation     = "instanceReservation"
	labelPool               = "labelPool"
	defaultIP               = "0.0.0.0"
	ipLabel                 = "io.rancher.scheduler.ips"
)

type host struct {
	id    string
	pools map[string]ResourcePool
}

func NewScheduler(sleepTime int) *Scheduler {
	initialized := false
	if sleepTime < 0 {
		initialized = true
	}
	return &Scheduler{
		hosts:       map[string]*host{},
		sleepTime:   sleepTime,
		initialized: initialized,
	}
}

type Scheduler struct {
	mu          sync.RWMutex
	hosts       map[string]*host
	sleepTime   int
	initialized bool
	mdClient    metadata.Client
	rclient     *client.RancherClient
	knownHosts  map[string]bool
	//lock for initialization update
	iniMu       sync.RWMutex
	lastEventMu sync.Mutex
	lastEvent   time.Time
	globalMu    sync.RWMutex
}

func (s *Scheduler) PrioritizeCandidates(resourceRequests []ResourceRequest, context Context) ([]string, error) {
	s.globalMu.RLock()
	defer s.globalMu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	defer s.setLastEvent()

	filteredHosts := []string{}
	for host := range s.hosts {
		filteredHosts = append(filteredHosts, host)
	}

	filters := getFilters()
	for _, filter := range filters {
		filteredHosts = filter.Filter(s, resourceRequests, context, filteredHosts)
	}
	filteredHosts = sortHosts(s, resourceRequests, context, filteredHosts)
	return filteredHosts, nil
}

func (s *Scheduler) ReserveResources(hostID string, force bool, resourceRequests []ResourceRequest, context Context) (map[string]interface{}, error) {
	s.globalMu.RLock()
	defer s.globalMu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	defer s.setLastEvent()

	logrus.Infof("Reserving %+v for %v. Force=%v", resourceRequests, hostID, force)
	h, ok := s.hosts[hostID]
	if !ok {
		// If the host isn't present, it is most likely that it hasn't been registered with the scheduler yet.
		// When it is, this reservation will get counted by the initial population.
		logrus.Warnf("Host %v not found for reserving %v. Skipping reservation", hostID, resourceRequests)
		return nil, nil
	}

	reserveActions := getReserveActions()
	data := map[string]interface{}{}

	executedActions := []ReserveAction{}

	for _, action := range reserveActions {
		err := action.Reserve(s, resourceRequests, context, h, force, data)
		executedActions = append(executedActions, action)
		if err != nil {
			logrus.Error("Error happens in reserving resource. Rolling back the reservation")
			// rollback previous reserve actions
			for _, exeAction := range executedActions {
				exeAction.RollBack(s, resourceRequests, context, h)
			}
			return nil, err
		}
	}
	return data, nil
}

func (s *Scheduler) ReleaseResources(hostID string, resourceRequests []ResourceRequest, context Context) error {
	s.globalMu.RLock()
	defer s.globalMu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	defer s.setLastEvent()

	logrus.Infof("Releasing %+v for %v", resourceRequests, hostID)
	h, ok := s.hosts[hostID]
	if !ok {
		logrus.Infof("Host %v not found for releasing %v. Nothing to do.", hostID, resourceRequests)
		return nil
	}
	releaseActions := getReleaseActions()

	for _, rAction := range releaseActions {
		rAction.Release(s, resourceRequests, context, h)
	}
	return nil
}

func (s *Scheduler) CreateResourcePool(hostUUID string, pool ResourcePool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.hosts[hostUUID]
	if !ok {
		h = &host{
			pools: map[string]ResourcePool{},
			id:    hostUUID,
		}
		s.hosts[hostUUID] = h
	}

	if _, ok := h.pools[pool.GetPoolResourceType()]; ok {
		return fmt.Errorf("Pool %v already exists on host %v", pool.GetPoolResourceType(), hostUUID)
	}

	pool.Create(h)

	return nil
}

func (s *Scheduler) UpdateResourcePool(hostUUID string, pool ResourcePool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.hosts[hostUUID]
	if !ok {
		return false
	}

	_, ok = h.pools[pool.GetPoolResourceType()]
	if !ok {
		return false
	}

	pool.Update(h)

	return true
}

func (s *Scheduler) CheckResourcePool(hostUUID string, resourceType string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.hosts[hostUUID]
	if !ok {
		return false
	}

	_, ok = h.pools[resourceType]
	if !ok {
		return false
	}

	return true
}

func (s *Scheduler) RemoveHost(hostUUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.Infof("Removing host %v.", hostUUID)
	delete(s.hosts, hostUUID)
}

func (s *Scheduler) CompareHostLabels(hosts []metadata.Host) bool {
	if len(s.hosts) != len(hosts) {
		return true
	}
	for _, host := range hosts {
		originalHost, ok := s.hosts[host.UUID]
		if !ok {
			return true
		}
		prevMap := originalHost.pools[hostLabels].(*LabelPool)
		currMap := host.Labels
		if !reflect.DeepEqual(prevMap.Labels, currMap) {
			return true
		}
	}
	return false
}

func (s *Scheduler) UpdateWithMetadata(force bool) (bool, error) {
	// if scheduler is not initialized or is updated by force, trigger the update logic
	s.iniMu.Lock()
	defer s.iniMu.Unlock()

	// After we're initialized, don't perform the sync if the an event has come in in the last two seconds.
	// Scheduling is bursty, so this mitigates performing the sync during a scheduling burst.
	// Syncing and handling events at the same time is ok, but avoiding it is better.
	if s.initialized {
		check := s.getLastEvent().Add(time.Second * 5)
		now := time.Now()
		if check.After(now) || check.Equal(now) {
			return false, nil
		}
	}

	if !s.initialized || force {
		s.globalMu.Lock()
		defer s.globalMu.Unlock()
		hosts, err := s.mdClient.GetHosts()
		if err != nil {
			return false, err
		}

		containers, err := s.mdClient.GetContainers()
		if err != nil {
			return false, err
		}

		currentHostDeploymentsMap := map[string][]string{}
		for _, c := range containers {
			dm, ok := c.Labels[deploymentUnitLabel]
			if !ok {
				continue
			}
			currentHostDeploymentsMap[c.HostUUID] = removeDuplicates(append(currentHostDeploymentsMap[c.HostUUID], dm))
		}

		usedResourcesByHost, err := GetUsedResourcesByHost(s.mdClient)
		if err != nil {
			return false, err
		}
		newKnownHosts := map[string]bool{}

		for _, h := range hosts {
			newKnownHosts[h.UUID] = true
			delete(s.knownHosts, h.UUID)

			poolInits := map[string]int64{
				instancePool: totalAvailableInstances,
				cpuPool:      h.MilliCPU,
				memoryPool:   h.Memory,
				storageSize:  h.LocalStorageMb,
			}

			for resourceKey, total := range poolInits {
				// Update totals available, not amount used
				poolDoesntExist := !s.UpdateResourcePool(h.UUID, &ComputeResourcePool{
					Resource:  resourceKey,
					Used:      usedResourcesByHost[h.UUID][resourceKey],
					Total:     total,
					UpdateAll: true,
				})
				if poolDoesntExist {
					usedResource := usedResourcesByHost[h.UUID][resourceKey]
					if err := s.CreateResourcePool(h.UUID, &ComputeResourcePool{Resource: resourceKey, Total: total, Used: usedResource}); err != nil {
						logrus.Panicf("Received an error creating resource pool. This shouldn't have happened. Error: %v.", err)
					}
				}
			}

			portPool, err := GetPortPoolFromHost(h, s.mdClient)
			if err != nil {
				return false, err
			}
			portPool.ShouldUpdate = true
			poolDoesntExist := !s.UpdateResourcePool(h.UUID, portPool)
			if poolDoesntExist {
				s.CreateResourcePool(h.UUID, portPool)
			}
			// updating label pool
			labelPool := &LabelPool{
				Resource: hostLabels,
				Labels:   h.Labels,
			}
			poolDoesntExist = !s.UpdateResourcePool(h.UUID, labelPool)
			if poolDoesntExist {
				s.CreateResourcePool(h.UUID, labelPool)
			}

			curDuPool := &DeploymentUnitPool{
				Resource:    currentDeploymentUnitPool,
				Deployments: currentHostDeploymentsMap[h.UUID],
			}
			poolDoesntExist = !s.UpdateResourcePool(h.UUID, curDuPool)
			if poolDoesntExist {
				s.CreateResourcePool(h.UUID, curDuPool)
			}

		}

		for uuid := range s.knownHosts {
			s.RemoveHost(uuid)
		}

		s.knownHosts = newKnownHosts
		if !force {
			s.initialized = true
		}
	}
	return true, nil
}

func (s *Scheduler) GetMetadataClient() metadata.Client {
	return s.mdClient
}

func (s *Scheduler) SetMetadataClient(client metadata.Client) {
	s.mdClient = client
}

func (s *Scheduler) SetAPIClient(client *client.RancherClient) {
	s.rclient = client
}

func (s *Scheduler) getContainerIPByAPI(deploymentUnitUUID string) (string, error) {
	listOpts := client.NewListOpts()
	listOpts.Filters["deploymentUnitUuid"] = deploymentUnitUUID
	collection, err := s.rclient.Container.List(listOpts)
	if err != nil {
		logrus.Errorf("getContainerIPByAPI: failed to request rancher-api: %v", err)
		return "", err
	}
	for _, c := range collection.Data {
		if c.PrimaryIpAddress != "" {
			logrus.Debugf("getContainerIPByAPI: deploymentUnitUUID, %s PrimaryIpAddress, %s", deploymentUnitUUID, c.PrimaryIpAddress)
			return c.PrimaryIpAddress, nil
		}
	}
	return "", nil
}

func (s *Scheduler) reserveTempPool(hostID string, requests []ResourceRequest) {
	if s.sleepTime >= 0 {
		for _, rr := range requests {
			if computeReq, ok := rr.(AmountBasedResourceRequest); ok {
				pool := s.hosts[hostID].pools[computeReq.Resource].(*ComputeResourcePool)
				if pool.Resource == instanceReservation {
					pool.Used += computeReq.Amount
					go func(amount int64, t int) {
						time.Sleep(time.Second * time.Duration(t))
						s.mu.Lock()
						pool.Used -= amount
						s.mu.Unlock()
					}(computeReq.Amount, s.sleepTime)
				}
			}
		}
	}
}

func (s *Scheduler) setLastEvent() {
	s.lastEventMu.Lock()
	defer s.lastEventMu.Unlock()
	s.lastEvent = time.Now()
}

func (s *Scheduler) getLastEvent() time.Time {
	s.lastEventMu.Lock()
	defer s.lastEventMu.Unlock()
	le := s.lastEvent // Get a copy while under the lock
	return le
}
