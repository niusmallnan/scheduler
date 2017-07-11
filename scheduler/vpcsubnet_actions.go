package scheduler

import (
	"net"

	"github.com/Sirupsen/logrus"
)

const (
	vpcSubnetLabel            = "io.rancher.vpc.subnet"
	deploymentUnitLabel       = "io.rancher.service.deployment.unit"
	tempDeploymentUnitPool    = "tempDeploymentUnitPool"
	currentDeploymentUnitPool = "currentDeploymentUnitPool"
)

type VpcSubnetFilter struct {
}

func (v VpcSubnetFilter) Filter(scheduler *Scheduler, resourceRequest []ResourceRequest, context Context, hosts []string) []string {
	logrus.Debugf("Filter context: %+v", context)
	var matchHost string
breakLabel:
	for _, host := range hosts {
		lpool, ok := scheduler.hosts[host].pools["hostLabels"]
		if !ok {
			continue
		}
		subnet, ok := lpool.(*LabelPool).Labels[vpcSubnetLabel]
		if !ok || subnet == "" {
			continue
		}
		_, ipnet, err := net.ParseCIDR(subnet)
		if err != nil {
			continue
		}

		for _, con := range context {
			logrus.Debugf("Get Contxt deploymentUnitUUID: %s", con.DeploymentUnitUUID)
			containerIP, err := scheduler.getContainerIPByAPI(con.DeploymentUnitUUID)
			if err != nil || containerIP == "" {
				continue
			}
			if ipnet.Contains(net.ParseIP(containerIP)) {
				matchHost = host
				break breakLabel
			}
		}
	}
	if matchHost != "" {
		logrus.Debugf("VpcSubnetFilter match host: %s", matchHost)
		return []string{matchHost}
	}
	return hosts
}

type VpcSubnetReleaseAction struct{}

func (v VpcSubnetReleaseAction) Release(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host) {
	logrus.Debugf("Release context: %+v", context)
	if context != nil && len(context) > 0 {
		dPool, ok := scheduler.hosts[host.id].pools[tempDeploymentUnitPool]
		if !ok {
			logrus.Infof("Host %v doesn't have resource pool tempDeploymentUnitPool. Nothing to do.", host.id)
			return
		}
		pool := dPool.(*DeploymentUnitPool)
		for _, cont := range context {
			deployments := append(pool.Deployments, cont.DeploymentUnitUUID)
			deployments = removeDuplicates(deployments)
			pool.Deployments = deployments
		}
		logrus.Debugf("Host:%s DeploymentUnitPool: %v", host.id, scheduler.hosts[host.id].pools[tempDeploymentUnitPool])
	}
}

type VpcSubnetReserveAction struct{}

func (v *VpcSubnetReserveAction) Reserve(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host, force bool, data map[string]interface{}) error {
	logrus.Debugf("Reserve context: %+v", context)
	if context != nil && len(context) > 0 {
		dPool, ok := scheduler.hosts[host.id].pools[tempDeploymentUnitPool]
		if !ok {
			logrus.Infof("Host %v doesn't have resource pool tempDeploymentUnitPool. Nothing to do.", host.id)
			return nil
		}
		deployments := dPool.(*DeploymentUnitPool).Deployments
		matchIndex := -1
	breakLabel:
		for index, deployment := range deployments {
			for _, con := range context {
				logrus.Debugf("Get Contxt deploymentUnitUUID: %s", con.DeploymentUnitUUID)
				if con.DeploymentUnitUUID == deployment {
					matchIndex = index
					break breakLabel
				}
			}
		}
		if matchIndex >= 0 {
			dPool.(*DeploymentUnitPool).Deployments = append(deployments[:matchIndex], deployments[matchIndex+1:]...)
		}
		logrus.Debugf("Host:%s DeploymentUnitPool: %v", host.id, scheduler.hosts[host.id].pools[tempDeploymentUnitPool])
	}
	return nil
}

func (v *VpcSubnetReserveAction) RollBack(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host) {
}
