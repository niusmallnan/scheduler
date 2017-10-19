package scheduler

import (
	"net"
	"strings"
)

const (
	requireAnyLabel    = "io.rancher.scheduler.require_any"
	perHostSubnetLabel = "io.rancher.network.per_host_subnet.subnet"
)

// LabelFilter define a filter based on label constraints. For example, require_any label constraints
type LabelFilter struct {
}

func (l LabelFilter) Filter(scheduler *Scheduler, resourceRequest []ResourceRequest, context Context, hosts []string) []string {
	constraints := getAllConstraints()
	qualifiedHosts := []string{}
	for _, host := range hosts {
		qualified := true
		for _, constraint := range constraints {
			if !constraint.Match(host, scheduler, context) {
				qualified = false
			}
		}
		if qualified {
			qualifiedHosts = append(qualifiedHosts, host)
		}
	}
	return qualifiedHosts
}

type Constraints interface {
	Match(string, *Scheduler, Context) bool
}

func getAllConstraints() []Constraints {
	RequireAny := RequireAnyLabelContraints{}
	RequestedIP := RequestedIPContraints{}
	return []Constraints{RequireAny, RequestedIP}
}

// RequestedIPContraints for per-host-subnet networking
// https://github.com/rancher/rancher/issues/10168
type RequestedIPContraints struct{}

func (c RequestedIPContraints) Match(host string, s *Scheduler, context Context) bool {
	p, ok := s.hosts[host].pools["hostLabels"]
	if !ok {
		return true
	}
	val, ok := p.(*LabelPool).Labels[perHostSubnetLabel]
	if !ok || val == "" {
		return true
	}
	_, ipnet, err := net.ParseCIDR(val)
	if err != nil {
		return false
	}
	containerLabels := getLabelFromContext(context)
	for _, l := range containerLabels {
		if requestedIPStr, ok := l["io.rancher.container.requested_ip"]; ok {
			return ipnet.Contains(net.ParseIP(requestedIPStr))
		}
	}

	return true
}

type RequireAnyLabelContraints struct{}

func (c RequireAnyLabelContraints) Match(host string, s *Scheduler, context Context) bool {
	p, ok := s.hosts[host].pools["hostLabels"]
	if !ok {
		return true
	}
	val, ok := p.(*LabelPool).Labels[requireAnyLabel]
	if !ok || val == "" {
		return true
	}
	labelSet := parseLabel(val)
	containerLabels := getLabelFromContext(context)
	for key, value := range labelSet {
		for _, ls := range containerLabels {
			if value == "" {
				if _, ok := ls[key]; ok {
					return true
				}
			} else {
				if ls[key] == value {
					return true
				}
			}
		}
	}
	return false
}

func parseLabel(value string) map[string]string {
	value = strings.ToLower(value)
	parts := strings.Split(value, ",")
	result := map[string]string{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		p := strings.Split(part, "=")
		if len(p) == 2 {
			result[p[0]] = p[1]
		} else if len(p) == 1 {
			result[p[0]] = ""
		}
	}
	return result
}

func getLabelFromContext(context Context) []map[string]string {
	result := []map[string]string{}
	for _, con := range context {
		lowerMap := map[string]string{}
		for key, value := range con.Data.Fields.Labels {
			lowerMap[strings.ToLower(key)] = strings.ToLower(value)
		}
		result = append(result, lowerMap)
	}
	return result
}
