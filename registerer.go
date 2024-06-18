package frostdb

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// reusableRegistry is a wrapper on top of a prometheus registry that allows
// metrics to be registered multiple times. This has been specifically designed
// for use cases where a table or db is dropped then recreated with the same
// name. If a new collector with the same labels is registered, that metric is
// reset to 0.
type reusableRegistry struct {
	internalReg prometheus.Registerer

	protected struct {
		sync.Mutex
		registered map[string]struct{}
	}
}

var _ prometheus.Registerer = (*reusableRegistry)(nil)

func newReusableRegistry(reg prometheus.Registerer) *reusableRegistry {
	r := &reusableRegistry{
		internalReg: reg,
	}
	r.protected.registered = make(map[string]struct{})
	return r
}

func (r *reusableRegistry) Register(c prometheus.Collector) error {
	d := make(chan *prometheus.Desc, 1)
	c.Describe(d)
	desc := <-d

	r.protected.Lock()
	defer r.protected.Unlock()
	metricDesc := desc.String()
	if _, ok := r.protected.registered[metricDesc]; ok {
		_ = r.internalReg.Unregister(c)
	} else {
		r.protected.registered[metricDesc] = struct{}{}
	}
	return r.internalReg.Register(c)
}

func (r *reusableRegistry) MustRegister(collectors ...prometheus.Collector) {
	for _, c := range collectors {
		if err := r.Register(c); err != nil {
			panic(err)
		}
	}
}

func (r *reusableRegistry) Unregister(c prometheus.Collector) bool {
	d := make(chan *prometheus.Desc, 1)
	c.Describe(d)
	desc := <-d

	r.protected.Lock()
	defer r.protected.Unlock()
	metricDesc := desc.String()
	delete(r.protected.registered, metricDesc)
	return r.internalReg.Unregister(c)
}
