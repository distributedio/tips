package metrics

import (
	"net/http"
	"testing"
)

const (
	defaultLabel = "test"
	defaultlabel = "label"
)

func TestMetrics(t *testing.T) {

	go func() {
		http.ListenAndServe(":8888", nil)
	}()

	gm.LogMetricsCounterVec.WithLabelValues("INFO").Inc()
}
