package metrics

import (
	"net/http"

	"go.uber.org/zap/zapcore"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	//promethus default namespace
	namespace = "tips"

	//promethues default label key
	opt       = "opt"
	leader    = "leader"
	labelName = "level"
	gckeys    = "gckeys"
)

var (
	//Label value slice when creating prometheus object
	optLabel    = []string{opt}
	leaderLabel = []string{leader}
	gcKeysLabel = []string{gckeys}

	// global prometheus object
	gm *Metrics
)

//Metrics prometheus statistics
type Metrics struct {
	//command biz
	TxnCommitHistogramVec *prometheus.HistogramVec
	TxnFailuresCounterVec *prometheus.CounterVec

	//biz
	TopicCounterVec        *prometheus.CounterVec
	SubscribtionCounterVec *prometheus.CounterVec
	SnapshotCounterVec     *prometheus.CounterVec

	//logger
	LogMetricsCounterVec *prometheus.CounterVec
}

//init create global object
func init() {
	gm = &Metrics{}

	gm.TopicCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "txn_retries_total",
			Help:      "The total of txn retries",
		}, optLabel)
	prometheus.MustRegister(gm.TopicCounterVec)

	gm.SnapshotCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "txn_conflicts_total",
			Help:      "The total of txn conflicts",
		}, optLabel)
	prometheus.MustRegister(gm.SnapshotCounterVec)

	gm.SubscribtionCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "txn_failures_total",
			Help:      "The total of txn failures",
		}, optLabel)
	prometheus.MustRegister(gm.SubscribtionCounterVec)

	gm.TxnCommitHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "txn_commit_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
			Help:      "The cost times of txn commit",
		}, optLabel)
	prometheus.MustRegister(gm.TxnCommitHistogramVec)

	gm.LogMetricsCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "logs_entries_total",
			Help:      "Number of logs of certain level",
		},
		[]string{labelName},
	)
	prometheus.MustRegister(gm.LogMetricsCounterVec)

	http.Handle("/tips/metrics", prometheus.Handler())
}

//GetMetrics return metrics object
func GetMetrics() *Metrics {
	return gm
}

//Measure logger level rate
func Measure(e zapcore.Entry) error {
	label := e.LoggerName + "_" + e.Level.String()
	gm.LogMetricsCounterVec.WithLabelValues(label).Inc()
	return nil
}
