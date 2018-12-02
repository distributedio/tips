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
	optLabel    = []string{opt}
	leaderLabel = []string{leader}
	gcKeysLabel = []string{gckeys}

	gm *Metrics
)

type Metrics struct {
	//command biz
	// TxnCommitHistogramVec *prometheus.HistogramVec
	// TxnFailuresCounterVec *prometheus.CounterVec

	TopicsHistogramVec        *prometheus.HistogramVec
	SubscribtionsHistogramVec *prometheus.HistogramVec
	SnapshotsHistogramVec     *prometheus.HistogramVec
	MessagesHistogramVec      *prometheus.HistogramVec
	MessagesSizeHistogramVec  *prometheus.HistogramVec

	//logger
	LogMetricsCounterVec *prometheus.CounterVec
}

//init create global object
func init() {
	gm = &Metrics{}

	gm.TopicsHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "topics_opt_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
			Help:      "The cost times of txn topic opt",
		}, optLabel)
	prometheus.MustRegister(gm.TopicsHistogramVec)

	gm.SubscribtionsHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "subscribtions_opt_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
			Help:      "The cost times of subscribtion opt",
		}, optLabel)
	prometheus.MustRegister(gm.SubscribtionsHistogramVec)

	gm.SnapshotsHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "snapshots_opt_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
			Help:      "The cost times of snapshot opt",
		}, optLabel)
	prometheus.MustRegister(gm.SnapshotsHistogramVec)

	gm.MessagesHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "messages_opt_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
			Help:      "The cost times of message opt",
		}, optLabel)
	prometheus.MustRegister(gm.MessagesHistogramVec)

	gm.MessagesSizeHistogramVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "messages_size_seconds",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 20),
			Help:      "The size of message opt",
		}, optLabel)
	prometheus.MustRegister(gm.MessagesSizeHistogramVec)

	gm.LogMetricsCounterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "logs_entries_total",
			Help:      "Number of logs of certain level",
		},
		[]string{labelName},
	)
	prometheus.MustRegister(gm.LogMetricsCounterVec)

	http.Handle("/metrics", prometheus.Handler())
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
