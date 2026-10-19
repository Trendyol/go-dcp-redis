package metric

import (
	"github.com/Trendyol/go-dcp-redis/redis/bulk"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	RedisConnectorLatency            = "redis_connector_latency_ms"
	RedisConnectorBulkRequestLatency = "redis_connector_bulk_request_process_latency_ms"
	RedisConnectorBulkRequestSize    = "redis_connector_bulk_request_size"
	RedisConnectorRetriedCommands    = "redis_connector_retried_commands_total"
)

type MetricCollector struct {
	bulk                      *bulk.Bulk
	redisConnectorLatency     prometheus.Gauge
	redisConnectorBulkLatency prometheus.Gauge
	redisConnectorBulkSize    prometheus.Gauge
	redisConnectorRetried     *prometheus.Desc
}

func NewMetricCollector(bulk *bulk.Bulk) *MetricCollector {
	return &MetricCollector{
		bulk: bulk,
		redisConnectorLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: RedisConnectorLatency,
			Help: "Time to adding to the batch.",
		}),
		redisConnectorBulkLatency: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: RedisConnectorBulkRequestLatency,
			Help: "Time to process bulk request.",
		}),
		redisConnectorBulkSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: RedisConnectorBulkRequestSize,
			Help: "Number of commands in the last pipeline sent to Redis.",
		}),
		redisConnectorRetried: prometheus.NewDesc(
			RedisConnectorRetriedCommands,
			"Total number of Redis commands re-sent after a transient error.",
			nil, nil,
		),
	}
}

func (s *MetricCollector) Collect(ch chan<- prometheus.Metric) {
	metric := s.bulk.GetMetric()

	s.redisConnectorLatency.Set(float64(metric.ProcessLatencyMs))
	s.redisConnectorBulkLatency.Set(float64(metric.BulkRequestProcessLatencyMs))
	s.redisConnectorBulkSize.Set(float64(metric.BulkRequestSize))

	s.redisConnectorLatency.Collect(ch)
	s.redisConnectorBulkLatency.Collect(ch)
	s.redisConnectorBulkSize.Collect(ch)
	ch <- prometheus.MustNewConstMetric(s.redisConnectorRetried, prometheus.CounterValue, float64(metric.RetriedCommands))
}

func (s *MetricCollector) Describe(ch chan<- *prometheus.Desc) {
	s.redisConnectorLatency.Describe(ch)
	s.redisConnectorBulkLatency.Describe(ch)
	s.redisConnectorBulkSize.Describe(ch)
	ch <- s.redisConnectorRetried
}
