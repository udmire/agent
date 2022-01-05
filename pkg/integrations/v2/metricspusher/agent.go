// Package agent is an "example" integration that has very little functionality,
// but is still useful in practice. The Agent integration re-exposes the Agent's
// own metrics endpoint and allows the Agent to scrape itself.
package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/agent/pkg/integrations/v2"
	"github.com/grafana/agent/pkg/metrics"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
)

/*
Example configuration:

server:
  log_level: debug
  http_listen_port: 12345

metrics:
  wal_directory: /tmp/agent
  global:
    scrape_interval: 1m
    # Push to locally running Cortex
    remote_write:
    - url: http://localhost:9009/api/prom/push
  configs:
    # Create an instance for our integration to push to.
    - name: default

integrations:
  metricspusher:
    target_instance: default
*/

// Config controls the Agent integration.
type Config struct {
	// Target metrics instance name to push metrics to. Must exist.
	TargetInstance string `yaml:"target_instance"`
}

// Name returns the name of the integration that this config represents.
func (c *Config) Name() string { return "metricspusher" }

// ApplyDefaults applies runtime-specific defaults to c.
func (c *Config) ApplyDefaults(globals integrations.Globals) error {
	// Make sure the TargetInstance exists
	for _, inst := range globals.Metrics.Config().Configs {
		if inst.Name == c.TargetInstance {
			return nil
		}
	}
	return fmt.Errorf("metrics instance %q is not defined", c.TargetInstance)
}

// Identifier uniquely identifies this instance of Config.
func (c *Config) Identifier(globals integrations.Globals) (string, error) {
	return globals.AgentIdentifier, nil
}

// NewIntegration converts this config into an instance of an integration.
func (c *Config) NewIntegration(l log.Logger, globals integrations.Globals) (integrations.Integration, error) {
	return newIntegration(l, c, globals)
}

func init() {
	integrations.Register(&Config{}, integrations.TypeSingleton)
}

type metricsPusherIntegration struct {
	log      log.Logger
	metrics  *metrics.Agent
	instance string
}

func newIntegration(l log.Logger, c *Config, globals integrations.Globals) (*metricsPusherIntegration, error) {
	return &metricsPusherIntegration{
		log:      l,
		metrics:  globals.Metrics,
		instance: c.TargetInstance,
	}, nil
}

func (i *metricsPusherIntegration) RunIntegration(ctx context.Context) error {
	// Create an example metric every 5 seconds as long as the integration is
	// running.
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			// We're being asked to stop running
			return nil
		case <-t.C:
			if err := i.createExampleMetric(ctx); err != nil {
				level.Error(i.log).Log("msg", "failed to create example metric", "err", err)
			}
		}
	}
}

func (i *metricsPusherIntegration) createExampleMetric(ctx context.Context) error {
	// Get our running instance. This shouldn't be cached, since it can change
	// when the config changes. Do the lookup every time, it's cheap.
	inst, err := i.metrics.InstanceManager().GetInstance(i.instance)
	if err != nil {
		return fmt.Errorf("failed to get instance to push to: %w", err)
	}

	// Series to create a metric for. This is `my_metric_name{foo="bar"}`
	// There must always at least be a label for model.MetricNameLabel
	// (which is internally the `__name__` label)
	writeSeries := labels.Labels{
		{Name: model.MetricNameLabel, Value: "my_metric_name"},
		{Name: "foo", Value: "bar"},
	}

	// Get an appender for us to send metrics to.
	app := inst.Appender(ctx)

	// Append a sample. Samples are collected in-memory and only written
	// once Commit is called.
	//
	// Append returns a ref ID represending the labels that can be passed on
	// future calls for slightly faster appends, but we won't worry about that
	// here.
	_, err = app.Append(
		0,                              // cached ref ID. You can just use 0 here.
		writeSeries,                    // series labels
		timestamp.FromTime(time.Now()), // sample timestamp
		1234.56,                        // sample value
	)
	if err != nil {
		return fmt.Errorf("failed to append sample: %w", err)
	}

	// Finally, commit the pending samples. This will write them to the WAL which
	// will then be read and written via remote_write.
	return app.Commit()
}
