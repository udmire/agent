package instance

import (
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-kit/log/level"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promconfig "github.com/prometheus/prometheus/config"
)

func BenchmarkAddingConfigs(b *testing.B) {
	tt := []struct {
		numConfigs, numJobs int
	}{
		//{100, 1},
		{1000, 1},
		// {2000, 1},
	}

	for _, tc := range tt {
		name := fmt.Sprintf("%d_configs_%d_jobs", tc.numConfigs, tc.numJobs)
		b.Run(name, func(b *testing.B) {
			output := fmt.Sprintf("/home/mdurham/utils/logs/benchmarklog-%d.txt", tc.numConfigs)
			profile := fmt.Sprintf("/home/mdurham/utils/logs/benchmarklog-%d.pprof", tc.numConfigs)
			//benchmarkingConfigs(b, output, profile, tc.numConfigs)
			benchmarkingBulkConfigs(b, output, profile, tc.numConfigs)
		})
	}
}

func benchmarkingConfigs(b *testing.B, file string, profile string, iterations int) {
	err := os.RemoveAll(profile)
	if err != nil {
		return
	}
	profileio, err := os.Create(profile)
	require.NoError(b, err)
	err = pprof.StartCPUProfile(profileio)
	require.NoError(b, err)
	defer pprof.StopCPUProfile()
	var sb = strings.Builder{}
	walDir, _ := ioutil.TempDir(os.TempDir(), "wal*")
	b.Cleanup(func() {
		os.RemoveAll(walDir)
		require.NoError(b, err)
	})
	logger := log.NewNopLogger()
	testServer, closeFunc, _ := getBenchmarkServer()
	defer closeFunc()
	var fakeAgent = &FakeAgent{
		Log:    logger,
		WalDir: walDir,
	}
	basicManager := NewBasicManager(DefaultBasicManagerConfig, logger, fakeAgent.newBenchmarkInstance)
	modalManager, err := NewModalManager(prometheus.NewRegistry(), logger, basicManager, ModeShared)

	require.NoError(b, err)
	defer modalManager.Stop()

	if err != nil {
		err := level.Error(logger).Log("err", err)
		require.NoError(b, err)
	}
	sb.WriteString("id, time in ms \n")
	for i := 0; i < iterations; i++ {
		globalConfig := getBenchmarkGlobalConfig()
		cfg := getBenchmarkTestConfig(i, &globalConfig, testServer)
		cfg.WALTruncateFrequency = time.Hour
		cfg.RemoteFlushDeadline = time.Hour
		start := time.Now()
		err := modalManager.ApplyConfig(cfg)
		duration := time.Since(start)
		println(fmt.Sprintf("apply took %s", duration))
		sb.WriteString(fmt.Sprintf("%d , %d \n", i, duration/time.Millisecond))
		if err != nil {
			logger.Log("err", err)
		}
	}

	err = ioutil.WriteFile(file, []byte(sb.String()), 0644)
	require.NoError(b, err)
}

func benchmarkingBulkConfigs(b *testing.B, file string, profile string, iterations int) {
	err := os.RemoveAll(profile)
	if err != nil {
		return
	}
	profileio, err := os.Create(profile)
	require.NoError(b, err)
	err = pprof.StartCPUProfile(profileio)
	require.NoError(b, err)
	defer pprof.StopCPUProfile()
	var sb = strings.Builder{}
	walDir, _ := ioutil.TempDir(os.TempDir(), "wal*")
	b.Cleanup(func() {
		os.RemoveAll(walDir)
		require.NoError(b, err)
	})
	logger := log.NewNopLogger()
	testServer, closeFunc, _ := getBenchmarkServer()
	defer closeFunc()
	var fakeAgent = &FakeAgent{
		Log:    logger,
		WalDir: walDir,
	}
	basicManager := NewBasicManager(DefaultBasicManagerConfig, logger, fakeAgent.newBenchmarkInstance)
	modalManager, err := NewModalManager(prometheus.NewRegistry(), logger, basicManager, ModeShared)

	require.NoError(b, err)
	defer modalManager.Stop()

	if err != nil {
		err := level.Error(logger).Log("err", err)
		require.NoError(b, err)
	}
	sb.WriteString("id, time in ms \n")
	configs := make([]Config, 0)
	for i := 0; i < iterations; i++ {
		globalConfig := getBenchmarkGlobalConfig()
		cfg := getBenchmarkTestConfig(i, &globalConfig, testServer)
		cfg.WALTruncateFrequency = time.Hour
		cfg.RemoteFlushDeadline = time.Hour
		configs = append(configs, cfg)

	}
	start := time.Now()
	err = modalManager.ApplyConfigs(configs)
	duration := time.Since(start)
	println(fmt.Sprintf("apply took %s", duration))

	err = ioutil.WriteFile(file, []byte(sb.String()), 0644)
	require.NoError(b, err)
}

func getBenchmarkTestConfig(instanceId int, global *GlobalConfig, scrapeAddr string) Config {
	container := fmt.Sprintf("%d", instanceId)
	scrapeCfg := promconfig.DefaultScrapeConfig
	scrapeCfg.JobName = container
	scrapeCfg.ScrapeInterval = global.Prometheus.ScrapeInterval
	scrapeCfg.ScrapeTimeout = global.Prometheus.ScrapeTimeout
	scrapeCfg.ServiceDiscoveryConfigs = discovery.Configs{
		discovery.StaticConfig{{
			Targets: []model.LabelSet{{model.AddressLabel: model.LabelValue(scrapeCfg.JobName)}},
			Labels: model.LabelSet{
				model.LabelName("container"): model.LabelValue(scrapeCfg.JobName),
			},
		}},
	}
	cfg := DefaultConfig
	cfg.Name = fmt.Sprintf("test %d", instanceId)
	cfg.ScrapeConfigs = []*promconfig.ScrapeConfig{&scrapeCfg}
	cfg.global = *global

	return cfg
}

func getBenchmarkGlobalConfig() GlobalConfig {

	return GlobalConfig{
		Prometheus: promconfig.GlobalConfig{
			ScrapeInterval:     model.Duration(time.Millisecond * 50),
			ScrapeTimeout:      model.Duration(time.Millisecond * 100),
			EvaluationInterval: model.Duration(time.Hour),
		},
	}
}

func getBenchmarkServer() (addr string, closeFunc func(), registry prometheus.Registry) {
	reg := prometheus.NewRegistry()

	testCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "test_metric_total",
	})
	testCounter.Inc()
	reg.MustRegister(testCounter)

	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	httpSrv := httptest.NewServer(handler)
	return httpSrv.Listener.Addr().String(), httpSrv.Close, registry
}

type FakeAgent struct {
	Log    log.Logger
	WalDir string
}

func (a *FakeAgent) newBenchmarkInstance(c Config) (ManagedInstance, error) {

	// Controls the label
	instanceLabel := "instance_group_name"

	reg := prometheus.WrapRegistererWith(prometheus.Labels{
		instanceLabel: c.Name,
	}, prometheus.DefaultRegisterer)

	return defaultInstanceFactory(reg, c, a.WalDir, a.Log)
}

func defaultInstanceFactory(reg prometheus.Registerer, cfg Config, walDir string, logger log.Logger) (ManagedInstance, error) {
	return New(reg, cfg, walDir, logger)
}
