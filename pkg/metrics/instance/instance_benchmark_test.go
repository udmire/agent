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

	"github.com/go-kit/log/level"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	promconfig "github.com/prometheus/prometheus/config"
)

func BenchmarkAddingConfigs(b *testing.B) {
	// BenchmarkingConfigs("/home/mdurham/utils/logs/benchmarklog-100.txt", "/home/mdurham/utils/logs/benchmarklog-100.pprof", 100)
	// BenchmarkingConfigs("/home/mdurham/utils/logs/benchmarklog-1000.txt", "/home/mdurham/utils/logs/benchmarklog-1000.pprof", 1_000)
	// BenchmarkingConfigs("/home/mdurham/utils/logs/benchmarklog-2000.txt", 2_000)
	BenchmarkingConfigs("/home/mdurham/utils/logs/benchmarklog-4000.txt", "/home/mdurham/utils/logs/benchmarklog-4000.pprof", 4_000)
}

func BenchmarkingConfigs(file string, profile string, iterations int) {
	os.RemoveAll(profile)
	profileio, err := os.Create(profile)
	if err != nil {
		return
	}
	pprof.StartCPUProfile(profileio)
	defer pprof.StopCPUProfile()
	var sb = strings.Builder{}
	walDir, _ := ioutil.TempDir(os.TempDir(), "wal*")
	defer os.RemoveAll(walDir)
	logger := log.NewNopLogger()
	testServer, closeFunc, _ := getBenchmarkServer()
	defer closeFunc()
	var fakeAgent = &FakeAgent{
		Log:    logger,
		WalDir: walDir,
	}
	basicManager := NewBasicManager(BasicManagerConfig{InstanceRestartBackoff: time.Minute}, logger, fakeAgent.newBenchmarkInstance)
	modalManager, err := NewModalManager(prometheus.DefaultRegisterer, logger, basicManager, "shared")
	if err != nil {
		level.Error(logger).Log("err", err)
	}
	// Occasionally a manager will throw an error if there isnt a slight delay
	sb.WriteString("id, time in ms \n")
	for i := 0; i < iterations; i++ {
		globalConfig := getBenchmarkGlobalConfig()
		cfg := getBenchmarkTestConfig(i, &globalConfig, testServer)
		cfg.WALTruncateFrequency = time.Hour
		cfg.RemoteFlushDeadline = time.Hour
		start := time.Now()
		err := modalManager.ApplyConfig(cfg)
		duration := time.Since(start)
		sb.WriteString(fmt.Sprintf("%d , %d \n", i, duration/time.Millisecond))
		if err != nil {
			logger.Log("err", err)
		}
	}
	modalManager.Stop()
	basicManager.Stop()

	ioutil.WriteFile(file, []byte(sb.String()), 0644)
}

func setupLogger(path string) log.Logger {
	f, _ := os.Create(path)
	w := log.NewSyncWriter(f)
	logger := log.NewLogfmtLogger(w)
	logger = level.NewFilter(logger, level.AllowNone())
	level.SquelchNoLevel(true)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	return logger
}

func getBenchmarkTestConfig(instanceId int, global *GlobalConfig, scrapeAddr string) Config {

	scrapeCfg := promconfig.DefaultScrapeConfig
	scrapeCfg.JobName = "test"
	scrapeCfg.ScrapeInterval = global.Prometheus.ScrapeInterval
	scrapeCfg.ScrapeTimeout = global.Prometheus.ScrapeTimeout
	scrapeCfg.ServiceDiscoveryConfigs = discovery.Configs{
		discovery.StaticConfig{{
			Targets: []model.LabelSet{{
				model.AddressLabel: model.LabelValue(scrapeAddr),
			}},
			Labels: model.LabelSet{},
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
