package configstore

import (
	"context"
	"errors"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/cortexproject/cortex/pkg/util"
	util_log "github.com/cortexproject/cortex/pkg/util/log"

	"github.com/cortexproject/cortex/pkg/ring/kv"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/grafana/agent/pkg/metrics/instance"
	"github.com/hashicorp/consul/api"
	"github.com/weaveworks/common/instrument"
)

// RemoteClient is a simple wrapper to allow the shortcircuit of consul, while being backwards compatible with non
// consul kv stores
type RemoteClient struct {
	kv.Client
	consul *api.Client
	config kv.Config
	log    log.Logger
}

var backoffConfig = util.BackoffConfig{
	MinBackoff: 1 * time.Second,
	MaxBackoff: 1 * time.Minute,
}

const longPollDuration = 10 * time.Second

func (r *RemoteClient) WatchEventsConsul(prefix string, ctx context.Context, f func(bundle WatchBundle) bool) {
	var (
		backoff = util.NewBackoff(ctx, backoffConfig)
		index   = uint64(0)
		limiter = r.createRateLimiter()
	)
	for backoff.Ongoing() {
		err := limiter.Wait(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			level.Error(util_log.Logger).Log("msg", "error while rate-limiting", "prefix", prefix, "err", err)
			backoff.Wait()
			continue
		}

		queryOptions := &api.QueryOptions{
			AllowStale:        !r.config.Consul.ConsistentReads,
			RequireConsistent: r.config.Consul.ConsistentReads,
			WaitIndex:         index,
			WaitTime:          longPollDuration,
		}

		kvps, meta, err := r.consul.KV().List(r.config.Prefix, queryOptions.WithContext(ctx))
		// kvps being nil here is not an error -- quite the opposite. Consul returns index,
		// which makes next query blocking, so there is no need to detect this and act on it.
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "error getting path", "prefix", prefix, "err", err)
			backoff.Wait()
			continue
		}
		backoff.Reset()

		newIndex, skip := checkLastIndex(index, meta.LastIndex)
		if skip {
			continue
		}
		c := GetCodec()
		events := make([]WatchEvent, 0)
		for _, kvp := range kvps {
			// We asked for values newer than 'index', but Consul returns all values below given prefix,
			// even those that haven't changed. We don't need to report all of them as updated.
			if index > 0 && kvp.ModifyIndex <= index && kvp.CreateIndex <= index {
				continue
			}

			out, err := c.Decode(kvp.Value)
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "error decoding list of values for prefix:key", "prefix", prefix, "key", kvp.Key, "err", err)
				continue
			}
			cfg, err := instance.UnmarshalConfig(strings.NewReader(out.(string)))
			key := strings.TrimPrefix(kvp.Key, r.config.Prefix)

			events = append(events, WatchEvent{
				Key:    key,
				Config: cfg,
			})
		}
		bundle := WatchBundle{Events: events}
		if !f(bundle) {
			return
		}

		index = newIndex
	}

}

type Backoff struct {
	cfg          BackoffConfig
	ctx          context.Context
	numRetries   int
	nextDelayMin time.Duration
	nextDelayMax time.Duration
}

// BackoffConfig configures a Backoff
type BackoffConfig struct {
	MinBackoff time.Duration `yaml:"min_period"`  // start backoff at this level
	MaxBackoff time.Duration `yaml:"max_period"`  // increase exponentially to this level
	MaxRetries int           `yaml:"max_retries"` // give up after this many; zero means infinite retries
}

// AllConsul is ONLY usable when consul is the keystore. This is a performance improvement in using the client directly
//	instead of the cortex multi store kv interface. That interface returns the list then each value must be retrieved
//	individually. This returns all the keys and values in one call and works on them in memory
func (r *RemoteClient) AllConsul(ctx context.Context, keep func(key string) bool) (<-chan []*instance.Config, error) {
	var configs []*instance.Config
	c := GetCodec()

	pairs, err := r.listConsul(ctx)

	if err != nil {
		return nil, err
	}
	for _, kvp := range pairs {
		if keep != nil && !keep(kvp.Key) {
			level.Debug(r.log).Log("msg", "skipping key that was filtered out", "key", kvp.Key)
			continue
		}
		value, err := c.Decode(kvp.Value)
		if err != nil {
			level.Error(r.log).Log("msg", "failed to decode config from store", "key", kvp.Key, "err", err)
			continue
		}
		if value == nil {
			// Config was deleted since we called list, skip it.
			level.Debug(r.log).Log("msg", "skipping key that was deleted after list was called", "key", kvp.Key)
			continue
		}

		cfg, err := instance.UnmarshalConfig(strings.NewReader(value.(string)))
		if err != nil {
			level.Error(r.log).Log("msg", "failed to unmarshal config from store", "key", kvp.Key, "err", err)
			continue
		}
		configs = append(configs, cfg)
	}
	ch := make(chan []*instance.Config, len(configs))
	ch <- configs
	close(ch)
	return ch, nil
}

// listConsul returns Key Value Pairs instead of []string
func (r *RemoteClient) listConsul(ctx context.Context) (api.KVPairs, error) {
	var pairs api.KVPairs
	options := &api.QueryOptions{
		AllowStale:        !r.config.Consul.ConsistentReads,
		RequireConsistent: r.config.Consul.ConsistentReads,
	}
	// This is copied from cortex list so that stats stay the same
	err := instrument.CollectedRequest(ctx, "List", consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		var err error
		pairs, _, err = r.consul.KV().List(r.config.Prefix, options.WithContext(ctx))
		return err
	})

	if err != nil {
		return nil, err
	}
	// This mirrors the previous behavior of returning a blank array as opposed to nil.
	if pairs == nil {
		blankPairs := make(api.KVPairs, 0)
		return blankPairs, nil
	}
	for _, kvp := range pairs {
		kvp.Key = strings.TrimPrefix(kvp.Key, r.config.Prefix)
	}
	return pairs, nil
}

func (c *RemoteClient) createRateLimiter() *rate.Limiter {
	if c.config.Consul.WatchKeyRateLimit <= 0 {
		// burst is ignored when limit = rate.Inf
		return rate.NewLimiter(rate.Inf, 0)
	}
	burst := c.config.Consul.WatchKeyBurstSize
	if burst < 1 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(c.config.Consul.WatchKeyRateLimit), burst)
}

func checkLastIndex(index, metaLastIndex uint64) (newIndex uint64, skip bool) {
	// See https://www.consul.io/api/features/blocking.html#implementation-details for logic behind these checks.
	if metaLastIndex == 0 {
		// Don't just keep using index=0.
		// After blocking request, returned index must be at least 1.
		return 1, false
	} else if metaLastIndex < index {
		// Index reset.
		return 0, false
	} else if index == metaLastIndex {
		// Skip if the index is the same as last time, because the key value is
		// guaranteed to be the same as last time
		return metaLastIndex, true
	} else {
		return metaLastIndex, false
	}
}
