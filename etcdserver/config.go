package etcdserver

import (
	"context"
	"crypto/tls"
	"time"

	cli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/signalfx/embedded-etcd/etcdclient"
	common "github.com/signalfx/embedded-etcd/etcdcommon"
)

type Config struct {
	*embed.Config
	ClusterName        string
	InitialCluster     []string
	CleanUpInterval    *time.Duration
	DialTimeout        *time.Duration
	AutoSyncInterval   *time.Duration
	StartupGracePeriod *time.Duration
	UnhealthyTTL       *time.Duration
}

// GetClientFromConfig returns a client with the supplied context from the config
func (c *Config) GetClientFromConfig(ctx context.Context) (*etcdclient.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// create a client to the existing cluster
	return etcdclient.New(cli.Config{
		Endpoints:        c.InitialCluster,
		DialTimeout:      common.DurationOrDefault(c.DialTimeout, DefaultDialTimeout),
		TLS:              &tls.Config{InsecureSkipVerify: true}, // insecure for now
		AutoSyncInterval: common.DurationOrDefault(c.AutoSyncInterval, DefaultAutoSyncInterval),
		Context:          ctx, // pass in the context so the temp client closes with a cancelled context
	})
}

// NewConfig returns a new config object with defaults provided by etcd embed
func NewConfig() *Config {
	return &Config{Config: embed.NewConfig()}
}
