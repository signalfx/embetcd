package etcdserver

import (
	"context"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/pkg/transport"
	"io/ioutil"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/embed"
	"github.com/signalfx/golib/pointer"
)

func newConfigWithDefaults(in *embed.Config) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = in.Name
	cfg.InitialClusterToken = in.InitialClusterToken
	cfg.Dir = in.Dir
	cfg.PeerAutoTLS = in.PeerAutoTLS
	cfg.PeerTLSInfo = in.PeerTLSInfo
	cfg.LPUrls = in.LPUrls
	cfg.APUrls = in.APUrls
	cfg.ClientAutoTLS = in.ClientAutoTLS
	cfg.ClientTLSInfo = in.ClientTLSInfo
	cfg.LCUrls = in.LCUrls
	cfg.ACUrls = in.ACUrls
	cfg.Metrics = in.Metrics
	cfg.ListenMetricsUrls = in.ListenMetricsUrls
	cfg.ClusterState = in.ClusterState
	return cfg
}

func TestMemberCleanup(t *testing.T) {
	type arg struct {
		etcdCfg          *embed.Config
		initialCluster   []string
		autoSyncInterval *time.Duration
		dialTimeout      *time.Duration
		startTimeout     *time.Duration
		wantStartErr     bool
	}
	tests := []struct {
		name        string
		args        []arg
		wantErr     bool
		stopTimeout *time.Duration
	}{
		{
			name: "ensure that etcd server cleans up cluster",
			args: []arg{
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test-server-1",
							InitialClusterToken: "standup-four-servers",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3580"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3580"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3579"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3579"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:3581"}},
							ClusterState:        embed.ClusterStateFlagNew,
						}),
					initialCluster: []string{"http://127.0.0.1:3579", "http://127.0.0.1:3679", "http://127.0.0.1:3779", "http://127.0.0.1:3889"},
				},
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test-server-2",
							InitialClusterToken: "standup-four-servers",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3680"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3680"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3679"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3679"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:3681"}},
							ClusterState:        embed.ClusterStateFlagExisting,
						}),
					initialCluster: []string{"http://127.0.0.1:3579", "http://127.0.0.1:3679", "http://127.0.0.1:3779", "http://127.0.0.1:3889"},
				},
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test-server-3",
							InitialClusterToken: "standup-four-servers",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3780"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3780"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3779"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3779"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:3781"}},
							ClusterState:        embed.ClusterStateFlagExisting,
						}),
					initialCluster: []string{"http://127.0.0.1:3579", "http://127.0.0.1:3679", "http://127.0.0.1:3779", "http://127.0.0.1:3889"},
				},
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test-server-4",
							InitialClusterToken: "standup-four-servers",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3880"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3880"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3879"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3879"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:3881"}},
							ClusterState:        embed.ClusterStateFlagExisting,
						}),
					initialCluster: []string{"http://127.0.0.1:3579", "http://127.0.0.1:3679", "http://127.0.0.1:3779", "http://127.0.0.1:3889"},
				},
			},
		},
	}
	for _, tt := range tests {
		servers := make([]*Server, 0)
		tempDirs := make([]string, 0)
		t.Run(tt.name, func(t *testing.T) {
			for _, a := range tt.args {
				// create temp data directory for etcd server
				etcdDir, err := ioutil.TempDir("", "TestEtcdServer")
				if err != nil {
					t.Errorf("failed to create temporary data dir for etcd server %v", err)
				}

				// store the dataDir for test clean up
				tempDirs = append(tempDirs, etcdDir)

				// set etcd server dir
				a.etcdCfg.Dir = etcdDir

				// create a new server
				server := New()

				// create a context for starting the server
				var timeout context.Context
				var cancel context.CancelFunc
				if a.startTimeout != nil {
					timeout, cancel = context.WithTimeout(context.Background(), *a.startTimeout)
				} else {
					timeout, cancel = context.WithTimeout(context.Background(), 120*time.Second)
				}

				// start the server
				err = server.Start(timeout, a.etcdCfg, a.etcdCfg.InitialClusterToken, a.initialCluster, a.dialTimeout, a.autoSyncInterval)
				cancel()
				if a.wantStartErr != (err != nil) {
					t.Errorf("New() expected error while starting up server %v but didn't get it error = %v, wantErr %v", server, err, tt.wantErr)
					return
				}

				// if the server started correctly
				if err == nil {
					// cover ErrAlreadyRunning
					if server.Start(timeout, a.etcdCfg, a.etcdCfg.InitialClusterToken, a.initialCluster, a.dialTimeout, a.autoSyncInterval) != ErrAlreadyRunning {
						t.Errorf("Already running server did not return ErrAlreadyRunning when start was invoked %v", server)
					}

					if server.IsRunning() {
						// store server to servers for test clean up
						servers = append(servers, server)
					}
				}
			}
			// verify that all cluster members have the same cluster size
			for _, server := range servers {

				// get the list of etcd cluster members
				members, err := server.Client.MemberList(context.Background())
				if (err != nil) != tt.wantErr {
					t.Errorf("server.Client.MemberList() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				// log the member list for reference in test output
				t.Log("member list:")
				for _, member := range members.Members {
					t.Log(member)
				}

				// verify the member list is as long as we expect
				if len(members.Members) != len(servers) {
					t.Errorf("etcd member list length (%d) does not match expected length (%d)", len(members.Members), len(servers))
					return
				}
			}

			// stop a server abruptly
			servers[0].Server.HardStop()
			servers[0].Server.Stop()

			timeout, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()
			for timeout.Err() == nil {
				members, err := servers[1].Client.MemberList(context.Background())
				if err == nil && len(members.Members) == (len(servers)-1) {
					time.Sleep(time.Second * 30)
					return
				}
				time.Sleep(1 * time.Second)
			}

		})
	}
}

func TestNew(t *testing.T) {
	type arg struct {
		etcdCfg          *embed.Config
		initialCluster   []string
		autoSyncInterval *time.Duration
		dialTimeout      *time.Duration
		startTimeout     *time.Duration
		wantStartErr     bool
	}
	tests := []struct {
		name        string
		args        []arg
		wantErr     bool
		stopTimeout *time.Duration
	}{
		{
			name: "err stand up single etcd server for joining",
			args: []arg{
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:         "test1",
							ClusterState: embed.ClusterStateFlagExisting,
						}),
					wantStartErr: true,
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
		{
			name: "successfully stand up single etcd server",
			args: []arg{
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test1",
							InitialClusterToken: "standup-one-server",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2380"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2380"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2379"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2379"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:2381"}},
							ClusterState:        embed.ClusterStateFlagNew,
						}),
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
		{
			name: "successfully stand up two etcd servers with multiple peer and client urls",
			args: []arg{
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test-server-1",
							InitialClusterToken: "standup-two-servers",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2380"}, {Scheme: "http", Host: "0.0.0.0:2480"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2380"}, {Scheme: "http", Host: "127.0.0.1:2480"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2379"}, {Scheme: "http", Host: "0.0.0.0:2479"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2379"}, {Scheme: "http", Host: "127.0.0.1:2479"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:2381"}},
							ClusterState:        embed.ClusterStateFlagNew,
						}),
					initialCluster: []string{"http://127.0.0.1:2379", "http://127.0.0.1:2479", "http://127.0.0.1:2579", "http://127.0.0.1:2679"},
				},
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test-server-2",
							InitialClusterToken: "standup-two-servers",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2580"}, {Scheme: "http", Host: "0.0.0.0:2680"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2580"}, {Scheme: "http", Host: "127.0.0.1:2680"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2579"}, {Scheme: "http", Host: "0.0.0.0:2679"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2579"}, {Scheme: "http", Host: "127.0.0.1:2679"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:2581"}},
							ClusterState:        embed.ClusterStateFlagExisting,
						}),
					initialCluster: []string{"http://127.0.0.1:2379", "http://127.0.0.1:2479", "http://127.0.0.1:2579"},
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
		{
			name: "unsuccessfully stand up two etcd servers with different cluster names",
			args: []arg{
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test-server-1",
							InitialClusterToken: "standups-two-server-with-dif-names",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2380"}, {Scheme: "http", Host: "0.0.0.0:2480"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2380"}, {Scheme: "http", Host: "127.0.0.1:2480"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2379"}, {Scheme: "http", Host: "0.0.0.0:2479"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2379"}, {Scheme: "http", Host: "127.0.0.1:2479"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:2381"}},
							ClusterState:        embed.ClusterStateFlagNew,
						}),
					initialCluster: []string{"http://127.0.0.1:2379", "http://127.0.0.1:2479", "http://127.0.0.1:2579", "http://127.0.0.1:2679"},
				},
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{

							Name:                "test-server-2",
							InitialClusterToken: "standup-two-servers-with-dif-cluster",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2580"}, {Scheme: "http", Host: "0.0.0.0:2680"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2580"}, {Scheme: "http", Host: "127.0.0.1:2680"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2579"}, {Scheme: "http", Host: "0.0.0.0:2679"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2579"}, {Scheme: "http", Host: "127.0.0.1:2679"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:2581"}},
							ClusterState:        embed.ClusterStateFlagExisting,
						}),
					initialCluster: []string{"http://127.0.0.1:2379", "http://127.0.0.1:2479", "http://127.0.0.1:2579"},
					wantStartErr:   true,
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
		{
			name: "etcd servers with conflicting names should error out",
			args: []arg{
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:              "test-server-1",
							PeerAutoTLS:       false,
							PeerTLSInfo:       transport.TLSInfo{},
							LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2380"}},
							APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2380"}},
							ClientAutoTLS:     false,
							ClientTLSInfo:     transport.TLSInfo{},
							LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2379"}},
							ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2379"}},
							Metrics:           "basic",
							ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2381"}},
							ClusterState:      embed.ClusterStateFlagNew,
						}),
					initialCluster: []string{"http://127.0.0.1:2379", "http://127.0.0.1:2479"},
				},
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:              "test-server-1",
							PeerAutoTLS:       false,
							PeerTLSInfo:       transport.TLSInfo{},
							LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2480"}},
							APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2480"}},
							ClientAutoTLS:     false,
							ClientTLSInfo:     transport.TLSInfo{},
							LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2479"}},
							ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2479"}},
							Metrics:           "basic",
							ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2481"}},
							ClusterState:      embed.ClusterStateFlagExisting,
						}),
					// set a short timeout since this is an error case
					startTimeout:   pointer.Duration(time.Second * 5),
					initialCluster: []string{"http://127.0.0.1:2379", "http://127.0.0.1:2479"},
					wantStartErr:   true,
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
		{
			name: "successfully stand up three etcd servers",
			args: []arg{
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test-server-1",
							InitialClusterToken: "standup-three-servers",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2580"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2580"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2579"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2579"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:2581"}},
							ClusterState:        embed.ClusterStateFlagNew,
						}),
					initialCluster: []string{"http://127.0.0.1:2579", "http://127.0.0.1:2679", "http://127.0.0.1:2779"},
				},
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test-server-2",
							InitialClusterToken: "standup-three-servers",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2680"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2680"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2679"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2679"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:2681"}},
							ClusterState:        embed.ClusterStateFlagExisting,
						}),
					initialCluster: []string{"http://127.0.0.1:2579", "http://127.0.0.1:2679", "http://127.0.0.1:2779"},
				},
				{
					etcdCfg: newConfigWithDefaults(
						&embed.Config{
							Name:                "test-server-3",
							InitialClusterToken: "standup-three-servers",
							PeerAutoTLS:         false,
							PeerTLSInfo:         transport.TLSInfo{},
							LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2780"}},
							APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2780"}},
							ClientAutoTLS:       false,
							ClientTLSInfo:       transport.TLSInfo{},
							LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:2779"}},
							ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:2779"}},
							Metrics:             "basic",
							ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:2781"}},
							ClusterState:        embed.ClusterStateFlagExisting,
						}),
					initialCluster: []string{"http://127.0.0.1:2579", "http://127.0.0.1:2679", "http://127.0.0.1:2779"},
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
	}
	for _, tt := range tests {
		servers := make([]*Server, 0)
		tempDirs := make([]string, 0)
		t.Run(tt.name, func(t *testing.T) {
			for _, a := range tt.args {
				// create temp data directory for etcd server
				etcdDir, err := ioutil.TempDir("", "TestEtcdServer")
				if err != nil {
					t.Errorf("failed to create temporary data dir for etcd server %v", err)
				}

				// store the dataDir for test clean up
				tempDirs = append(tempDirs, etcdDir)

				// set etcd server dir
				a.etcdCfg.Dir = etcdDir

				// create a new server
				server := New()

				// create a context for starting the server
				var timeout context.Context
				var cancel context.CancelFunc
				if a.startTimeout != nil {
					timeout, cancel = context.WithTimeout(context.Background(), *a.startTimeout)
				} else {
					timeout, cancel = context.WithTimeout(context.Background(), 120*time.Second)
				}

				// start the server
				err = server.Start(timeout, a.etcdCfg, a.etcdCfg.InitialClusterToken, a.initialCluster, a.dialTimeout, a.autoSyncInterval)
				cancel()
				if a.wantStartErr != (err != nil) {
					t.Errorf("New() expected error while starting up server %v but didn't get it error = %v, wantErr %v", server, err, tt.wantErr)
					return
				}

				// if the server started correctly
				if err == nil {
					// cover ErrAlreadyRunning
					if server.Start(timeout, a.etcdCfg, a.etcdCfg.InitialClusterToken, a.initialCluster, a.dialTimeout, a.autoSyncInterval) != ErrAlreadyRunning {
						t.Errorf("Already running server did not return ErrAlreadyRunning when start was invoked %v", server)
					}

					if server.IsRunning() {
						// store server to servers for test clean up
						servers = append(servers, server)
					}
				}
			}
			// verify that all cluster members have the same cluster size
			for _, server := range servers {
				if !server.IsRunning() {
					t.Errorf("etcd %v server is not running", server)
					return
				}

				// get the list of etcd cluster members
				members, err := server.Client.MemberList(context.Background())
				if (err != nil) != tt.wantErr {
					t.Errorf("server.Client.MemberList() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				// log the member list for reference in test output
				t.Log("member list:")
				for _, member := range members.Members {
					t.Log(member)
				}

				// verify the member list is as long as we expect
				if len(members.Members) != len(servers) {
					t.Errorf("etcd member list length (%d) does not match expected length (%d)", len(members.Members), len(servers))
					return
				}


			}
		})

		// clean up servers
		for _, server := range servers {
			var ctx context.Context
			var cancel context.CancelFunc
			if tt.stopTimeout == nil {
				ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
			} else {
				ctx, cancel = context.WithTimeout(context.Background(), *tt.stopTimeout)
			}

			// shutdown
			if err := server.Shutdown(ctx); err != nil && err != etcdserver.ErrNotEnoughStartedMembers && err != rpctypes.ErrMemberNotEnoughStarted && err != rpctypes.ErrGRPCMemberNotEnoughStarted {
				t.Errorf("error while closing etccd client for server '%s' %v", server.Config().Name, err)
			}

			// cover IsRunning()
			if server.IsRunning() {
				t.Errorf("server %v did not shutdown properly", server)
			}

			// cover ErrAlreadyStopped error
			if err := server.Shutdown(ctx); err != ErrAlreadyStopped {
				t.Errorf("Already stopped server did not return ErrAlreadyStopped when stop was called: %v", server)
			}
			cancel()
		}

		// remove temp etcd dirs created during
		for _, path := range tempDirs {
			if err := os.RemoveAll(path); err != nil {
				t.Errorf("error while removing etcd temp dir '%s' %v", path, err)
			}
		}
	}
}

func TestServer_waitForShutdown(t *testing.T) {
	closedDone := make(chan struct{})
	close(closedDone)
	expiredContext, _ := context.WithTimeout(context.Background(), 0)
	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()

	etcdDir, err := ioutil.TempDir("", "TestEtcdServer")
	if err != nil {
		t.Errorf("failed to create temporary data dir for etcd server %v", err)
	}

	type args struct {
		done chan struct{}
		ctx  context.Context
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test done channel closed",
			args: args{
				done: closedDone,
				ctx:  context.Background(),
			},
		},
		{
			name: "test ctx.Done() with expired context",
			args: args{
				done: make(chan struct{}),
				ctx:  expiredContext,
			},
		},
		{
			name: "test ctx.Done() with canceled context",
			args: args{
				done: make(chan struct{}),
				ctx:  canceledContext,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			timeout, cancel := context.WithTimeout(context.Background(), time.Second*5)

			config := embed.NewConfig()
			config.Dir = etcdDir

			if err := s.Start(timeout, config, config.InitialClusterToken, []string{}, nil, nil); err != nil {
				t.Errorf("Server.waitForShutdown() failed to set up the server for test %v", err)
			}
			cancel()

			if err := s.waitForShutdown(tt.args.done, tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("Server.waitForShutdown() error = %v, wantErr %v", err, tt.wantErr)
			}
			// clean up tests
			s.Close()
		})
	}
}

func Test_waitForReady(t *testing.T) {
	closedStructCh := make(chan struct{})
	close(closedStructCh)
	closedErrCh := make(chan error)
	close(closedErrCh)
	type args struct {
		readyCh <-chan struct{}
		errCh   <-chan error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test readyCh returns",
			args: args{
				readyCh: closedStructCh,
				errCh:   make(chan error),
			},
			wantErr: false,
		},
		{
			name: "test errCh returns",
			args: args{
				readyCh: make(chan struct{}),
				errCh:   closedErrCh,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := waitForReady(tt.args.readyCh, tt.args.errCh); (err != nil) != tt.wantErr {
				t.Errorf("waitForReady() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServer_errorHandlerRoutine(t *testing.T) {
	closedStructCh := make(chan struct{})
	close(closedStructCh)
	closedErrCh := make(chan error)
	close(closedErrCh)

	etcdDir, err := ioutil.TempDir("", "TestEtcdServer")
	if err != nil {
		t.Errorf("failed to create temporary data dir for etcd server %v", err)
	}

	type args struct {
		stopCh <-chan struct{}
		errCh  <-chan error
	}
	tests := []struct {
		name string
		args args
	}{
		{
			// by testing the error channel case we actually get coverage of the stop ch too
			name: "err channel returns value",
			args: args{
				stopCh: make(chan struct{}),
				errCh:  closedErrCh,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			timeout, cancel := context.WithTimeout(context.Background(), time.Second*5)

			config := embed.NewConfig()
			config.Dir = etcdDir

			if err := s.Start(timeout, config, config.InitialClusterToken, []string{}, nil, nil); err != nil {
				t.Errorf("Server.errorHandlerRoutine() failed to set up the server for test %v", err)
			}
			cancel()
			s.errorHandlerRoutine(tt.args.stopCh, tt.args.errCh)
			if s.IsRunning() {
				t.Errorf("Server.errorHandlerRoutine() did not shutdownt he server")
			}
		})
	}
}

func Test_duration(t *testing.T) {
	type args struct {
		in  *time.Duration
		def time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			name: "expect default to be returned when incoming value is nil",
			args: args{
				def: time.Second * 5,
			},
			want: time.Second * 5,
		},
		{
			name: "expect default to be returned when incoming value is nil",
			args: args{
				in:  pointer.Duration(time.Second * 1),
				def: time.Second * 5,
			},
			want: time.Second * 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := duration(tt.args.in, tt.args.def); got != tt.want {
				t.Errorf("duration() = %v, want %v", got, tt.want)
			}
		})
	}
}
