package embetcd

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	cli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/signalfx/golib/pointer"
)

// helper function to get an etcd config with default values and then overwrite what we want
func testNewConfigWithDefaults(in *Config) *Config {
	cfg := NewConfig()
	cfg.Name = in.Name
	cfg.ClusterName = in.ClusterName
	cfg.InitialCluster = in.InitialCluster
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

// creates a data directory in a temporary directory
func testCreateEtcdDir(t *testing.T, tempDirs []string) (string, error) {
	// create temp data directory for etcd server
	etcdDir, err := ioutil.TempDir("", "TestEtcdServer")
	if err != nil {
		t.Errorf("Failed to create directory")
		t.Fail()
	}
	if err == nil {
		// store the dataDir for test clean up
		tempDirs = append(tempDirs, etcdDir)
	}

	return etcdDir, err
}

// starts a server using the supplied etcd cfg and timeout and returns the server and modified array of servers
func testStartServer(t *testing.T, etcdCfg *Config, startTimeout *time.Duration, servers []*Server) (*Server, []*Server, error) {
	var err error
	// create a new server
	server := New()

	// create a context for starting the server
	var timeout context.Context
	var cancel context.CancelFunc
	if startTimeout != nil {
		timeout, cancel = context.WithTimeout(context.Background(), *startTimeout)
	} else {
		timeout, cancel = context.WithTimeout(context.Background(), 120*time.Second)
	}

	// start the server
	err = server.Start(timeout, etcdCfg)

	cancel()

	// if the server started correctly
	if err == nil {
		// cover ErrAlreadyRunning
		if server.Start(timeout, etcdCfg) != ErrAlreadyRunning {
			t.Errorf("Already running server did not return ErrAlreadyRunning when start was invoked %v", server)
			t.Fail()
		}

		if server.IsRunning() {
			// store server to servers for test clean up
			servers = append(servers, server)
		}
	}

	return server, servers, err
}

func testAssertConsistentClusterSize(t *testing.T, servers []*Server) error {
	// verify that all cluster members have the same cluster size
	for _, server := range servers {

		// get the list of etcd cluster members
		members := server.Server.Cluster().Members()

		// log the member list for reference in test output
		t.Logf("%s member list:", server.Server.Cfg.Name)
		for _, member := range members {
			t.Log(member)
		}

		// verify the member list is as long as we expect
		if len(members) != len(servers) {
			return fmt.Errorf("etcd member list length (%d) does not match expected length (%d)", len(members), len(servers))
		}
	}
	return nil
}

func testTearDownCluster(t *testing.T, servers []*Server) {
	t.Log("TEST TEAR DOWN")
	// clean up servers that might be running
	for _, server := range servers {
		t.Log("Tearing down server: ", server.Config().Name, server.Server.ID())
		server.Server.HardStop()
		server.Close()
	}
}

func testTearDownDirectories(t *testing.T, dirs []string) {
	// clean up directories that might remain
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Error while removing etcd temp dir '%s' %v", dir, err)
		}
	}
}

func TestNew(t *testing.T) {
	type arg struct {
		etcdCfg          *Config
		AutoSyncInterval time.Duration
		dialTimeout      time.Duration
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
			// ports 23XX
			name: "err stand up single etcd server for joining",
			args: []arg{
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:         "test1",
								ClusterState: embed.ClusterStateFlagExisting,
							},
						}),
					wantStartErr: true,
					startTimeout: pointer.Duration(time.Second * 5),
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
		{
			// ports 24XX
			name: "successfully stand up single etcd server",
			args: []arg{
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test1",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2480"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2480"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2470"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2470"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2491"}},
								ClusterState:      embed.ClusterStateFlagNew,
							},
							ClusterName: "standup-one-server",
						}),
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
		{
			// ports 25XX, 26XX
			name: "successfully stand up two etcd servers with multiple peer and client urls",
			args: []arg{
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-1",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2580"}, {Scheme: "http", Host: "0.0.0.0:2581"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2580"}, {Scheme: "http", Host: "127.0.0.1:2581"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2570"}, {Scheme: "http", Host: "0.0.0.0:2571"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2570"}, {Scheme: "http", Host: "127.0.0.1:2571"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2591"}},
								ClusterState:      embed.ClusterStateFlagNew,
							},
							InitialCluster: []string{"http://127.0.0.1:2570", "http://127.0.0.1:2571", "http://127.0.0.1:2670", "http://127.0.0.1:2671"},
							ClusterName:    "standup-two-servers",
						}),
				},
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-2",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2680"}, {Scheme: "http", Host: "0.0.0.0:2681"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2680"}, {Scheme: "http", Host: "127.0.0.1:2681"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2670"}, {Scheme: "http", Host: "0.0.0.0:2671"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2670"}, {Scheme: "http", Host: "127.0.0.1:2671"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2691"}},
								ClusterState:      embed.ClusterStateFlagExisting,
							},
							InitialCluster: []string{"http://127.0.0.1:2570", "http://127.0.0.1:2571", "http://127.0.0.1:2670", "http://127.0.0.1:2671"},
							ClusterName:    "standup-two-servers",
						}),
				},
			},
			stopTimeout: pointer.Duration(time.Second * 30),
		},
		{
			// ports 27XX, 28XX
			name: "server should fail to join a cluster that has a different cluster name than it's config",
			args: []arg{
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-1",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2780"}, {Scheme: "http", Host: "0.0.0.0:2781"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2780"}, {Scheme: "http", Host: "127.0.0.1:2781"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2770"}, {Scheme: "http", Host: "0.0.0.0:2771"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2770"}, {Scheme: "http", Host: "127.0.0.1:2771"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2791"}},
								ClusterState:      embed.ClusterStateFlagNew,
							},
							ClusterName:    "standups-two-server-with-dif-names",
							InitialCluster: []string{"http://127.0.0.1:2770", "http://127.0.0.1:2771", "http://127.0.0.1:2880", "http://127.0.0.1:2881"},
						}),
				},
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-2",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2880"}, {Scheme: "http", Host: "0.0.0.0:2881"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2880"}, {Scheme: "http", Host: "127.0.0.1:2881"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2870"}, {Scheme: "http", Host: "0.0.0.0:2871"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2870"}, {Scheme: "http", Host: "127.0.0.1:2871"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2891"}},
								ClusterState:      embed.ClusterStateFlagExisting,
							},
							InitialCluster: []string{"http://127.0.0.1:2770", "http://127.0.0.1:2771", "http://127.0.0.1:2880", "http://127.0.0.1:2881"},
							ClusterName:    "standup-two-servers-with-dif-cluster",
						}),
					wantStartErr: true,
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
		{
			// ports 29XX, 30XX
			name: "etcd servers with conflicting names should error out",
			args: []arg{
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-1",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2980"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2980"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:2970"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:2970"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:2981"}},
								ClusterState:      embed.ClusterStateFlagNew,
							},
							InitialCluster: []string{"http://127.0.0.1:2970", "http://127.0.0.1:3070"},
						}),
				},
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-1",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3080"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3080"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3070"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3070"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:3091"}},
								ClusterState:      embed.ClusterStateFlagExisting,
							},
							InitialCluster: []string{"http://127.0.0.1:2970", "http://127.0.0.1:3070"},
						}),
					// set a short timeout since this is an error case
					startTimeout: pointer.Duration(time.Second * 5),
					wantStartErr: true,
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
		{
			// ports 31XX, 32XX, 33XX
			name: "successfully stand up three etcd servers",
			args: []arg{
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:                "test-server-1",
								InitialClusterToken: "standup-three-servers",
								PeerAutoTLS:         false,
								PeerTLSInfo:         transport.TLSInfo{},
								LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3180"}},
								APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3180"}},
								ClientAutoTLS:       false,
								ClientTLSInfo:       transport.TLSInfo{},
								LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3170"}},
								ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3170"}},
								Metrics:             "basic",
								ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:3191"}},
								ClusterState:        embed.ClusterStateFlagNew,
							},
							InitialCluster: []string{"http://127.0.0.1:3170", "http://127.0.0.1:3270", "http://127.0.0.1:3370"},
						}),
				},
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:                "test-server-2",
								InitialClusterToken: "standup-three-servers",
								PeerAutoTLS:         false,
								PeerTLSInfo:         transport.TLSInfo{},
								LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3280"}},
								APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3280"}},
								ClientAutoTLS:       false,
								ClientTLSInfo:       transport.TLSInfo{},
								LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3270"}},
								ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3270"}},
								Metrics:             "basic",
								ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:3291"}},
								ClusterState:        embed.ClusterStateFlagExisting,
							},
							InitialCluster: []string{"http://127.0.0.1:3170", "http://127.0.0.1:3270", "http://127.0.0.1:3370"},
						}),
				},
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:                "test-server-3",
								InitialClusterToken: "standup-three-servers",
								PeerAutoTLS:         false,
								PeerTLSInfo:         transport.TLSInfo{},
								LPUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3380"}},
								APUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3380"}},
								ClientAutoTLS:       false,
								ClientTLSInfo:       transport.TLSInfo{},
								LCUrls:              []url.URL{{Scheme: "http", Host: "0.0.0.0:3370"}},
								ACUrls:              []url.URL{{Scheme: "http", Host: "127.0.0.1:3370"}},
								Metrics:             "basic",
								ListenMetricsUrls:   []url.URL{{Scheme: "http", Host: "127.0.0.1:3391"}},
								ClusterState:        embed.ClusterStateFlagExisting,
							},
							InitialCluster: []string{"http://127.0.0.1:3170", "http://127.0.0.1:3270", "http://127.0.0.1:3370"},
						}),
				},
			},
			stopTimeout: pointer.Duration(time.Second * 30),
		},
		{
			// ports 34XX, 34XX intentional conflict
			name: "etcd servers should fail and return an error if their configured port is blocked",
			args: []arg{
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-1",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3480"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3480"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3470"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3470"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:3481"}},
								ClusterState:      embed.ClusterStateFlagNew,
							},
							InitialCluster: []string{"http://127.0.0.1:3470", "http://127.0.0.1:3470"},
						}),
				},
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-2",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3480"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3480"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3470"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3470"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:3491"}},
								ClusterState:      embed.ClusterStateFlagExisting,
							},
							InitialCluster: []string{"http://127.0.0.1:3470"},
						}),
					// set a short timeout since this is an error case
					startTimeout: pointer.Duration(time.Second * 30),
					wantStartErr: true,
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDirs := make([]string, 0, len(tt.args))
			defer testTearDownDirectories(t, tempDirs)

			servers := make([]*Server, 0, len(tt.args))
			defer testTearDownCluster(t, servers)

			// iterate over each server config
			for _, a := range tt.args {

				t.Log(a.etcdCfg.Name)

				// create the etcd server directory
				var err error
				if a.etcdCfg.Dir, err = testCreateEtcdDir(t, tempDirs); err != nil {
					t.Errorf("failed to create temporary data dir for etcd server %v", err)
					return
				}

				// start the server
				if _, servers, err = testStartServer(t, a.etcdCfg, a.startTimeout, servers); (err != nil) != a.wantStartErr {
					t.Errorf("New() expected error while starting up server %s [%v] but didn't get error = %v, wantErr %v", a.etcdCfg.Name, a.etcdCfg.APUrls, err, a.wantStartErr)
					return
				}
			}

			t.Log("Ensuring each member is up and aware of each other..")
			if err := testAssertConsistentClusterSize(t, servers); err != nil {
				t.Errorf(err.Error())
				return
			}

			t.Log("Shutting down each member in the cluster")
			for _, server := range servers {
				var ctx context.Context
				var cancel context.CancelFunc
				if tt.stopTimeout == nil {
					ctx, cancel = context.WithTimeout(context.Background(), time.Second*30)
				} else {
					ctx, cancel = context.WithTimeout(context.Background(), *tt.stopTimeout)
				}

				// shutdown
				t.Logf("Shutting down server %s. There are %d open file descriptors open.", server.config.Name, openFDCount(t))

				if err := server.Shutdown(ctx); err != nil && err != etcdserver.ErrNotEnoughStartedMembers && err != rpctypes.ErrMemberNotEnoughStarted && err != rpctypes.ErrGRPCMemberNotEnoughStarted {
					t.Errorf("error while closing etccd server '%s' %v", server.Config().Name, err)
				} else {
					t.Logf("The returned error when shutting down was: %v", err)
				}

				// cover IsRunning()
				if server.IsRunning() {
					t.Errorf("server %s, %v did not shutdown properly", server.Config().Name, server.Server.ID())
				}

				// cover ErrAlreadyStopped error
				if err := server.Shutdown(ctx); err != ErrAlreadyStopped {
					t.Errorf("Already stopped server did not return ErrAlreadyStopped when stop was called: %v", server)
				}

				t.Logf("Shutdown server %s. There are %d open file descriptors open.", server.config.Name, openFDCount(t))

				// cleanUpStart should just run
				server.cleanUpStart(fmt.Errorf("bogus error"))
				cancel()
			}
		})
	}
}

func TestMemberHaltsAndIsReAdded(t *testing.T) {
	type arg struct {
		etcdCfg          *Config
		AutoSyncInterval time.Duration
		dialTimeout      time.Duration
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
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-1",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3580"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3580"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3579"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3579"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:3581"}},
								ClusterState:      embed.ClusterStateFlagNew,
							},
							InitialCluster: []string{"http://127.0.0.1:3579", "http://127.0.0.1:3679", "http://127.0.0.1:3779", "http://127.0.0.1:3879"},
							ClusterName:    "standup-four-servers",
						}),
				},
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-2",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3680"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3680"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3679"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3679"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:3681"}},
								ClusterState:      embed.ClusterStateFlagExisting,
							},
							InitialCluster: []string{"http://127.0.0.1:3579", "http://127.0.0.1:3679", "http://127.0.0.1:3779", "http://127.0.0.1:3879"},
							ClusterName:    "standup-four-servers",
						}),
				},
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-3",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3780"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3780"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3779"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3779"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:3781"}},
								ClusterState:      embed.ClusterStateFlagExisting,
							},
							InitialCluster: []string{"http://127.0.0.1:3579", "http://127.0.0.1:3679", "http://127.0.0.1:3779", "http://127.0.0.1:3879"},
							ClusterName:    "standup-four-servers",
						}),
				},
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test-server-4",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3880"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3880"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3879"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3879"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:3881"}},
								ClusterState:      embed.ClusterStateFlagExisting,
							},
							InitialCluster: []string{"http://127.0.0.1:3579", "http://127.0.0.1:3679", "http://127.0.0.1:3779", "http://127.0.0.1:3879"},
							ClusterName:    "standup-four-servers",
						}),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDirs := make([]string, 0, len(tt.args))
			defer testTearDownDirectories(t, tempDirs)

			servers := make([]*Server, 0, len(tt.args))
			defer testTearDownCluster(t, servers)

			// iterate over each server config
			for _, a := range tt.args {
				// create the etcd server directory
				var err error
				if a.etcdCfg.Dir, err = testCreateEtcdDir(t, tempDirs); err != nil {
					t.Errorf("failed to create temporary data dir for etcd server %v", err)
					return
				}

				// start the server
				var server *Server
				if server, servers, err = testStartServer(t, a.etcdCfg, a.startTimeout, servers); (err != nil) != a.wantStartErr {
					t.Errorf("New() expected error while starting up server %v but didn't get error = %v, wantErr %v", server, err, a.wantStartErr)
					return
				}
			}

			t.Log("Ensuring each member is up and aware of each other..")
			if err := testAssertConsistentClusterSize(t, servers); err != nil {
				t.Errorf(err.Error())
				return
			}

			// stop a server abruptly
			t.Logf("Killing server in cluster: %s %v.  There are %d open file descriptors.", servers[0].Config().Name, servers[0].Server.ID(), openFDCount(t))

			servers[0].Server.HardStop() // hard stop does not clear the listener
			servers[0].Close()           // invoking close is what ultimately stops the etcd server from hogging the port

			t.Logf("Kiled server: %s %v.  There are %d open file descriptors.", servers[0].Config().Name, servers[0].Server.ID(), openFDCount(t))

			// remove the server from the list of servers
			servers = servers[1:]

			// ensure the cluster size is eventually consistent with some timeout
			timeout, cancel := context.WithTimeout(context.Background(), time.Second*120)
			defer cancel()
			for timeout.Err() == nil {
				members := servers[0].Server.Cluster().Members()
				if len(members) == len(servers) {
					for _, member := range members {
						t.Log(member)
					}
					break
				}
				time.Sleep(5 * time.Second)
			}

			// if the timeout timed out there was an error
			if timeout.Err() != nil {
				t.Error(timeout.Err().Error())
			}

			t.Log("Adding the first server back to the cluster")
			// change the cluster state in the test to existing since we're rejoining
			tt.args[0].etcdCfg.ClusterState = embed.ClusterStateFlagExisting
			var err error
			if _, servers, err = testStartServer(t, tt.args[0].etcdCfg, tt.args[0].startTimeout, servers); err != nil {
				t.Error("Failed to add the first server back to the cluster", err)
				return
			}

			t.Log("Ensuring each member is up and aware of each other..")
			if err := testAssertConsistentClusterSize(t, servers); err != nil {
				t.Errorf(err.Error())
				return
			}
		})
	}
}

func TestServer_cleanUpStart(t *testing.T) {
	type arg struct {
		etcdCfg          *Config
		AutoSyncInterval time.Duration
		dialTimeout      time.Duration
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
			// ports 35XX, 35XX intentional conflict
			name: "successfully stand up single etcd server",
			args: []arg{
				{
					etcdCfg: testNewConfigWithDefaults(
						&Config{
							Config: &embed.Config{
								Name:              "test1",
								PeerAutoTLS:       false,
								PeerTLSInfo:       transport.TLSInfo{},
								LPUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3980"}},
								APUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3980"}},
								ClientAutoTLS:     false,
								ClientTLSInfo:     transport.TLSInfo{},
								LCUrls:            []url.URL{{Scheme: "http", Host: "0.0.0.0:3970"}},
								ACUrls:            []url.URL{{Scheme: "http", Host: "127.0.0.1:3970"}},
								Metrics:           "basic",
								ListenMetricsUrls: []url.URL{{Scheme: "http", Host: "127.0.0.1:3991"}},
								ClusterState:      embed.ClusterStateFlagNew,
							},
							ClusterName: "standup-one-server",
						}),
				},
			},
			stopTimeout: pointer.Duration(time.Second * 1),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDirs := make([]string, 0, len(tt.args))
			defer testTearDownDirectories(t, tempDirs)

			servers := make([]*Server, 0, len(tt.args))
			defer testTearDownCluster(t, servers)

			// iterate over each server config
			for _, a := range tt.args {

				t.Log(a.etcdCfg.Name)

				// create the etcd server directory
				var err error
				if a.etcdCfg.Dir, err = testCreateEtcdDir(t, tempDirs); err != nil {
					t.Errorf("failed to create temporary data dir for etcd server %v", err)
					return
				}

				// start the server
				if _, servers, err = testStartServer(t, a.etcdCfg, a.startTimeout, servers); (err != nil) != a.wantStartErr {
					t.Errorf("New() expected error while starting up server %s [%v] but didn't get error = %v, wantErr %v", a.etcdCfg.Name, a.etcdCfg.APUrls, err, a.wantStartErr)
					return
				}
			}

			t.Log("Ensuring each member is up and aware of each other..")
			if err := testAssertConsistentClusterSize(t, servers); err != nil {
				t.Errorf(err.Error())
				return
			}
			t.Log("Forcibly cleaning up the cluster")
			for _, server := range servers {
				server.cleanUpStart(fmt.Errorf("bogus error"))
			}
		})
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

			config := NewConfig()
			config.Dir = etcdDir

			if err := s.Start(timeout, config); err != nil {
				t.Errorf("Server.waitForShutdown() failed to set up the server for test %v", err)
			}
			cancel()

			if err := s.waitForShutdown(tt.args.ctx, tt.args.done); (err != nil) != tt.wantErr {
				t.Errorf("Server.waitForShutdown() error = %v, wantErr %v", err, tt.wantErr)
			}
			// clean up tests
			s.Close()
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
			// timeout, cancel := context.WithTimeout(context.Background(), time.Second*5)

			config := NewConfig()
			config.Dir = etcdDir

			// add to the server wait group to avoid a negative waitgroup
			s.routineContext, s.routineCancel = context.WithCancel(context.Background())
			s.routineWg.Add(1)

			s.errorHandlerRoutine(s.routineContext, tt.args.stopCh, tt.args.errCh)
			if s.IsRunning() {
				t.Errorf("Server.errorHandlerRoutine() did not shutdownt he server")
			}
		})
	}
}

func TestConfig_GetClientFromConfig(t *testing.T) {
	cfg := Config{InitialCluster: []string{}}
	_, err := cfg.GetClientFromConfig(nil)
	if err != cli.ErrNoAvailableEndpoints {
		t.Errorf("Config.GetClientFromConfig() want = (%v) got = (%v)", cli.ErrNoAvailableEndpoints, err)
	}
}

// openFDCount returns the number of open file descriptors for the running process.
// This function is based on content from https://groups.google.com/forum/#!topic/golang-nuts/c0AnWXjzNIA
func openFDCount(t *testing.T) (lines int) {
	if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
		// get the pid for this process
		pid := os.Getpid()

		// build the command
		lsofCmd := fmt.Sprintf("lsof -p %v", pid)

		// execute the command in a shell and get the output
		out, err := exec.Command("/bin/sh", "-c", lsofCmd).Output()
		if err != nil {
			t.Errorf("a problem occurred while checking the number of open file descriptors %v", err)
		}

		// count the number of lines returned from the lsof command
		lines = bytes.Count(out, []byte("\n"))
	}
	return lines
}
