package etcdserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	cli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver/api/v3client"
	"github.com/signalfx/embedded-etcd/etcdclient"
	common "github.com/signalfx/embedded-etcd/etcdcommon"
	"github.com/signalfx/embedded-etcd/etcderrors"
)

const (
	EtcdClusterNamespace      = "__etcd-cluster__"
	DefaultUnhealthyTTL       = time.Second * 15
	DefaultCleanUpInterval    = time.Second * 5
	DefaultStartUpGracePeriod = time.Second * 5
	DefaultShutdownTimeout    = time.Minute * 1
	DefaultDialTimeout        = time.Second * 5
	DefaultAutoSyncInterval   = time.Second * 1
)

// setupClusterNamespace configures the client with the EtcdClusterNamespace prefix
func setupClusterNamespace(client *etcdclient.Client) {
	// this package reserves a key namespace defined by the constant
	client.KV = namespace.NewKV(client.KV, EtcdClusterNamespace)
	client.Watcher = namespace.NewWatcher(client.Watcher, EtcdClusterNamespace)
	client.Lease = namespace.NewLease(client.Lease, EtcdClusterNamespace)
}

type Config struct {
	*embed.Config
	ClusterName string
	InitialCluster []string
	CleanUpInterval *time.Duration
	DialTimeout *time.Duration
	AutoSyncInterval *time.Duration
	StartupGracePeriod *time.Duration
	UnhealthyTTL *time.Duration
}

// GetClientFromConfig returns a client with the supplied context from the config
func (c *Config) GetClientFromConfig(ctx context.Context) (cl *etcdclient.Client, err error){
	if ctx == nil {
		ctx = context.Background()
	}
	// create a client to the existing cluster
	cl, err = etcdclient.New(cli.Config{
		Endpoints:        c.InitialCluster,
		DialTimeout:      common.DurationOrDefault(c.DialTimeout, DefaultDialTimeout),
		TLS:              &tls.Config{InsecureSkipVerify: true}, // insecure for now
		AutoSyncInterval: common.DurationOrDefault(c.AutoSyncInterval, DefaultAutoSyncInterval),
		Context:          ctx, // pass in the context so the temp client closes with a cancelled context
	})
	return
}

// NewConfig returns a new config object with defaults provided by etcd embed
func NewConfig() *Config {
	return &Config{Config: embed.NewConfig()}
}

// Server manages an etcd embedded server
type Server struct {
	// the currently running etcd server
	*embed.Etcd

	// the config for the current etcd server
	config *Config

	// mutex for managing the server
	mutex sync.RWMutex

	// routineCount keeps track of long running go routines that are started by Start()
	routineCount int32
}

// indicates whether the server has been stopped or not and is not thread safe
func (s *Server) isRunning() bool {
	if s != nil {
		// check for running go routines, if they exist then return true
		if atomic.LoadInt32(&s.routineCount) > 0 {
			return true
		}
		if s.Etcd != nil && s.Etcd.Server != nil {
			select {
			case <-s.Etcd.Server.StopNotify():
				// StopNotify() returns a channel that returns nil when the server is stopped
				return false
			default:
				// if StopNotify is blocking then return true
				return true
			}
		}
	}

	return false
}

// IsRunning indicates whether the server has been stopped or not and is thread safe
func (s *Server) IsRunning() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.isRunning()
}

func (s *Server) addMemberToExistingCluster (ctx context.Context, tempcli *etcdclient.Client) (err error){
	// loop while the context hasn't closed
	var conflict bool
	for ctx.Err() == nil {
		// create a child context with its own timeout for checking if we conflict with the cluster
		timeout, cancel := context.WithTimeout(ctx, time.Second*5)

		// TODO: lock on a synchronized mutex in etcd to prevent multiple instances of the same name from joining at the same time

		// Ensure that the server name does not already exist in the cluster.
		// We want to ensure uniquely named cluster members.
		// If this member died and is trying to rejoin, we want to retry until
		// the cluster removes it or our parent context expires.
		conflict, err = ServerNameConflicts(ctx, tempcli, s.config.Name)

		if !conflict && err == nil {
			// if the server name doesn't conflict, cancel the timeout
			cancel()

			// create a child context with its own timeout for adding ourselves to the cluster
			timeout, cancel = context.WithTimeout(ctx, time.Second*5)

			// add the member
			_, err = tempcli.MemberAdd(timeout, common.URLSToStringSlice(s.config.APUrls))

			// break out of loop if we added ourselves cleanly
			if err == nil && timeout.Err() == nil {
				cancel()
				break
			}
		}

		// wait for the until timeout to try again
		<-timeout.Done()

		// cancel the timeout context
		cancel()
	}
	fmt.Println("BROKE OUT", err)
	return err
}

// startupValidation validates if the clsuter is running and that the config file makes sense
func (s *Server) startupValidation(cfg *Config) error{
	// return if the server is already running
	if s.isRunning() {
		return etcderrors.ErrAlreadyRunning
	}

	// validate the etcd configuration
	return cfg.Validate()
}

// prepare a new cluster
func (s *Server) prepareForNewCluster(ctx context.Context) (err error) {
	s.config.Config.InitialCluster = s.config.InitialClusterFromName(s.config.Name)
	return err
}

// prepare for an existing cluster
func (s *Server) prepareForExistingCluster(ctx context.Context) (err error) {
	// create a temporary client
	var tempcli *etcdclient.Client

	// get a etcdclient to the cluster using the config file
	tempcli, err = s.config.GetClientFromConfig(ctx)

	if err == nil {
		// defer closing the temp cli
		defer tempcli.Close()

		// set up the temp cli for the cluster namespace
		setupClusterNamespace(tempcli)

		// verify that the cluster name matches before we add a member to the cluster
		// if you add the member to the cluster and check for the cluster name before you start the server,
		// then you run the risk of breaking quorum and stalling out on the cluster name check
		if err == nil {
			// validate that the cluster name isn't already in use
			resp, err := tempcli.Get(ctx, "/name")
			if err != nil || len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) != s.config.ClusterName {
				return etcderrors.ErrClusterNameConflict
			}
		}

		// get the peer address string for joining the cluster
		s.config.Config.InitialCluster = getServerPeers(ctx, tempcli, s.config.InitialClusterFromName(s.config.Name))

		fmt.Println(s.config.Config.InitialCluster)

		// announce to the cluster that we're going to add this server
		err = s.addMemberToExistingCluster(ctx, tempcli)
	}



	return err
}

// Start starts the server with the given config
func (s *Server) Start(ctx context.Context, cfg *Config) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// validate current state and config
	if err = s.startupValidation(cfg); err != nil {
		return err
	}

	// save the config to the server for reference
	s.config = cfg

	// prepare the server to start
	if s.config.ClusterState == embed.ClusterStateFlagNew{
		err = s.prepareForNewCluster(ctx)
	} else {
		err = s.prepareForExistingCluster(ctx)
	}

	// remove the data dir because we require each server to be completely removed
	// from the cluster before we can rejoin
	// TODO: if we ever use snapshotting or want to restore a cluster this will need to be revised
	err = os.RemoveAll(cfg.Dir)

	fmt.Println("JOINED the cluster")


	// start the server
	if err == nil {
		// create a context for this server
		s.Etcd, err = embed.StartEtcd(cfg.Config)
	}

	// wait for the server to be ready or error out
	if err == nil {
		err = common.WaitForStructChOrErrCh(ctx, s.Etcd.Server.ReadyNotify(), s.Etcd.Err())
	}

	if err == nil {
		// initialize the server
		err = s.initializeAdditionalServerRoutines(ctx, s.Etcd, cfg)
	}

	// after the server successfully started if there is any error we should shut it down on our way out
	if err != nil && s.isRunning() {
		s.Shutdown(context.Background())
	}

	return err
}

func (s *Server) initializeAdditionalServerRoutines(ctx context.Context, server *embed.Etcd, cfg *Config) (err error){

	// set up the cluster client
	var client *etcdclient.Client
	if err == nil {
		// s.newServerClient() creates a new etcd client that doesn't go out over the wire via grpc,
		// but rather invokes functions directly on the server itself.  This should be fast.
		client, err = s.newServerClient()
	}

	// store the member name because we've successfully started up
	if err == nil {
		// TODO: log this in the future when a upcoming version of etcd gives us access to the etcd logger
		// put the cluster name will put the configured cluster name
		// this should already have been validated by join if we're joining an existing cluster
		_, err = client.Put(ctx, "/name", cfg.ClusterName)
	}

	// set up go routines to handle stuff
	if err == nil {
		err = s.memberKeyRoutine(ctx, client, cfg.Name, server.Server.StopNotify(), s.Err())
	}

	if err == nil {
		go s.errorHandlerRoutine(s.Server.StopNotify(), s.Err()) // routine to shutdown the cluster on error
		go s.clusterCleanupRoutine(s.Server.StopNotify(), nil, nil, client) // routine to remove unhealthy members from the cluster
	}


	return err
}

// start routine to keep the member's key alive
func (s *Server) memberKeyRoutine(ctx context.Context, client *etcdclient.Client, name string, stopNotify <-chan struct{}, errCh <-chan error) (err error) {
	var lease *cli.LeaseGrantResponse
	// create a new lease with a 5 second ttl
	lease, err = client.Grant(context.Background(), 5)

	// keep the lease alive if we successfully put the key in
	if err == nil {
		_, err = client.KeepAlive(context.Background(), lease.ID)
	}

	// put the key in
	if err == nil {
		_, err = client.Put(ctx, path.Join("", "members", name), fmt.Sprintf("%v", s.Server.ID()), cli.WithLease(lease.ID))
	}

	if err == nil {
		// launch go routine to handle revocation of the lease
		go func() {
			// account for the routine
			atomic.AddInt32(&s.routineCount, 1)
			defer atomic.AddInt32(&s.routineCount, -1)

			// wait for the server to stop or for an error to occur
			common.WaitForStructChOrErrCh(context.Background(), stopNotify, errCh)

			// revoke the lease when the server stops or gets into an error state
			// swallow the error because if the server is stopped we expect an error to occur on revocation
			_, _ = client.Revoke(context.Background(), lease.ID)

			// close the client if we're the last open go routine
			if atomic.LoadInt32(&s.routineCount) == 1 {
				client.Close()
			}
		}()
	}

	return err
}

// waitForShutdown waits for the context to conclude, or the done channel to return
func (s *Server) waitForShutdown(done chan struct{}, ctx context.Context) (err error) {
	select {
	// wait for the context to complete
	case <-ctx.Done():
		if ctx.Err() != nil {
			// we timed out so do a hard stop on the server
			if s != nil && s.Etcd != nil && s.Etcd.Server != nil {
				s.Server.HardStop()
				// invoke close after hard stop to free up what ever port we're bound too
				s.Close()
			}
		}
	case <-done:
		break
	}
	return
}

// Shutdown shuts down the server
func (s *Server) Shutdown(ctx context.Context) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if !s.isRunning() {
		return etcderrors.ErrAlreadyStopped
	}

	members := s.Server.Cluster().Members()

	endpoints := make([]string, 0, len(members))
	for _, member := range members {
		if len(member.ClientURLs) > 0 {
			endpoints = append(endpoints, member.ClientURLs...)
		}
	}

	// use a temporary client to try removing ourselves from the cluster
	var tempcli *etcdclient.Client
	tempcli, err = etcdclient.New(cli.Config{
		Endpoints:        endpoints,
		DialTimeout:      DefaultDialTimeout,
		TLS:              &tls.Config{InsecureSkipVerify: true}, // insecure for now
		AutoSyncInterval: DefaultAutoSyncInterval,
		Context:          ctx,
	})

	if err == nil {
		setupClusterNamespace(tempcli)
		defer tempcli.Close()
	}

	if len(members) > 1 {
		// loop while the context hasn't closed
		for ctx.Err() == nil {

			// create a child context with its own timeout
			timeout, cancel := context.WithTimeout(ctx, time.Second*10)

			// use the temporary client to try removing ourselves from the cluster
			var unlock func(context.Context) error
			if unlock, err = tempcli.Lock(timeout, s.Server.Cfg.Name); err == nil {
				_, err = tempcli.MemberRemove(ctx, uint64(s.Server.ID()))
				unlock(context.Background())
				if err == nil {
					break
				}
			}

			// wait for the until timeout to try again
			<-timeout.Done()

			// cancel the timeout context
			cancel()

		}
	}

	// try to gracefully close the server
	done := make(chan struct{})
	go func() {
		// close the server
		s.Close()
		close(done)
	}()

	// wait for the cluster to shutdown
	s.waitForShutdown(done, ctx)

	return err
}

// clusterCleanupRoutine iteratively checks member health and removes bad members
func (s *Server) clusterCleanupRoutine(stopCh <-chan struct{}, ttl *time.Duration, cleanUpInterval *time.Duration, client *etcdclient.Client) {
	// account for the routine
	atomic.AddInt32(&s.routineCount, 1)
	defer atomic.AddInt32(&s.routineCount, -1)

	// set up ticker
	ticker := time.NewTicker(common.DurationOrDefault(cleanUpInterval, DefaultCleanUpInterval))
	defer ticker.Stop()

	// set up a background context for the cluster cleanup operations create cancelable contexts from
	ctx := context.Background()

	// members is a map of known members and the times the member was discovered and last seen healthy
	members := NewMembers(client)

	// continuously check the cluster health on the ticker interval
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			if s != nil && s.Etcd != nil && s.Server != nil {

				timeout, cancel := context.WithTimeout(ctx, DefaultCleanUpInterval)
				unlock, err := client.Lock(timeout, "remove")
				cancel()
				if err != nil {
					continue
				}

				// get the list of members the server is aware of
				currentMembers := s.Server.Cluster().Members()

				// create a wait group to wait for health status from each member of the cluster
				wg := sync.WaitGroup{}

				// get the cluster member health concurrently
				for _, cmember := range currentMembers {

					// wait to check health until the member is listed as started
					if !cmember.IsStarted() {
						continue
					}

					// fetch the health of the member in a separate go routine
					wg.Add(1)
					go func(m *Member) {
						defer wg.Done()
						m.Update(client)
					}(members.Get(cmember))

				}

				// wait for all members to update their health status
				wg.Wait()

				// clean up the member list
				members.Clean(ttl, nil, nil, ctx)

				// clean up any members that exceed their ttl and ignore context deadline exceeded error
				_ = unlock(ctx)
			}
		}
	}
}

// errorHandlerRoutine waits for errors to occur and attempts
// to shutdown the cluster with a 30 second timeout
func (s *Server) errorHandlerRoutine(stopCh <-chan struct{}, errCh <-chan error) {
	// account for the routine
	atomic.AddInt32(&s.routineCount, 1)

	select {
	case <-stopCh:
		atomic.AddInt32(&s.routineCount, -1)
		return
	case <-errCh:
		// encountered an error
		// shutdown the server if it is running
		timeout, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
		defer cancel()			// close the client if we're the last open go routine
		atomic.AddInt32(&s.routineCount, -1)
		s.Shutdown(timeout)
	}
}

// returns true if the server name conflicts or returns false if it doesn't
func ServerNameConflicts(ctx context.Context, client *etcdclient.Client, name string) (bool, error) {
	// get the cluster members
	var members *cli.MemberListResponse
	var err error
	if members, err = client.MemberList(ctx); err == nil && ctx.Err() != context.DeadlineExceeded && members != nil {
		// add members to the initial cluster
		for _, member := range members.Members {
			if member.Name == name {
				return true, etcderrors.ErrNameConflict
			}
		}
		return false, nil
	}
	return true, err
}

// newServerClient returns a new client for the server with the server prefix
func (s *Server) newServerClient() (client *etcdclient.Client, err error) {
	if !s.isRunning() {
		return nil, etcderrors.ErrServerStopped
	}

	// v3client.New() creates a new v3client that doesn't go out over grpc,
	// but rather goes directly through the server itself.  This should be fast!
	client = &etcdclient.Client{Client:v3client.New(s.Etcd.Server)}

	// this package reserves a key namespace defined by the constant
	setupClusterNamespace(client)

	return client, nil
}

// getServerPeers returns the peer urls for the cluster formatted for the initialCluster server configuration.
// The context that is passed in should have a configured timeout.
func getServerPeers(ctx context.Context, c *etcdclient.Client, initialCluster ...string) (peers string) {
	if len(initialCluster) > 0 {
		peers = initialCluster[0]
	}

	// get the list of members
	members, err := c.MemberList(ctx)

	if err == nil && ctx.Err() != context.DeadlineExceeded {

		// add members to the initial cluster
		for _, member := range members.Members {

			// if there's at least one peer url add it to the initial cluster
			if pURLS := member.GetPeerURLs(); len(pURLS) > 0 {
				// the etcdCfg should already have the server's address so we can safely append ",%s=%s"
				for _, url := range member.GetPeerURLs() {
					peers = fmt.Sprintf("%s,%s=%s", peers, member.Name, url)
				}
			}

		}
	}

	return peers
}

// New returns a new etcd Server
func New() *Server {
	return &Server{mutex: sync.RWMutex{}}
}
