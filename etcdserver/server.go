package etcdserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"

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

// ServerNameConflicts returns true if the server name conflicts or returns false if it doesn't
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

// getServerPeers returns the peer urls for the cluster formatted for the initialCluster server configuration.
// The context that is passed in should have a configured timeout.
func getServerPeers(ctx context.Context, c *etcdclient.Client, initialCluster ...string) (peers string, err error) {
	if len(initialCluster) > 0 {
		peers = initialCluster[0]
	}

	var members *cli.MemberListResponse
	// get the list of members
	members, err = c.MemberList(ctx)

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

	return peers, err
}

// Server manages an etcd embedded server
type Server struct {
	// the currently running etcd server
	*embed.Etcd

	// the config for the current etcd server
	config *Config

	// mutex for managing the server
	mutex sync.RWMutex

	// routine context
	routineContext context.Context

	// cancel
	routineCancel context.CancelFunc

	// routineCount keeps track of long running go routines that are started by Start()
	routineCount int32

	// routineWg is a wait group used to wait for running routines to complete
	routineWg sync.WaitGroup
}

// indicates whether the server has been stopped or not and is not thread safe
func (s *Server) isRunning() bool {
	if s != nil {
		//// check for running go routines, if they exist then return true
		//if atomic.LoadInt32(&s.routineCount) > 0 {
		//	return true
		//}
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

func (s *Server) addMemberToExistingCluster(ctx context.Context, tempcli *etcdclient.Client) (err error) {
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

	return err
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

	// keep trying to get the cluster peers
	for ctx.Err() == nil {

		// get an etcdclient to the cluster using the config file
		tempcli, err = s.config.GetClientFromConfig(ctx)

		if err == nil {

			// set up the temp cli for the cluster namespace
			setupClusterNamespace(tempcli)

			// verify that the cluster name matches before we add a member to the cluster
			// if you add the member to the cluster and check for the cluster name before you start the server,
			// then you run the risk of breaking quorum and stalling out on the cluster name check
			resp, err := tempcli.Get(ctx, "/name")
			if err != nil || len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) != s.config.ClusterName {
				return etcderrors.ErrClusterNameConflict
			}

			// get the peer address string for joining the cluster
			s.config.Config.InitialCluster, err = getServerPeers(ctx, tempcli, s.config.InitialClusterFromName(s.config.Name))

			if err == nil {
				// announce to the cluster that we're going to add this server
				err = s.addMemberToExistingCluster(ctx, tempcli)
			}

			tempcli.Close()

			if err == nil {
				return nil
			}
		}
	}
	return err
}

// startupValidation validates if the clsuter is running and that the config file makes sense
func (s *Server) startupValidation(cfg *Config) error {
	// return if the server is already running
	if s.isRunning() {
		return etcderrors.ErrAlreadyRunning
	}

	// validate the etcd configuration
	return cfg.Validate()
}

// Start starts the server with the given config
func (s *Server) Start(ctx context.Context, cfg *Config) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// validate current state and config
	if err = s.startupValidation(cfg); err != nil {
		return err
	}

	// remove the data dir because we require each server to be completely removed
	// from the cluster before we can rejoin
	// TODO: if we ever use snapshotting or want to restore a cluster this will need to be revised
	err = os.RemoveAll(cfg.Dir)

	// save the config to the server for reference
	s.config = cfg

	// prepare the server to start
	if s.config.ClusterState == embed.ClusterStateFlagNew {
		err = s.prepareForNewCluster(ctx)
	} else {
		err = s.prepareForExistingCluster(ctx)
	}

	// start the server
	if err == nil {
		// create a context for this server
		s.Etcd, err = embed.StartEtcd(cfg.Config)
	}

	// wait for the server to be ready or error out
	if err == nil {
		err = common.WaitForStructChOrErrCh(ctx, s.Etcd.Server.ReadyNotify(), s.Etcd.Err())
	}

	// initialize the server
	if err == nil {
		err = s.initializeAdditionalServerRoutines(ctx, s.Etcd, cfg)
	}

	// after the server successfully started if there is any error we should shut it down on our way out
	if err != nil && s.isRunning() {
		s.Shutdown(context.Background())
	}

	return err
}

// waits for the etcd server to stop or return an err and then revokes the member key lease and closes the client
func memberKeyRoutine(ctx context.Context, client *etcdclient.Client, lease *cli.LeaseGrantResponse, stopNotify <-chan struct{}, errCh <-chan error) {
	// wait for the server to stop or for an error to occur
	common.WaitForStructChOrErrCh(ctx, stopNotify, errCh)

	// revoke the lease when the server stops or gets into an error state
	// swallow the error because if the server is stopped we expect an error to occur on revocation
	_, _ = client.Revoke(context.Background(), lease.ID)

	// close the client
	client.Close()
}

// errorHandlerRoutine waits for errors to occur and attempts
// to shutdown the cluster with a 30 second timeout
func (s *Server) errorHandlerRoutine(ctx context.Context, stopCh <-chan struct{}, errCh <-chan error) {
	select {
	case <-ctx.Done():
		//atomic.AddInt32(&s.routineCount, -1)
		s.routineWg.Done()
	case <-stopCh:
		//atomic.AddInt32(&s.routineCount, -1)
		s.routineWg.Done()
	case <-errCh:
		// encountered an error
		// shutdown the server if it is running
		timeout, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
		// Shutdown() blocks and waits for the wait group
		// since this routine is part of that wait group we need to mark it as done before invoking shutdown
		//atomic.AddInt32(&s.routineCount, -1)
		s.routineWg.Done()
		s.Shutdown(timeout)
		cancel()
	}
}

// clusterCleanupRoutine iteratively checks member health and removes bad members
func (s *Server) clusterCleanupRoutine(ctx context.Context, stopCh <-chan struct{}, ttl *time.Duration, cleanUpInterval *time.Duration, client *etcdclient.Client) {
	// set up ticker
	ticker := time.NewTicker(common.DurationOrDefault(cleanUpInterval, DefaultCleanUpInterval))
	defer ticker.Stop()

	// close the client on exit
	defer client.Close()

	// members is a map of known members and the times the member was discovered and last seen healthy
	members := NewMembers(client)

	// continuously check the cluster health on the ticker interval
	for {
		select {
		case <-ctx.Done():
			return
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

				// members.Clean will use this map to check for members that may have already moved
				currentMemberIDs := make(map[uint64]struct{}, len(currentMembers))

				// get the cluster member health concurrently
				for _, cmember := range currentMembers {

					// wait to check health until the member is listed as started
					if !cmember.IsStarted() {
						continue
					}

					currentMemberIDs[uint64(cmember.ID)] = struct{}{}

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
				members.Clean(ttl, nil, nil, currentMemberIDs, ctx)

				// clean up any members that exceed their ttl and ignore context deadline exceeded error
				_ = unlock(ctx)
			}
		}
	}
}

// initializeAdditionalServerRoutines launches routines for managing the etcd server
// including periodic member/server clean up, etcd server errors, and keeping a key alive for this server/member instance.
func (s *Server) initializeAdditionalServerRoutines(ctx context.Context, server *embed.Etcd, cfg *Config) (err error) {
	// set up the memberKeyClient client used to keep the member key alive
	var memberKeyClient *etcdclient.Client

	// s.newServerClient() creates a new etcd client that doesn't go out over the wire via grpc,
	// but rather invokes functions directly on the server itself.  This should be fast.
	memberKeyClient, err = s.newServerClient()

	// create cancelable context to signal for the routines to stop.  Shutdown() will cancel the context
	s.routineContext, s.routineCancel = context.WithCancel(context.Background())

	// store the member name because we've successfully started up
	if err == nil {
		// TODO: log this in the future when a upcoming version of etcd gives us access to the etcd logger
		// put the cluster name will put the configured cluster name
		// this should already have been validated by join if we're joining an existing cluster
		_, err = memberKeyClient.Put(ctx, "/name", cfg.ClusterName)
	}

	// Use the server client to create a key for this server/member under "__etcd-cluster__/members/<name>" with a keep alive lease
	// this lease will expire when the server goes down indicating to the rest of the cluster that the server actually went down
	// this offers a little more protection for when a member is unhealthy but still sending keep alives
	var lease *cli.LeaseGrantResponse
	lease, _, err = memberKeyClient.PutWithKeepAlive(ctx, path.Join("", "members", cfg.Name), fmt.Sprintf("%v", s.Server.ID()), 5)

	// Set up the clusterCleanUp client for the cluster clean up routine to use.
	// s.newServerClient() creates a new etcd client that doesn't go out over the wire via grpc,
	// but rather invokes functions directly on the server itself.  This should be fast.
	var clusterCleanUpClient *etcdclient.Client
	clusterCleanUpClient, err = s.newServerClient()

	// actually launch the routines
	if err == nil {
		s.routineWg.Add(3)

		// routine to handle revocation of the lease
		go func() {
			memberKeyRoutine(s.routineContext, memberKeyClient, lease, server.Server.StopNotify(), s.Err())
			s.routineWg.Done()
		}()

		// routine to shutdown the cluster on error
		go s.errorHandlerRoutine(s.routineContext, s.Server.StopNotify(), s.Err())

		// routine to remove unhealthy members from the cluster
		go func() {
			s.clusterCleanupRoutine(s.routineContext, s.Server.StopNotify(), nil, nil, clusterCleanUpClient)
			s.routineWg.Done()
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

	// stop any running routines
	if s.routineCancel != nil {
		s.routineCancel()
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
				unlock(timeout)
				// mask the member not found err because it could mean a cluster clean up routine cleaned us up already
				if err == nil || err == rpctypes.ErrMemberNotFound {
					err = nil
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

		// wait for the running routines to stop
		s.routineWg.Wait()

		// close the done channel
		close(done)
	}()

	// wait for the cluster to shutdown
	s.waitForShutdown(done, ctx)

	return err
}

// newServerClient returns a new client for the server with the server prefix
func (s *Server) newServerClient() (client *etcdclient.Client, err error) {
	// v3client.New() creates a new v3client that doesn't go out over grpc,
	// but rather goes directly through the server itself.  This should be fast!
	client = &etcdclient.Client{Client: v3client.New(s.Etcd.Server)}

	// this package reserves a key namespace defined by the constant
	setupClusterNamespace(client)

	return client, nil
}

// New returns a new etcd Server
func New() *Server {
	return &Server{mutex: sync.RWMutex{}}
}
