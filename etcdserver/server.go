package etcdserver

import (
	"context"
	"crypto/tls"
	"fmt"
	cli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/etcd/etcdserver/api/v3client"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/coreos/etcd/pkg/types"
	"net/url"
	"os"
	"sync"
	"time"
)

const (
	EtcdClusterNamespace      = "__etcd-cluster__/"
	DefaultUnhealthyTTL       = time.Second * 15
	DefaultCleanUpInterval    = time.Second * 5
	DefaultStartUpGracePeriod = time.Second * 5
	DefaultShutdownTimeout    = time.Minute * 1
	DefaultDialTimeout        = time.Second * 5
	DefaultAutoSyncInterval   = time.Second * 1
)

var (
	ErrAlreadyRunning      = fmt.Errorf("server is already running")
	ErrAlreadyStopped      = fmt.Errorf("server is already stopped")
	ErrNameConflict        = fmt.Errorf("server name is in conflict with an existing cluster member")
	ErrClusterNameConflict = fmt.Errorf("cluster name either does not exist in the cluster under '/_etcd-cluster/name' or is different from this server's cluster name")
)

type Server struct {
	*embed.Etcd
	mutex sync.RWMutex
	// Client is an etcd client
	Client *cli.Client
}

// isRunning indicates whether the server has been stopped or not and is not thread safe
func (s *Server) isRunning() bool {
	if s != nil && s.Etcd != nil && s.Etcd.Server != nil {
		select {
		case <-s.Etcd.Server.StopNotify():
			// StopNotify() returns a channel that returns nil when the server is stopped
			return false
		default:
			// if StopNotify is blocking then return true
			return true
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

// waits for the ready channel or an error
func waitForReady(readyCh <-chan struct{}, errCh <-chan error) error {
	// wait for the server to start or error out
	select {
	case <-readyCh:
		return nil
	case err := <-errCh:
		return err
	}
}

// returns true if the server name conflicts or returns false if it doesn't
func ServerNameConflicts(ctx context.Context, client *cli.Client, name string) (bool, error) {
	// get the cluster members
	var members *cli.MemberListResponse
	var err error
	if members, err = client.MemberList(ctx); err == nil && ctx.Err() != context.DeadlineExceeded && members != nil {
		// add members to the initial cluster
		for _, member := range members.Members {
			if member.Name == name {
				return true, ErrNameConflict
			}
		}
		return false, nil
	}
	return true, err
}

// joinCluster notifies the exiting cluster that this server exists
// and will be joining shortly. It returns a string of peer urls
// formatted for *embed.Config.InitialCluster ["name=peerurl",]
func (s *Server) joinCluster(ctx context.Context, cfg *embed.Config, clusterName string, initialCluster []string, dialTimeout *time.Duration, autoSyncInterval *time.Duration) (peers string, err error) {
	// create a temporary client
	var tempcli *cli.Client

	// create a client to the existing cluster
	tempcli, err = cli.New(cli.Config{
		Endpoints:        initialCluster,
		DialTimeout:      duration(dialTimeout, DefaultDialTimeout),
		TLS:              &tls.Config{InsecureSkipVerify: true}, // insecure for now
		AutoSyncInterval: duration(autoSyncInterval, DefaultAutoSyncInterval),
		Context:          ctx, // pass in the context so the temp client closes with a cancelled context
	})

	if err != nil {
		return peers, err
	}

	if ctx.Err() != nil {
		return peers, ctx.Err()
	}

	defer tempcli.Close()

	setupClusterNamespace(tempcli)

	fmt.Println(clusterName)

	if resp, err := tempcli.Get(ctx, "name"); err != nil || len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) != clusterName {
		return peers, ErrClusterNameConflict
	} else {
		fmt.Println(clusterName)
		fmt.Println(resp)
		fmt.Println(string(resp.Kvs[0].Value) != clusterName)
	}

	peers = getServerPeers(ctx, tempcli, cfg.InitialClusterFromName(cfg.Name))

	var conflict bool
	// loop while the context hasn't closed
	for ctx.Err() == nil {

		// create a child context with its own timeout for checking if we conflict with the cluster
		timeout, cancel := context.WithTimeout(ctx, time.Second*5)

		// TODO: lock on a synchronized mutex in etcd to prevent multiple instances of the same name from joining at the same time

		// Ensure that the server name does not already exist in the cluster.
		// We want to ensure uniquely named cluster members.
		// If this member died and is trying to rejoin, we want to retry until
		// the cluster removes it or our parent context expires.
		conflict, err = ServerNameConflicts(ctx, tempcli, cfg.Name)

		if !conflict && err == nil {
			// if the server name doesn't conflict, cancel the timeout
			cancel()

			// create a child context with its own timeout for adding ourselves to the cluster
			timeout, cancel = context.WithTimeout(ctx, time.Second*5)

			// add the member
			_, err = tempcli.MemberAdd(timeout, URLSToStringSlice(cfg.APUrls))

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

	return peers, err
}

// Start starts the server with the given config
func (s *Server) Start(ctx context.Context, cfg *embed.Config, clusterName string, initialCluster []string, dialTimeout *time.Duration, autoSyncInterval *time.Duration) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// validate the etcd configuration
	if err = cfg.Validate(); err != nil {
		return err
	}

	if !s.isRunning() {

		// remove the data dir because we require each server to be completely removed
		// from the cluster before we can rejoin
		// TODO: if we ever use snapshotting or want to restore a cluster this will need to be revised
		_ = os.RemoveAll(cfg.Dir)

		if cfg.ClusterState == embed.ClusterStateFlagExisting {
			// join cluster will attempt to add the member to the cluster before starting the server
			// will retry for as long as the context timeout
			cfg.InitialCluster, err = s.joinCluster(ctx, cfg, clusterName, initialCluster, dialTimeout, autoSyncInterval)
			if err != nil {
				return err
			}
		} else {
			cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
		}

		// start the server
		if s.Etcd, err = embed.StartEtcd(cfg); err == nil {

			// v3client.New() creates a new v3client that doesn't go out over grpc,
			// but rather goes directly through the server itself.  This should be fast!
			s.Client = v3client.New(s.Etcd.Server)

			// this package reserves a key namespace defined by the constant
			setupClusterNamespace(s.Client)

			// wait for the server to start or error out
			err = waitForReady(s.Server.ReadyNotify(), s.Err())

			s.Client.Put(ctx, "name", clusterName)

			var lease *cli.LeaseGrantResponse
			// create a new lease with a 5 second ttl
			if lease, err = s.Client.Grant(context.Background(), 5); err == nil {
				s.Client.Put(ctx, fmt.Sprintf("members/%s", cfg.Name), string(time.Now().Unix()), cli.WithLease(lease.ID))
			}

			if err == nil {
				go s.errorHandlerRoutine(s.Server.StopNotify(), s.Err()) // routine to shutdown the cluster on error
				// TODO: pass through unhealthy member ttl and clean up interval
				go s.clusterCleanupRoutine(s.Server.StopNotify(), nil, nil) // routine to remove unhealthy members from the cluster
			}

		}

	} else {
		err = ErrAlreadyRunning
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

	if s.isRunning() {
		// loop while the context hasn't closed
		for ctx.Err() == nil {

			// create a child context with its own timeout for checking if we conflict with the cluster
			timeout, cancel := context.WithTimeout(ctx, time.Second*10)

			// remove the server from the cluster
			if s != nil && s.Client != nil {

				// if there's at least one member, we need to remove ourselves from the cluster
				members, err := s.Client.MemberList(ctx)
				if err == nil && len(members.Members) > 1 {
					_, err = s.Client.MemberRemove(ctx, uint64(s.Server.ID()))
				}

				if err == nil {
					// close the client
					s.Client.Close()
					break
				}
			}

			// wait for the until timeout to try again
			<-timeout.Done()

			// cancel the timeout context
			cancel()

		}

		// try to gracefully close the server
		done := make(chan struct{}, 1)
		go func() {
			// close the server
			s.Close()
			close(done)
		}()

		// wait for the cluster to shutdown
		s.waitForShutdown(done, ctx)
	} else {
		err = ErrAlreadyStopped
	}

	return err
}

type memberTimes struct {
	*membership.Member
	client     *cli.Client
	discovered time.Time
	lastHealth time.Time
}

// clusterCleanupRoutine iteratively checks member health and removes bad members
func (s *Server) clusterCleanupRoutine(stopCh <-chan struct{}, ttl *time.Duration, cleanUpInterval *time.Duration) {
	// set up ticker
	ticker := time.NewTicker(duration(cleanUpInterval, DefaultCleanUpInterval))
	defer ticker.Stop()

	members := make(map[types.ID]*memberTimes, 0)

	// continuously check the cluster health on the ticker interval
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			if s != nil && s.Etcd != nil && s.Server != nil {
				// get the list of members the server is aware of
				currentMembers := s.Server.Cluster().Members()

				// create a wait group to wait for health status from each member of the cluster
				wg := sync.WaitGroup{}

				// for each member get the cluster health concurrently
				for _, member := range currentMembers {

					fmt.Println(member, member.IsStarted())
					// wait to check health until the member is listed as started
					if member.IsStarted() {
						var times *memberTimes
						var ok bool
						if times, ok = members[member.ID]; !ok {
							times = &memberTimes{
								Member:     member,
								discovered: time.Now(),
								lastHealth: time.Time{},
							}
							members[member.ID] = times
						}

						// fetch the health of the member in a separate go routine
						wg.Add(1)
						go func() {
							// check the health of each member with a timeout
							ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
							if checkMemberHealth(ctx, member.ClientURLs) {
								fmt.Println(member, " is not healthy")
								times.lastHealth = time.Now()
							} else {
								fmt.Println(member, " is healthy")
							}

							cancel()
							wg.Done()
						}()

					}
				}

				// wait for all members to update their health status
				wg.Wait()

				fmt.Println("DONE WAITING")

				// clean up any members that exceed their ttl
				s.cleanUpMembers(members, ttl)
			}
		}
	}
}

// processes each member time to see if it should be removed
func (s *Server) cleanUpMembers(members map[types.ID]*memberTimes, ttl *time.Duration) {
	fmt.Println("CLEANING UP MEMBERS STARTED")
	fmt.Println(members)
	for id, member := range members {
		fmt.Println("CLEANING UP MEMBER", member.Member.ID)
		// if the member is past its grace period and has not been seen healthy since the ttl
		if time.Since(member.discovered) > DefaultStartUpGracePeriod && time.Since(member.lastHealth) > duration(ttl, DefaultUnhealthyTTL) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			if s != nil && s.Etcd != nil && s.Etcd.Server != nil {
				fmt.Printf("Peer is unhealthy removing server %v %v \n", member.Name, member.ID)
				if _, err := s.Server.RemoveMember(ctx, uint64(member.ID)); err == nil {
					delete(members, id)
				}
			}
			cancel()
		}

	}
}

// errorHandlerRoutine waits for errors to occur and attempts
// to shutdown the cluster with a 30 second timeout
func (s *Server) errorHandlerRoutine(stopCh <-chan struct{}, errCh <-chan error) {
	select {
	case <-stopCh:
		return
	case <-errCh:
		// encountered an error
		// shutdown the server if it is running
		timeout, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
		defer cancel()
		s.Shutdown(timeout)
	}
}

// New returns a new etcd Server
func New() *Server {
	return &Server{mutex: sync.RWMutex{}}
}

// checkMemberHealth checks whether a member is healthy or not using it's client urls.
// The context that is passed in should have a configured timeout.
// This is modeled after etcdctl's method of checking endpoint health
// https://github.com/etcd-io/etcd/blob/v3.3.0/etcdctl/ctlv3/command/ep_command.go#L80
func checkMemberHealth(ctx context.Context, clientURLs []string) bool {
	if client, err := cli.New(cli.Config{Endpoints: clientURLs, Context: ctx}); err == nil {
		defer client.Close()
		if ctx.Err() == nil {
			// the health key doesn't exist by default and it doesn't actually matter if it does
			// We just want a response without error and rpc permission denied errors are OK.
			// The response could be an error, it just doesn't matter.  If the endpoint/server is down,
			// the client will return an error.
			if _, err := client.Get(ctx, "health"); ctx.Err() == nil && (err == nil || err == rpctypes.ErrPermissionDenied) {
				return true
			}
		}
	}
	return false
}

// urls to string slice converts an array of url.URL to a slice of strings
func URLSToStringSlice(urls []url.URL) []string {
	strs := make([]string, 0, len(urls))
	for _, u := range urls {
		strs = append(strs, u.String())
	}
	return strs
}

// getServerPeers returns the peer urls for the cluster formatted for the initialCluster server configuration.
// The context that is passed in should have a configured timeout.
func getServerPeers(ctx context.Context, c *cli.Client, initialCluster ...string) (peers string) {
	if len(initialCluster) > 0 {
		peers = initialCluster[0]
	}

	// get the list of members
	if members, err := c.MemberList(ctx); err == nil && ctx.Err() != context.DeadlineExceeded {

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

// duration returns the pointed duration or the specified default
func duration(in *time.Duration, def time.Duration) time.Duration {
	if in != nil {
		return *in
	}
	return def
}

// setupClusterNamespace configures the client with the EtcdClusterNamespace prefix
func setupClusterNamespace(client *cli.Client) {
	// this package reserves a key namespace defined by the constant
	client.KV = namespace.NewKV(client.KV, EtcdClusterNamespace)
	client.Watcher = namespace.NewWatcher(client.Watcher, EtcdClusterNamespace)
	client.Lease = namespace.NewLease(client.Lease, EtcdClusterNamespace)
}
