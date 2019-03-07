package etcdclient

import (
	"context"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"path"
	"time"

	cli "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	common "github.com/signalfx/embedded-etcd/etcdcommon"
)

const (
	DefaultEndpointUpdateInterval = time.Second * 15
)

// Client wraps around an etcd v3 client and adds some helper functions
type Client struct {
	*cli.Client
}

// Updates the endpoints on the client periodically and can be run in a dedicated go routine.
func (c *Client) GetClusterClientEndpoints() (endpoints []string, err error) {
	var members *cli.MemberListResponse
	members, err = c.MemberList(c.Ctx())
	if err == nil && members.Members != nil {
		for _, member := range members.Members {
			endpoints = append(endpoints, member.GetClientURLs()...)
		}
	}
	return endpoints, err
}

// UpdateEndpointRoutine updates this client's endpoints until the client closes
func (c *Client) UpdateEndpointsRoutine(interval *time.Duration) {
	ticker := time.NewTicker(common.DurationOrDefault(interval, DefaultEndpointUpdateInterval))
	for {
		select {
		case <-c.Ctx().Done():
			return
		case <-ticker.C:
			endpoints, err := c.GetClusterClientEndpoints()
			if err == nil && len(endpoints) > 0 {
				c.SetEndpoints(endpoints...)
			}
		}
	}
}

// Lock accepts an etcd client, context (with cancel), and name and creates a concurrent lock
func (c *Client) Lock(ctx context.Context, name string) (unlock func(context.Context) error, err error) {
	var session *concurrency.Session
	session, err = concurrency.NewSession(c.Client)

	if err != nil {
		// and return now
		return unlock, err
	}

	// create a mutex using the session under /mutex/name
	mutex := concurrency.NewMutex(session, path.Join("", "mutex", name))

	// lock the mutex and return a function to unlock the mutex
	err = mutex.Lock(ctx)

	// set unlock function
	if err == nil {
		unlock = func(ctx context.Context) (err error) {
			// we need to return the first error we encounter
			// but we need to do both operations
			errs := make([]error, 2)
			errs[0] = mutex.Unlock(ctx)
			errs[1] = session.Close()

			// return first error
			for _, err := range errs {
				if err != nil {
					return err
				}
			}

			// return nil
			return nil
		}
	}

	return unlock, err
}

// New returns a new etcd v3client wrapped with some helper functions
func New(cfg cli.Config) (client *Client, err error) {
	var etcdClient *cli.Client

	if etcdClient, err = cli.New(cfg); err == nil {
		client = &Client{Client: etcdClient}
	}

	return client, err
}

// CheckMemberHealth checks whether an etcd member is healthy or not using it's client urls.
// The context that is passed in should have a configured timeout.
// This is modeled after etcdctl's method of checking endpoint health
// https://github.com/etcd-io/etcd/blob/v3.3.0/etcdctl/ctlv3/command/ep_command.go#L80
func CheckMemberHealth(ctx context.Context, clientURLs []string) bool {
	if client, err := New(cli.Config{Endpoints: clientURLs, Context: ctx}); err == nil {
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


