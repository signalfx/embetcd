package etcdserver

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/etcdserver/membership"
	"github.com/signalfx/embedded-etcd/etcdclient"
	common "github.com/signalfx/embedded-etcd/etcdcommon"
	"path"
	"time"
)

// memberInfo
type Member struct {
	// stores the member
	*membership.Member
	// client used for checking the member's health
	Client     *etcdclient.Client
	// indicates the time the member was first discovered
	// this is used to account for member startup time
	Discovered time.Time
	// indicates the last time the member was seen healthy
	LastHealth time.Time
}

// healthRequestSuccessful attempts to request the health of the member using a client configured only to that
// member's endpoints.  If the request fails the client is bad.  If we get a permission denied error the cluster
// is still healthy.  This is how etcdctl checks member health.
func (m *Member) healthRequestSuccessful(ctx context.Context) bool {
	if m.Client != nil && m.Client.Ctx().Err() == nil {
			// the health key doesn't exist by default and it doesn't actually matter if it does
			// We just want a response without error and rpc permission denied errors are OK.
			// The response could be an error, it just doesn't matter.  If the endpoint/server is down,
			// the client will return an error.
			if _, err := m.Client.Get(ctx, "health"); ctx.Err() == nil && (err == nil || err == rpctypes.ErrPermissionDenied) {
				return true
			}
	}
	return false
}

// checks the health of the given member
func (m *Member) Update(client *etcdclient.Client) {
	// get the member key from the supplied client
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	resp, err := client.Get(ctx,  path.Join("", "members", m.Name))

	// update the last time the member was healthy
	// Check if the member is healthy and if the member's leased key is available.  The leased key is a protection
	// for when the cluster maybe in an temporarily unhealthy state but the members are still keeping their key alive
	if m.healthRequestSuccessful(ctx) || (err == nil && len(resp.Kvs) > 0) {
		m.LastHealth = time.Now()
	}

	cancel()
}

type Members struct {
	members map[uint64]*Member
	Client *etcdclient.Client
}

// get retrieves an existing member or creates a new memberInfo and returns it
func (m *Members) Get(member *membership.Member) (info *Member){
	var ok bool
	info, ok = m.members[uint64(member.ID)]

	// create the member if it doesn't
	if !ok {
		client, _ := etcdclient.New(clientv3.Config{Endpoints: member.ClientURLs})
		info = &Member{
			Client: client,
			Discovered: time.Now(),
			LastHealth: time.Time{},
		}

		// save the reference
		m.members[uint64(member.ID)] = info
	}

	// update the info with the incoming membership
	info.Member = member

	// update the member client endpoints
	info.Client.SetEndpoints(member.ClientURLs...)

	return info
}

// Remove a member from the memberInfo list
func (m *Members) Remove(id uint64) {
	if member, ok := m.members[id]; ok {
		member.Client.Close()
		delete(m.members, id)
	}
}

// Clean up members in the list that exceeded their ttl and their startup graceperiod
// Etcd is allergic to adding and removing multiple instances at the same time.
// This should be serial and not concurrent
func (m *Members) Clean(ttl *time.Duration, graceperiod *time.Duration, memberRemoveTimeout *time.Duration, ctx context.Context){
	for id, member := range m.members {
		if time.Since(member.Discovered) > common.DurationOrDefault(graceperiod, DefaultStartUpGracePeriod) && time.Since(member.LastHealth) > common.DurationOrDefault(ttl, DefaultUnhealthyTTL) {
			fmt.Println("REMOVING MEMBER ", member.Name, member.ID)
			if m.Client != nil {
				timeout, cancel := context.WithTimeout(ctx, common.DurationOrDefault(memberRemoveTimeout, DefaultDialTimeout))
				if _, err := m.Client.MemberRemove(timeout, uint64(member.ID)); err == rpctypes.ErrMemberNotFound || err == nil {
					// remove the member from our local tracker if there was no error removing it via the
					// etcd client or if the returned error is that the member wasn't found
					m.Remove(id)
				}
				cancel()
			}
		}
	}
}

func NewMembers(client *etcdclient.Client) *Members{
	return &Members{members: make(map[uint64]*Member), Client: client}
}