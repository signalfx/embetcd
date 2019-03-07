package etcderrors

import "fmt"

var (
	ErrNameConflict        = fmt.Errorf("server name is in conflict with an existing cluster member")
	ErrAlreadyRunning      = fmt.Errorf("server is already running")
	ErrAlreadyStopped      = fmt.Errorf("server is already stopped")
	ErrClusterNameConflict = fmt.Errorf("cluster name either does not exist in the cluster under '/_etcd-cluster/name' or is different from this server's cluster name")
	ErrServerStopped       = fmt.Errorf("server is stopped")
)