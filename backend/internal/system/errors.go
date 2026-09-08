package system

import "errors"

// Rejections a command can meet in the FSM. They travel as errors.Is targets
// on the node that applies the command, and as a FailedPrecondition status
// when a follower forwards the command to the leader, where the forwarder
// turns the status back into ErrCommandRejected.
var (
	// ErrIllegalNodeStateTransition is returned when a node state change is
	// not permitted from the node's current state.
	ErrIllegalNodeStateTransition = errors.New("illegal node state transition")

	// ErrNodeNotFound is returned when a command names a node the cluster
	// does not know.
	ErrNodeNotFound = errors.New("node not found")

	// ErrCommandRejected wraps a leader's rejection of a forwarded command
	// whose specific cause did not survive the wire: the command was well
	// formed but the leader's state did not permit it.
	ErrCommandRejected = errors.New("command rejected by the leader")
)
