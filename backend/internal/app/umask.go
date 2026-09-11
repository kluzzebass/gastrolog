package app

import "syscall"

// restrictFileCreation makes every file this process creates owner-only,
// including the ones created by libraries.
//
// Raft snapshots carry the whole replicated config — the JWT signing
// secret, TLS private keys and cloud credentials — and hashicorp/raft
// creates them with os.Create, which is 0666 before the umask. Nothing in
// this tree can pass that library a mode, and the same is true of any
// future dependency that writes beside our data. Setting the umask here
// covers all of them at once.
//
// Set once at startup and never restored, so the previous value is
// discarded.
func restrictFileCreation() {
	_ = syscall.Umask(0o077)
}
