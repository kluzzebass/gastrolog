package auth

import "regexp"

// UsernamePattern is the shape every account name must have, wherever an
// account is created: the interactive registration and user-management RPCs
// and the initial admin credentials read from a file or the environment.
var UsernamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{3,64}$`)
