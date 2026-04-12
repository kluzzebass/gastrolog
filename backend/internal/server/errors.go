package server

import (
	"errors"
	"fmt"

	"connectrpc.com/connect"
)

// errRequired returns an InvalidArgument connect error for a missing
// required field. Replaces the pattern:
//
//	connect.NewError(connect.CodeInvalidArgument, errors.New("X required"))
func errRequired(field string) *connect.Error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s required", field))
}

// errInternal wraps an error as a CodeInternal connect error. Replaces:
//
//	connect.NewError(connect.CodeInternal, err)
func errInternal(err error) *connect.Error {
	return connect.NewError(connect.CodeInternal, err)
}

// errInvalidArg wraps an error as a CodeInvalidArgument connect error.
// Replaces:
//
//	connect.NewError(connect.CodeInvalidArgument, err)
func errInvalidArg(err error) *connect.Error {
	return connect.NewError(connect.CodeInvalidArgument, err)
}

// errNotFound wraps an error as a CodeNotFound connect error. Replaces:
//
//	connect.NewError(connect.CodeNotFound, err)
func errNotFound(err error) *connect.Error {
	return connect.NewError(connect.CodeNotFound, err)
}

// errPrecondition wraps an error as a CodeFailedPrecondition connect error.
func errPrecondition(err error) *connect.Error {
	return connect.NewError(connect.CodeFailedPrecondition, err)
}

// errAlreadyExists wraps an error as a CodeAlreadyExists connect error.
func errAlreadyExists(err error) *connect.Error {
	return connect.NewError(connect.CodeAlreadyExists, err)
}

// errUnauthenticated wraps an error as a CodeUnauthenticated connect error.
func errUnauthenticated(err error) *connect.Error {
	return connect.NewError(connect.CodeUnauthenticated, err)
}

// errRequiredMsg returns an InvalidArgument error with a custom message.
func errRequiredMsg(msg string) *connect.Error {
	return connect.NewError(connect.CodeInvalidArgument, errors.New(msg))
}
