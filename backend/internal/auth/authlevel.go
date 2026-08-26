package auth

import (
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// procedureLevels returns the authorization table keyed by Connect procedure
// ("/gastrolog.v1.AuthService/Login"). It is derived from the method
// descriptors, so a level travels with the RPC that declares it and the table
// cannot fall behind the schema. Built once; the descriptors are immutable.
//
// This covers Connect procedures only. Plain http.Handler routes on the same
// mux carry their own checks and no declaration can reach them: the
// managed-file upload endpoint (POST /api/v1/managed-files/upload) authorizes
// through the shared Verifier at admin level, /cluster/bootstrap-token
// compares a shared secret, the /healthz and /readyz probes are deliberately
// unauthenticated, and the embedded frontend assets under "/" are served
// unauthenticated. A new non-Connect route is invisible to the completeness
// test as well — it must gate itself.
var procedureLevels = sync.OnceValue(buildProcedureLevels)

// buildProcedureLevels walks every registered service and records the level
// each method declares. A method that declares none is left out of the table,
// which the interceptor treats as a denial.
func buildProcedureLevels() map[string]apiv1.AuthLevel {
	levels := make(map[string]apiv1.AuthLevel)
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := range services.Len() {
			svc := services.Get(i)
			methods := svc.Methods()
			for j := range methods.Len() {
				method := methods.Get(j)
				level := declaredAuthLevel(method)
				if level == apiv1.AuthLevel_AUTH_LEVEL_UNSPECIFIED {
					continue
				}
				levels[procedureName(svc, method)] = level
			}
		}
		return true
	})
	return levels
}

// declaredAuthLevel reads the auth_level method option, or UNSPECIFIED when the
// method carries no such option.
func declaredAuthLevel(method protoreflect.MethodDescriptor) apiv1.AuthLevel {
	level, ok := proto.GetExtension(method.Options(), apiv1.E_AuthLevel).(apiv1.AuthLevel)
	if !ok {
		return apiv1.AuthLevel_AUTH_LEVEL_UNSPECIFIED
	}
	return level
}

// procedureName renders the Connect procedure path for a method, matching
// connect.Spec().Procedure.
func procedureName(svc protoreflect.ServiceDescriptor, method protoreflect.MethodDescriptor) string {
	return "/" + string(svc.FullName()) + "/" + string(method.Name())
}
