package chatterbox

import (
	"math/rand/v2"
	"strings"
	"time"
)

// MultirecordFormat generates stack dumps, command help output, and similar
// multi-line content as separate records.
type MultirecordFormat struct {
	pools *AttributePools
}

// NewMultirecordFormat creates a multi-record format generator.
func NewMultirecordFormat(pools *AttributePools) *MultirecordFormat {
	return &MultirecordFormat{pools: pools}
}

func (f *MultirecordFormat) GenerateMulti(rng *rand.Rand) []recordDraft {
	switch rng.IntN(4) {
	case 0:
		return f.genGoStack(rng)
	case 1:
		return f.genJavaStack(rng)
	case 2:
		return f.genHelpOutput(rng)
	default:
		return f.genPythonTrace(rng)
	}
}

func (f *MultirecordFormat) genGoStack(rng *rand.Rand) []recordDraft {
	lines := []string{
		"panic: runtime error: invalid memory address or nil pointer dereference",
		"[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x4930a2]",
		"",
		"goroutine 42 [running]:",
		"main.(*Handler).handleRequest(0x0, 0xc0001a4000)",
		"	/home/app/server.go:127 +0xa2",
		"main.(*Server).ServeHTTP(0xc0000a8000, 0x7f8b1234, 0xc0001a2000)",
		"	/home/app/server.go:89 +0x1b5",
		"net/http.(*conn).serve(0xc0001a6000, 0x7f8b5678)",
		"	/usr/local/go/src/net/http/server.go:1925 +0x7d",
		"created by net/http.(*Server).Serve",
		"	/usr/local/go/src/net/http/server.go:3012 +0x2e5",
	}
	return f.linesToDrafts(rng, lines, "format", "stack", "language", "go")
}

func (f *MultirecordFormat) genJavaStack(rng *rand.Rand) []recordDraft {
	lines := []string{
		"java.lang.NullPointerException: Cannot invoke \"String.length()\" because \"value\" is null",
		"	at com.example.service.UserService.validate(UserService.java:42)",
		"	at com.example.controller.UserController.create(UserController.java:78)",
		"	at jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)",
		"	at jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)",
		"	at java.base/java.lang.reflect.Method.invoke(Method.java:568)",
		"	at org.springframework.cglib.proxy.MethodProxy.invoke(MethodProxy.java:218)",
		"	at org.springframework.aop.framework.CglibAopProxy$CglibMethodInvocation.invokeJoinpoint(CglibAopProxy.java:783)",
	}
	return f.linesToDrafts(rng, lines, "format", "stack", "language", "java")
}

func (f *MultirecordFormat) genPythonTrace(rng *rand.Rand) []recordDraft {
	lines := []string{
		"Traceback (most recent call last):",
		"  File \"/app/worker.py\", line 156, in process_job",
		"    result = external_api.fetch(job_id)",
		"  File \"/app/external_api.py\", line 89, in fetch",
		"    return response.json()",
		"  File \"/opt/venv/lib/python3.11/site-packages/requests/models.py\", line 971, in json",
		"    return complexjson.loads(self.text, **kwargs)",
		"json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)",
	}
	return f.linesToDrafts(rng, lines, "format", "stack", "language", "python")
}

func (f *MultirecordFormat) genHelpOutput(rng *rand.Rand) []recordDraft {
	helpless := []string{
		"kubectl",
		"docker",
		"helm",
		"terraform",
		"aws",
		"gcloud",
	}
	cmd := pick(rng, helpless)
	var lines []string
	switch cmd {
	case "kubectl":
		lines = []string{
			"kubectl controls the Kubernetes cluster manager.",
			"",
			"Find more information at: https://kubernetes.io/docs/reference/kubectl/",
			"",
			"Basic Commands (Beginner):",
			"  create        Create a resource from a file or from stdin.",
			"  expose        Take a replication controller, service, deployment or pod and expose it as a new Kubernetes service",
			"  run           Run a particular image on the cluster",
			"  set           Set specific features on objects",
			"",
			"Basic Commands (Intermediate):",
			"  get           Display one or many resources",
			"  explain       Document the fields associated with each supported API resource",
			"  edit          Edit a resource on the server",
		}
	case "docker":
		lines = []string{
			"Usage:  docker [OPTIONS] COMMAND",
			"",
			"A self-sufficient runtime for containers",
			"",
			"Options:",
			"      --config string      Location of client config files",
			"  -c, --context string     Name of the context to use",
			"  -D, --debug              Enable debug mode",
			"  -H, --host list          Daemon socket(s) to connect to",
			"  -l, --log-level string   Set the logging level",
			"",
			"Management Commands:",
			"  builder     Manage builds",
			"  config      Manage Docker configs",
			"  container   Manage containers",
		}
	case "terraform":
		lines = []string{
			"Usage: terraform [global options] <subcommand> [options] [args]",
			"",
			"The available commands for execution are listed below.",
			"The most common, useful commands are shown first.",
			"",
			"Main commands:",
			"  init          Prepare your working directory for other commands",
			"  validate      Check whether the configuration is valid",
			"  plan          Show changes required by the current configuration",
			"  apply         Create or update infrastructure",
			"  destroy       Destroy previously-created infrastructure",
		}
	default:
		lines = []string{
			cmd + " - command line tool",
			"",
			"Usage: " + cmd + " [OPTIONS] COMMAND",
			"",
			"Options:",
			"  --help    Show this message",
			"  --version Print version",
		}
	}
	return f.linesToDrafts(rng, lines, "format", "help", "command", cmd)
}

func (f *MultirecordFormat) linesToDrafts(rng *rand.Rand, lines []string, extraPairs ...string) []recordDraft {
	baseAttrs := map[string]string{
		"service": pick(rng, f.pools.Services),
		"host":    pick(rng, f.pools.Hosts),
	}
	for i := 0; i < len(extraPairs); i += 2 {
		if i+1 < len(extraPairs) {
			baseAttrs[extraPairs[i]] = extraPairs[i+1]
		}
	}

	// Use a single source timestamp for the whole block (as if it was logged at once).
	sourceTS := time.Now().Add(-time.Duration(rng.IntN(3600)) * time.Second)

	drafts := make([]recordDraft, 0, len(lines))
	for _, line := range lines {
		// Preserve leading indentation (tabs/spaces) to match real-world output.
		trimmed := strings.TrimRight(line, " \t\r\n")
		if trimmed == "" {
			continue // Skip blank lines
		}
		attrs := make(map[string]string, len(baseAttrs))
		for k, v := range baseAttrs {
			attrs[k] = v
		}
		drafts = append(drafts, recordDraft{
			Raw:      []byte(trimmed),
			Attrs:    attrs,
			SourceTS: sourceTS,
		})
	}
	return drafts
}
