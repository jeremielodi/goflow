// Command protogen compiles GoFlow's .proto files and invokes the
// protoc-gen-go / protoc-gen-go-grpc plugins directly, entirely in Go — no
// protoc or buf CLI binary required. Used because this environment's network
// made downloading those large binaries unreliable; protocompile (a pure-Go
// .proto parser) plus the already-`go install`-able codegen plugins avoids
// that entirely.
//
// Usage: go run ./tools/protogen
package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

type compileJob struct {
	protoDir  string
	protoFile string
	outDir    string
}

var jobs = []compileJob{
	{protoDir: "internal/zeebegrpc/proto", protoFile: "gateway.proto", outDir: "internal/zeebegrpc/pb"},
	{protoDir: "internal/eventlog/proto", protoFile: "events.proto", outDir: "internal/eventlog/pb"},
}

func main() {
	for _, job := range jobs {
		compileOne(job)
	}
	fmt.Println("done")
}

func compileOne(job compileJob) {
	// WithStandardImports lets .proto files `import "google/protobuf/*.proto"`
	// (e.g. Timestamp) without those files existing on disk under protoDir —
	// protocompile has the well-known-type descriptors built in.
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(
			&protocompile.SourceResolver{ImportPaths: []string{job.protoDir}},
		),
	}
	files, err := compiler.Compile(context.Background(), job.protoFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "compile error:", err)
		os.Exit(1)
	}

	// protoc-gen-go resolves types purely from req.ProtoFile, so the request
	// must include the full transitive import closure (e.g.
	// google/protobuf/timestamp.proto), not just the top-level requested
	// file — mirrors what protoc itself does when driving a plugin.
	seen := map[string]*descriptorpb.FileDescriptorProto{}
	for _, f := range files {
		collectDescriptors(f, seen)
	}
	fdProtos := make([]*descriptorpb.FileDescriptorProto, 0, len(seen))
	for _, fd := range seen {
		fdProtos = append(fdProtos, fd)
	}

	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{job.protoFile},
		ProtoFile:      fdProtos,
	}

	runPlugin("protoc-gen-go", "paths=source_relative", req, job.outDir)
	runPlugin("protoc-gen-go-grpc", "paths=source_relative", req, job.outDir)
}

// collectDescriptors walks f's import graph and adds a FileDescriptorProto
// for f and every file it (transitively) imports into seen, keyed by path.
func collectDescriptors(f protoreflect.FileDescriptor, seen map[string]*descriptorpb.FileDescriptorProto) {
	if _, ok := seen[f.Path()]; ok {
		return
	}
	seen[f.Path()] = protodesc.ToFileDescriptorProto(f)
	imports := f.Imports()
	for i := 0; i < imports.Len(); i++ {
		collectDescriptors(imports.Get(i).FileDescriptor, seen)
	}
}

func runPlugin(name, param string, req *pluginpb.CodeGeneratorRequest, outDir string) {
	req.Parameter = proto.String(param)
	reqBytes, err := proto.Marshal(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal request:", err)
		os.Exit(1)
	}

	cmd := exec.Command(name)
	cmd.Stdin = bytes.NewReader(reqBytes)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s failed: %v\n%s\n", name, err, stderr.String())
		os.Exit(1)
	}

	var resp pluginpb.CodeGeneratorResponse
	if err := proto.Unmarshal(out, &resp); err != nil {
		fmt.Fprintln(os.Stderr, "unmarshal response:", err)
		os.Exit(1)
	}
	if resp.Error != nil {
		fmt.Fprintf(os.Stderr, "%s reported error: %s\n", name, *resp.Error)
		os.Exit(1)
	}

	for _, f := range resp.File {
		path := filepath.Join(outDir, f.GetName())
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := os.WriteFile(path, []byte(f.GetContent()), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("wrote", path)
	}
}
