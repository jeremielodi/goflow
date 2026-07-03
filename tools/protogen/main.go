// Command protogen compiles internal/zeebegrpc/proto/gateway.proto and
// invokes the protoc-gen-go / protoc-gen-go-grpc plugins directly,
// entirely in Go — no protoc or buf CLI binary required. Used because
// this environment's network made downloading those large binaries
// unreliable; protocompile (a pure-Go .proto parser) plus the
// already-`go install`-able codegen plugins avoids that entirely.
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
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

const (
	protoDir  = "internal/zeebegrpc/proto"
	protoFile = "gateway.proto"
	outDir    = "internal/zeebegrpc/pb"
)

func main() {
	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{ImportPaths: []string{protoDir}},
	}
	files, err := compiler.Compile(context.Background(), protoFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "compile error:", err)
		os.Exit(1)
	}

	var fdProtos []*descriptorpb.FileDescriptorProto
	for _, f := range files {
		fdProtos = append(fdProtos, protodesc.ToFileDescriptorProto(f))
	}

	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{protoFile},
		ProtoFile:      fdProtos,
	}

	runPlugin("protoc-gen-go", "paths=source_relative", req)
	runPlugin("protoc-gen-go-grpc", "paths=source_relative", req)

	fmt.Println("done")
}

func runPlugin(name, param string, req *pluginpb.CodeGeneratorRequest) {
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
