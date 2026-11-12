# The Go guest SDK for Aerospike WASM UDFs, built with TinyGo; this builds its examples.
#
#   make                    examples/*/<name>_go.wasm
#   make fixtures           ... and copies them to tests/fixtures/wasm/
#   make SIMD=1             with 128-bit SIMD (see SIMD below)
#
# Each example is generated first: `go generate` runs cmd/udfgen over the package and writes
# udf_exports.go, the //export trampolines for its udf: directives. The generated file is checked in with
# the example, so a module builds without the step too.
#
# wasm-unknown: no WASI, nothing imported but the server's natives. No scheduler and no debug info; panics
# trap, which the host reports with the function's name. The leaking collector, because a module instance
# lives for one call or one stream and the hot paths allocate nothing per record.

# `wasm-unknown.json` inherits TinyGo's wasm-unknown target; `wasm-unknown-simd.json` is the same plus
# `+simd128`, so a kernel written as independent lanes vectorises and the server compiles it to NEON or SSE
# ahead of time. SIMD=1 selects it. Off by default, because a module built with it is refused at registration
# by a server whose WASM tier has no SIMD - Fast JIT, the x86-64 default - where the same source without it
# runs scalar. Build with it for aarch64 and aot nodes.
SIMD ?= 0
TARGET = $(if $(filter 1,$(SIMD)),./wasm-unknown-simd.json,./wasm-unknown.json)
TINYGO ?= tinygo
GO ?= go
TINYGO_FLAGS = -target=$(TARGET) -opt=2 -no-debug -panic=trap -scheduler=none -gc=leaking
EXAMPLES = vector resolver guard counter pncounter twopset lwwset crdt udf
WASM = $(foreach e,$(EXAMPLES),examples/$(e)/$(e)_go.wasm)
SDK = $(wildcard *.go) go.mod wasm-unknown.json wasm-unknown-simd.json
FIXTURES = ../tests/fixtures/wasm

.PHONY: all generate fixtures clean
all: $(WASM)

generate:
	$(GO) generate ./examples/...

examples/%_go.wasm: examples/%/main.go $(SDK) cmd/udfgen/main.go
	$(GO) generate ./examples/$*
	$(TINYGO) build -o $@ $(TINYGO_FLAGS) ./examples/$*

fixtures: all
	$(foreach e,$(EXAMPLES),cp -f examples/$(e)/$(e)_go.wasm $(FIXTURES)/$(e)_go.wasm;)

clean:
	rm -f $(WASM)
