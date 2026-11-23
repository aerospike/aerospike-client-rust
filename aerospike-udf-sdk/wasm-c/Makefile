# The C guest SDK for Aerospike WASM UDFs is one header; this builds its examples.
#
#   make                    examples/vector/vector_c.wasm, examples/resolver/resolver_c.wasm and
#                           examples/apitest/apitest_c.wasm
#   make CLANG=clang-18     a specific clang; wasm-ld must be on the path (lld)
#   make SIMD=1             with 128-bit SIMD (see SIMD below)
#
# Freestanding: no libc, no entry point, every non-static function exported. The header provides the memcpy
# and memset the compiler emits calls to. clang gives a module 64 KB of linear-memory stack, which these
# examples fit in because they keep their buffers in statics; a module with large locals links with
# `-Wl,-z,stack-size=N`, since an overflow is not caught at build time and traps the call.

CLANG ?= clang
CFLAGS ?= -O3
# SIMD=1 adds -msimd128: the vector kernel's four-lane sums in 128-bit registers, which mod-wamr compiles to
# NEON or SSE ahead of time. Off by default, because a module built with it is refused at registration by a
# server whose WASM tier has no SIMD - Fast JIT, the x86-64 default - where the same source without it runs
# scalar. Build with it for aarch64 and aot nodes.
SIMD ?= 0
WASM_FLAGS = --target=wasm32 -nostdlib -ffreestanding -Wall -Wextra -Wl,--no-entry \
	-Wl,--export-dynamic
ifeq ($(SIMD),1)
  WASM_FLAGS += -msimd128
endif

.PHONY: all clean
all: examples/vector/vector_c.wasm examples/resolver/resolver_c.wasm examples/apitest/apitest_c.wasm

examples/vector/vector_c.wasm: examples/vector/vector.c include/aerospike_udf.h
	$(CLANG) $(CFLAGS) $(WASM_FLAGS) -Iinclude -o $@ $<

examples/resolver/resolver_c.wasm: examples/resolver/resolver.c include/aerospike_udf.h
	$(CLANG) $(CFLAGS) $(WASM_FLAGS) -Iinclude -o $@ $<

examples/apitest/apitest_c.wasm: examples/apitest/apitest.c include/aerospike_udf.h
	$(CLANG) $(CFLAGS) $(WASM_FLAGS) -Iinclude -o $@ $<

clean:
	rm -f examples/vector/vector_c.wasm examples/resolver/resolver_c.wasm examples/apitest/apitest_c.wasm
