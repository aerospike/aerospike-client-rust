# The C++ guest SDK for Aerospike WASM UDFs is one header over the C one; this builds its examples.
#
#   make                    examples/*/<name>_cpp.wasm
#   make fixtures           ... and copies them to tests/fixtures/wasm/
#   make CLANG=clang-18     a specific clang; wasm-ld must be on the path (lld)
#   make SIMD=1             with 128-bit SIMD (see SIMD below)
#
# Freestanding C++20: no standard library, no exceptions, no RTTI, no entry point. The C header provides
# the memcpy and memset the compiler emits calls to; -fvisibility=hidden keeps everything but the
# attributed exports out of the export table. clang gives a module 64 KB of linear-memory stack, which
# holds these examples, whose parameters are small and whose big tables are static. A Text<N> or Bytes<N>
# parameter or local is N bytes of stack, several times over through the generated export: a module with
# large ones links with `-Wl,-z,stack-size=N`, as apitest does below (the header's "The stack" says how much).

CLANG ?= clang
CFLAGS ?= -O3
C_SDK = ../aerospike-udf-sdk-wasm-c/include
# SIMD=1 adds -msimd128: the vector kernel's four-lane sums in 128-bit registers, which mod-wamr compiles to
# NEON or SSE ahead of time. Off by default, because a module built with it is refused at registration by a
# server whose WASM tier has no SIMD - Fast JIT, the x86-64 default - where the same source without it runs
# scalar. Build with it for aarch64 and aot nodes.
SIMD ?= 0
WASM_FLAGS = -x c++ -std=c++20 --target=wasm32 -nostdlib -nostdinc++ -ffreestanding \
	-fno-exceptions -fno-rtti -fvisibility=hidden -Wall -Wextra -Wno-unused-parameter \
	-Wl,--no-entry
ifeq ($(SIMD),1)
  WASM_FLAGS += -msimd128
endif
EXAMPLES = vector resolver counter pncounter twopset lwwset registers crdt udf apitest
WASM = $(foreach e,$(EXAMPLES),examples/$(e)/$(e)_cpp.wasm)
FIXTURES = ../tests/fixtures/wasm

.PHONY: all fixtures clean
all: $(WASM)

examples/%_cpp.wasm: examples/%.cpp include/aerospike_udf.hpp $(C_SDK)/aerospike_udf.h
	$(CLANG) $(CFLAGS) $(WASM_FLAGS) -Iinclude -I$(C_SDK) -o $@ $<

# apitest takes its strings as Text<65536> parameters, which live on the stack: 4 MiB of it.
examples/apitest/apitest_cpp.wasm: WASM_FLAGS += -Wl,-z,stack-size=4194304

# Each example is examples/<name>/<name>.cpp.
.SECONDEXPANSION:
examples/%_cpp.wasm: examples/$$(notdir $$*)/$$(notdir $$*).cpp

fixtures: all
	$(foreach e,$(EXAMPLES),cp -f examples/$(e)/$(e)_cpp.wasm $(FIXTURES)/$(e)_cpp.wasm;)

clean:
	rm -f $(WASM)
