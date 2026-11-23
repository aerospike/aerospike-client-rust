# The Java guest SDK for Aerospike WASM UDFs, compiled with TeaVM; this builds its examples.
#
#   make                    examples/*/target/wasm/*_java.wasm
#   make fixtures FIXTURES=<dir>
#                           ... and copies them to <dir>
#
# Needs a JDK (17+) and Maven on the PATH; TeaVM comes from Maven Central on the first build. In this
# repository the toolchain lives in the `asd-build-wasm` container (see README.md), which has no view of
# this directory: copy it in with `podman cp`, build there, copy the .wasm files back.
MVN ?= mvn
# Where `make fixtures` copies the modules. No default: the SDK knows nothing of who uses them.
FIXTURES ?=

.PHONY: all fixtures clean
all:
	$(MVN) -q -B package

fixtures: all
	@test -n "$(FIXTURES)" || { echo "make fixtures FIXTURES=<directory>: where to copy the modules"; exit 1; }
	cp -f examples/hello/target/wasm/hello_java.wasm $(FIXTURES)/hello_java.wasm
	cp -f examples/vector/target/wasm/vector_java.wasm $(FIXTURES)/vector_java.wasm
	cp -f examples/resolver/target/wasm/resolver_java.wasm $(FIXTURES)/resolver_java.wasm
	cp -f examples/counter/target/wasm/counter_java.wasm $(FIXTURES)/counter_java.wasm
	cp -f examples/pncounter/target/wasm/pncounter_java.wasm $(FIXTURES)/pncounter_java.wasm
	cp -f examples/twopset/target/wasm/twopset_java.wasm $(FIXTURES)/twopset_java.wasm
	cp -f examples/lwwset/target/wasm/lwwset_java.wasm $(FIXTURES)/lwwset_java.wasm
	cp -f examples/registers/target/wasm/registers_java.wasm $(FIXTURES)/registers_java.wasm
	cp -f examples/crdt/target/wasm/crdt_java.wasm $(FIXTURES)/crdt_java.wasm
	cp -f examples/udf/target/wasm/udf_java.wasm $(FIXTURES)/udf_java.wasm
	cp -f examples/apitest/target/wasm/apitest_java.wasm $(FIXTURES)/apitest_java.wasm

clean:
	$(MVN) -q -B clean
