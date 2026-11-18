# The Java guest SDK for Aerospike WASM UDFs, compiled with TeaVM; this builds its examples.
#
#   make                    examples/*/target/wasm/*_java.wasm
#   make fixtures           ... and copies them to tests/fixtures/wasm/
#
# Needs a JDK (17+) and Maven on the PATH; TeaVM comes from Maven Central on the first build. In this
# repository the toolchain lives in the `asd-build-wasm` container (see README.md), which has no view of
# this directory: copy it in with `podman cp`, build there, copy the .wasm files back.
MVN ?= mvn
FIXTURES = ../tests/fixtures/wasm

.PHONY: all fixtures clean
all:
	$(MVN) -q -B package

fixtures: all
	cp -f examples/hello/target/wasm/hello_java.wasm $(FIXTURES)/hello_java.wasm
	cp -f examples/vector/target/wasm/vector_java.wasm $(FIXTURES)/vector_java.wasm
	cp -f examples/guard/target/wasm/guard_java.wasm $(FIXTURES)/guard_java.wasm
	cp -f examples/resolver/target/wasm/resolver_java.wasm $(FIXTURES)/resolver_java.wasm
	cp -f examples/counter/target/wasm/counter_java.wasm $(FIXTURES)/counter_java.wasm
	cp -f examples/pncounter/target/wasm/pncounter_java.wasm $(FIXTURES)/pncounter_java.wasm
	cp -f examples/twopset/target/wasm/twopset_java.wasm $(FIXTURES)/twopset_java.wasm
	cp -f examples/lwwset/target/wasm/lwwset_java.wasm $(FIXTURES)/lwwset_java.wasm
	cp -f examples/crdt/target/wasm/crdt_java.wasm $(FIXTURES)/crdt_java.wasm
	cp -f examples/udf/target/wasm/udf_java.wasm $(FIXTURES)/udf_java.wasm

clean:
	$(MVN) -q -B clean
