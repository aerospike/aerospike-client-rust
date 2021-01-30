# ======================================================================
# 
#  Standardized commands 
# 
# ======================================================================
.DEFAULT_GOAL := help

aerospike: $(DOCKER)  ## Start aerospike docker instance
	mkdir -p $(AEROSPIKE_DATA_PATH)
	$(DOCKER) run --rm -it $(AEROSPIKE_DOCKER_RUN) --name aerospike $(AEROSPIKE_URL)

audit: $(CARGO) $(AUDIT)  ## Check for security vulnerabilities
	$(CARGO) audit

benchmark: $(CARGO)  ## Benchmarks code
	$(CARGO) bench

build: $(CARGO)  ## Compiles crate workspace
	$(CARGO) build --release --workspace

doc: $(CARGO)  ## Build documentation
	$(CARGO) doc --no-deps

help: ## Show this menu 
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

lint: $(CARGO) clippy  ## Run style check
	$(CARGO) clippy --all --all-features -- -D warnings

release: $(CARGO) | style lint doc build test benchmark audit  ## Run everything needed for a release

style: | $(CARGO) rustfmt ## Run style check
	$(CARGO) fmt --all -- --check

test: $(CARGO)  ## Run tests
	$(CARGO) test --release

# ======================================================================
# 
#   Support
# 
# ======================================================================
# docker
DOCKER := $(shell which docker || echo /usr/local/bin/docker)

$(DOCKER):
	@echo "`docker` must be installed and available from PATH"
	@env | grep -i ^PATH
	@exit 1

# aerospike
AEROSPIKE_DATA_PATH ?= $(HOME)/.aerospike/data
AEROSPIKE_DEFAULT_TTL ?= 0
AEROSPIKE_DOCKER_REPO ?= aerospike/aerospike-server
AEROSPIKE_MEMORY ?= 1
AEROSPIKE_NAMESPACE ?= test
AEROSPIKE_STORAGE ?= 8
AEROSPIKE_URL ?= $(AEROSPIKE_DOCKER_REPO):$(AEROSPIKE_VERSION)
AEROSPIKE_VERSION ?= 5.4.0.2

AEROSPIKE_DOCKER_RUN := -p 3000:3000 -p 3001:3001 -p 3002:3002
ifneq "" "$(AEROSPIKE_DEFAULT_TTL)"
AEROSPIKE_DOCKER_RUN += -e "DEFAULT_TTL=$(AEROSPIKE_DEFAULT_TTL)"
endif
ifneq "" "$(AEROSPIKE_NAMESPACE)"
AEROSPIKE_DOCKER_RUN += -e "NAMESPACE=$(AEROSPIKE_NAMESPACE)"
endif
ifneq "" "$(AEROSPIKE_MEMORY)"
AEROSPIKE_DOCKER_RUN += -e "MEM_GB=$(AEROSPIKE_MEMORY)"
endif
ifneq "" "$(AEROSPIKE_STORAGE)"
AEROSPIKE_DOCKER_RUN += -e "STORAGE_GB=$(AEROSPIKE_STORAGE)"
endif

# rust
CARGO_HOME ?= $(HOME)/.cargo
CARGO := ${CARGO_HOME}/bin/cargo

AUDIT := $(shell (which cargo-audit 2>/dev/null || echo ${CARGO_HOME}/bin/cargo-audit))
CLIPPY := $(shell (which cargo-clippy 2>/dev/null || echo ${CARGO_HOME}/bin/cargo-clippy))
RUSTFMT := $(shell (which rustfmt 2>/dev/null || echo ${CARGO_HOME}/bin/rustfmt))
RUSTUP := $(shell (which rustup 2>/dev/null || echo ${CARGO_HOME}/bin/rustup))

# cargo audit
export AUDIT
$(AUDIT):
	$(CARGO) install cargo-audit

# cargo command
export CARGO
$(CARGO): $(RUSTUP)

# cargo clippy - as a component, the file may be there but the toolchain installation may not
clippy: $(RUSTUP)
	$(eval CLIPPY_INSTALLED := $(shell (rustup component list | grep -i \(installed\) | sort | grep -i clippy)))
ifeq "" "$(CLIPPY_INSTALLED)"
	$(RUSTUP) component add clippy
endif

# cargo fmt - as a component, the file may be there but the toolchain installation may not
rustfmt: $(RUSTFMT)
	$(eval RUSTFMT_INSTALLED := $(shell (rustup component list | grep -i \(installed\) | sort | grep -i rustfmt)))
ifeq "" "$(RUSTFMT_INSTALLED)"
	$(RUSTUP) component add rustfmt
endif
