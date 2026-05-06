RUST_PROJECT_ROOT ?= $(realpath ../..)
PROFILE ?= release
CFLAGS_EXTRA ?=

.PHONY: build test clean

build:
	CONFLUENT_KAFKA_LIB_DIR=$(RUST_PROJECT_ROOT)/target/$(PROFILE) \
	CFLAGS="$(CFLAGS_EXTRA)" \
	pip install -e . || pip install --no-dependencies -e .

test: build
	pip install .[dev] || pip install --no-dependencies .[dev]
	cd $(RUST_PROJECT_ROOT)/bindings/python && python -m pytest test/unit -v

clean:
	rm -rf build *.egg-info __pycache__ *.so
