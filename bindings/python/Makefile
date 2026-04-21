RUST_PROJECT_ROOT ?= $(realpath ../..)

.PHONY: build test clean

build:
	cd $(RUST_PROJECT_ROOT) && cargo build --features ffi --release
	pip install -e .

test: build
	cd $(RUST_PROJECT_ROOT)/bindings/python && python -m pytest test/unit -v

clean:
	rm -rf build *.egg-info __pycache__ *.so
