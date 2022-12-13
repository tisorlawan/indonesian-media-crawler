#!/bin/bash

DEBUG_DIR=${CARGO_TARGET_DIR:-./target}/debug/deps
COVERAGE_OUTPUT_DIR="./coverage"

export CARGO_INCREMENTAL=0
export RUSTFLAGS='-Cinstrument-coverage'
export LLVM_PROFILE_FILE='cargo-test-%p-%m.profraw'

cargo test

mkdir $COVERAGE_OUTPUT_DIR 2>/dev/null

grcov . \
	--binary-path "$DEBUG_DIR" \
	-s . \
	-t html \
	--branch --ignore-not-existing \
	--ignore '../*' \
	--ignore "/*" \
	-o $COVERAGE_OUTPUT_DIR/html \
	--llvm-path=/usr/bin
