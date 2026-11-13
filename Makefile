GO ?= go
GOFMT ?= gofmt
GOLANGCI_LINT ?= golangci-lint
BINDIR ?= bin
COVERPROFILE ?= coverage.out
FUZZTIME ?= 10s
FUZZWORKERS ?= 2
BENCH_ARGS ?= -workload all

.PHONY: build test race cover lint bench fuzz

build:
	mkdir -p "$(BINDIR)"
	$(GO) build -buildvcs=false -o "$(BINDIR)/" ./cmd/...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

cover:
	$(GO) test -covermode=atomic -coverprofile="$(COVERPROFILE)" ./...
	$(GO) tool cover -func="$(COVERPROFILE)"

lint:
	@set -e; files=$$($(GOFMT) -l cmd ember internal); \
		if [ -n "$$files" ]; then printf '%s\n' "$$files"; exit 1; fi
	GOFLAGS="$${GOFLAGS:+$$GOFLAGS }-buildvcs=false" $(GOLANGCI_LINT) run ./...

bench:
	$(GO) run -buildvcs=false ./cmd/emberbench $(BENCH_ARGS)

fuzz:
	$(GO) test ./internal/resp -run='^$$' -fuzz='^FuzzReadCommand$$' -fuzztime="$(FUZZTIME)" -parallel="$(FUZZWORKERS)"
