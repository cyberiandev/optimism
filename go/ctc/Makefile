SHELL := /bin/bash

GITCOMMIT := $(shell git rev-parse HEAD)
GITDATE := $(shell git show -s --format='%ct')
GITVERSION := $(shell cat package.json | jq .version)

LDFLAGSSTRING +=-X main.GitCommit=$(GITCOMMIT)
LDFLAGSSTRING +=-X main.GitDate=$(GITDATE)
LDFLAGSSTRING +=-X main.GitVersion=$(GITVERSION)
LDFLAGS :=-ldflags "$(LDFLAGSSTRING)"

ctc:
	env GO111MODULE=on go build $(LDFLAGS)

clean:
	rm ctc

test:
	go test -v ./...

lint:
	golangci-lint run ./...

binding:
	$(eval temp := $(shell mktemp))

	cat abis/OVM_CanonicalTransactionChain.json \
		| jq -r .bin > $(temp)

	cat abis/OVM_CanonicalTransactionChain.json \
		| jq .abi \
		| abigen --pkg bindings \
		--abi - \
		--out bindings/canonicaltransactionchain.go \
		--type CanonicalTransactionChain \
		--bin $(temp)

	rm $(temp)
