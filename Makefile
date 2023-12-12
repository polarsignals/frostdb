test:
	go test -tags assert,debug -race ./...

.PHONY: gen/proto
gen/proto:
	buf generate

lint:
	golangci-lint --timeout=5m run --fix