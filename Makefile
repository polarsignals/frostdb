test:
	go test -tags debug -race ./...

.PHONY: gen/proto
gen/proto:
	buf generate
