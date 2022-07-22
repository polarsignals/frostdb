test:
	go test -race ./...

.PHONY: gen/proto
gen/proto:
	buf generate
