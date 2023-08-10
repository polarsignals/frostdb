test:
	go test -tags assert,debug -race ./...

.PHONY: gen/proto
gen/proto:
	buf generate
