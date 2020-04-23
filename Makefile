all: clean fmt test build


install:
	@echo "Installing kiwi..."
	@go install ./cmd/kiwi/

clean:
	@echo "Cleaning up previous builds..."
	@rm -rf ./bin
	@echo "Tidy up Go modules..."
	@go mod tidy -v

fmt:
	@echo "Formatting..."
	@goimports -l -w ./

test:
	@echo "Running tests..."
	@go test -cover ./...

build:
	@echo "Building kiwi..."
	@mkdir -p ./bin
	@go build -o ./bin/kiwi ./cmd/kiwi/

disktests:
	@echo "Running on-disk tests..."
	@go test -count=1 -v -cover -tags=ondisk ./tests/...

.PHONY:	test
