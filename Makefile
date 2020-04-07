build: 
	go build -ldflags "-X github.com/cloudkarafka/cloudkarafka-manager/config.GitCommit=master \
  -X github.com/cloudkarafka/cloudkarafka-manager/config.Version=dev" -tags static -a

test: 
	go test -v ./...

run:
	go run app.go --authentication dev --dev

