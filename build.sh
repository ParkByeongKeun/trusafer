env GOOS=linux GOARCH=amd64 go build -o appserver_linux_amd64 *.go
env GOOS=darwin GOARCH=arm64 go build -o appserver_darwin_arm64 *.go