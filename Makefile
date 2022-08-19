
go.get:
	go get -u ./...

go.tidy:
	go mod tidy -compat=1.19

go.test:
	go test ./...

