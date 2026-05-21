# TritouBroker

## How to run the project

Run the command for protobuf code generation :

```sh
protoc --go_out=. --go-grpc_out=. src/proto/topic.proto
```

Result :

/topic/topic.pb.go → Go structs (protobuf messages)
/topic/topic_grpc.pb.go → gRPC interfaces

### Run the server

```sh
go run . -mode=server
```

### Run the client

```sh
go run . -mode=client
```

### Run in development

Install air :

```sh
go install github.com/air-verse/air@latest
```

or with homebrew :

```sh
brew install go-air
```

And now you can run the server with :

```sh
air
```

## Understand the code
