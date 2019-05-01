# CS425 MP3 - Distributed transactions

## Group member
Jianfeng Xia(jxia11), Cheng Hu(chenghu3)

## Environment
Go 1.11.5

## Instructions:
* Configurations:
    1. Servers:
      A running at VM01, port 9000
      B running at VM01, port 9001
      C running at VM01, port 9002
      D running at VM01, port 9003
      E running at VM01, port 9004
    2. Coordinator:
      Coordinator running at VM02, port 9000		
* To build: `go build mp3.go`
* To run:
  1. Run server: `./mp3 server name port`
  2. Run client: `./mp3 client name`
  3. Run coordinator: `./mp3 coordinator port`