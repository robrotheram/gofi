PROJECTNAME=$(shell basename "$(PWD)")

# Go related variables.
# Redirect error output to a file, so we can show it in development mode.
STDERR=/tmp/.$(PROJECTNAME)-stderr.txt

# PID file will keep the process id of the server
PID=/tmp/.$(PROJECTNAME).pid

protoc-install:
	./scripts/protoc-ubuntu-install.sh
	rm -rf protoc3*

proto-build-go:go buu
	go get -u github.com/golang/protobuf/protoc-gen-go
	protoc -I messages messages/*.proto --go_out=plugins=grpc:messages

proto-build-python:
	pip install grpcio-tools
	python -m grpc_tools.protoc --proto_path=messages --python_out=messages --grpc_python_out=messages messages/*.proto

update:
	dep ensure

clean:
	rm -rf package
build:
	mkdir -p package
	go build
	mv gofi package/gofi
	cp -r public package/.
	cp conf.yml package/conf.yml

build-docker:
	sudo docker build -t gofi .

start-docker:
	sudo docker run --rm -it -p 8008:8000 gofi

service-create:
	sudo docker stack deploy -c docker-compose.yml gofi

service-destroy:
	sudo docker stack rm gofi
