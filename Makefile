# Makefile - use `make` or `make help` to get a list of commands.
#
# Note - Comments inside this makefile should be made using a single
# hashtag `#`, lines with double hash-tags will be the messages that
# printed in the help command

# Name of the current directory
PROJECTNAME="orangegate"

# List of all Go-files to be processed
GOFILES=$(wildcard *.go)

# Docker image variables
IMAGE := $(PROJECTNAME)
VERSION := latest

# Ensures firing a blank `make` command default to help
.DEFAULT_GOAL := help

# Make is verbose in Linux. Make it silent
MAKEFLAGS += --silent


.PHONY: help
## `help`: Generates this help dialog for the Makefile
help: Makefile
	echo
	echo " Commands available in \`"$(PROJECTNAME)"\`:"
	echo
	sed -n 's/^[ \t]*##//p' $< | column -t -s ':' |  sed -e 's/^//'
	echo

.PHONY: repl
## `repl`: Play with orangegatedb through OQL
repl:
	go run repl/main.go

.PHONY: local-setup
## `local-setup`: Setup development environment locally
local-setup:
	echo "  >  Ensuring directory is a git repository"
	git init &> /dev/null
	echo "  >  Installing pre-commit"
	pip install --upgrade pre-commit &> /dev/null
	pre-commit install


# Will install missing dependencies
.PHONY: install
## `install`: Fetch dependencies needed to run `orangegate`
install:
	echo "  >  Getting dependencies..."
	go get -v $(get)
	go mod tidy

.PHONY: run
## :
## `run`: Run `orangegate` in production mode
run: export production_mode=production
run: export __BUILD_MODE__=production
run:
	go run main.go $(q)

.PHONY: run-debug
## `run-debug`: Run `orangegate` in debug mode
run-debug: export debug_mode=debug
run-debug: export __BUILD_MODE__=debug
run-debug:
	go run main.go $(q)


.PHONY: docker-gen
## :
## `docker-gen`: Create a production docker image for `orangegate`
docker-gen:
	echo "Building docker image \`$(IMAGE):$(VERSION)\`..."
	cp docker/orangegate/.dockerignore .dockerignore
	docker build --rm \
		--build-arg final_image=scratch \
		--build-arg build_mode=production \
		-t $(IMAGE):$(VERSION) . \
		-f ./docker/orangegate/Dockerfile
	rm .dockerignore


.PHONY: docker-debug
## `docker-debug`: Create debug-friendly docker images for `orangegate`
docker-debug:
	echo "Building docker image \`$(IMAGE):$(VERSION)\`..."
	docker build --rm=false \
		--build-arg final_image=golang:1.21 \
		--build-arg build_mode=debug \
		-t $(IMAGE)-debug:$(VERSION) . \
		-f ./docker/Dockerfile


.PHONY: clean-docker
## `clean-docker`: Delete an existing docker image
clean-docker:
	echo "Removing docker $(IMAGE):$(VERSION)..."
	docker rmi -f $(IMAGE):$(VERSION)

## :
##  NOTE: All docker-related commands can use `IMAGE`
## : and `VERSION` variables to modify the docker
## : image being targeted
## :
## : Example;
## :     make docker-gen IMAGE=new_project VERSION=3.15
## :
## : Likewise, both the `run` commands can pass runtime
## : arguments under the `q` arg
## :
## : Example;
## :	`make run q="time --version"`
