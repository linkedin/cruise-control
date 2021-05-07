# Use some sensible default shell settings
SHELL := /bin/bash
.ONESHELL:
.SILENT:

RED=\033[1;31m
CYAN=\033[0;36m
NC=\033[0m

ifdef BUILDKITE
$(info Environment passed in from Buildkite)
else
$(info Including local.mk file)
include local.mk
endif

# assumption here is that OS is only defined in windows, so set uid and gid to 0 (root)
ifdef OS
export USER_ID=0
export GROUP_ID=0
else
export USER_ID=$(shell id -u)
export GROUP_ID=$(shell id -g)
endif

RUN_UTIL=docker-compose run --rm alpine
RUN_AWS=docker-compose run --rm aws
RUN_GRADLE=docker-compose run --rm --entrypoint gradle gradle

export IMAGE_NAME=iress-cruise-control:0.${IMAGE_TAG}

.PHONY: kube build test
kube .kube:
	${RUN_AWS} eks update-kubeconfig --name ${EKS_CLUSTER_NAME}

test:
	${RUN_GRADLE} :test --info --rerun-tasks

build:
	${RUN_GRADLE} build

gradle_shell:
	docker-compose run --rm --entrypoint bash gradle

image:
	echo "building image"
	docker-compose build cruise-control

available:
	echo "Publishing ${IMAGE_NAME} to ${DEFAULT_DOCKER_REPO}"
	echo "${ARTIFACTORY_ACCESS_TOKEN}" | docker login -u "${ARTIFACTORY_USERNAME}" --password-stdin "${DEFAULT_DOCKER_REPO}"
	docker tag ${IMAGE_NAME} ${DEFAULT_DOCKER_REPO}/${IMAGE_NAME}
	docker push ${DEFAULT_DOCKER_REPO}/${IMAGE_NAME}
	docker logout ${DEFAULT_DOCKER_REPO}
	echo "Cleaning up"
	docker rmi $(IMAGE_NAME) -f || true
	docker rmi $(DEFAULT_DOCKER_REPO)/$(IMAGE_NAME) -f || true


local:
	docker-compose run cruise-control