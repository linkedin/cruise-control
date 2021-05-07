ifdef OS
export USER_ID=0
export GROUP_ID=0
else
export USER_ID=$(shell id -u)
export GROUP_ID=$(shell id -g)
endif


build_script:
	./scripts/what-changed.sh

	@docker-compose run --rm k14s ytt \
	-f templates/additional-steps.yaml \
	-f templates/funcs.star \
	-f templates/data.yaml \
	--data-value-file changed_folders=./changed_folders.json \
	-v branch=$(BUILDKITE_BRANCH)  \
	--output-files .out

