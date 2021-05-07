export DEFAULT_DOCKER_REPO=artifactory.jfrog.iress.online/iress-docker
export BOOTSTRAP_SERVERS=localhost:9092
export IMAGE_TAG:dev


.PHONY: authenticate
authenticate: 
	docker-compose run --user 0 --rm \
	-e OKTA_USERNAME=${OKTA_USERNAME} \
	-e AWS_SESSION_DURATION=${AWS_SESSION_DURATION} \
	authenticate --to ${AWS_ACCOUNT_NAME} --as devops /app/authenticate.sh
	aws sts get-caller-identity

.PHONY: connected
connected: authenticate kube copy_kube

.PHONY: copy_kube
copy_kube:
	${RUN_UTIL} cp /app/.kube/config /home/.kube/config

.PHONY: spring_clean
spring_clean: #deployment__clean 
	${RUN_UTIL} rm -rf .aws .kube .rules .working
	docker-compose down --remove-orphans

.PHONY: terraform_shell
terraform_shell:
	docker-compose run --entrypoint bash terraform

