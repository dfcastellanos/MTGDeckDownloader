install:
	pip install -r requirements.txt
	
test:
	pytest --verbose ./test

format:
	black */*.py

lint:
	pylint --disable=R,C */*.py

.PHONY: test

#_____________________________________________________________
# commands for building and deploying as AWS SAM applications

# USAGE: if it is the first deployment, lambda_deploy_guided must be called for
# both lambdas, i.e. as make lambda_deploy_guided name=deck_producer and then 
# make lambda_deploy_guided name=deck_consumer (accepting all the default options)
# After this call, the samconfig.toml files are updated with AWS-specific 
# details (specifically, the ECR arn for each Lambda function and the S3 bucket 
# with the sam data). After the first initial guided deployment has been performed,
# the targets make lambda_deploy name=... and lambda_deploy_all can be called.
# (alternatively, make lambda_deploy_all can be used for deploying both at the 
# same time).

lambda_build:
	cd lambdas/$(name)/ && \
	sam build --template template.yaml

lambda_deploy_guided:
	cd lambdas/$(name)/ && \
	sam build --template template.yaml && \
	sam deploy --guided --config-file samconfig.toml 

lambda_deploy: 
	cd lambdas/$(name)/ && \
	sam build --template template.yaml && \
	sam deploy --config-file samconfig.toml 
	
lambda_deploy_all:
	cd lambdas/deck_producer/ && \
	sam build --template template.yaml && \
	sam deploy --config-file samconfig.toml && \
	cd - && \
	cd lambdas/deck_consumer/ && \
	sam build --template template.yaml && \
	sam deploy --config-file samconfig.toml 
