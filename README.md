[![Python 3.8](https://github.com/kastellane/MTGDeckDownloader/actions/workflows/CI.yml/badge.svg)](https://github.com/kastellane/MTGDeckDownloader/actions/workflows/CI.yml)

<a href="https://codeclimate.com/github/kastellane/MTGDeckDownloader/maintainability"><img src="https://api.codeclimate.com/v1/badges/832a2c0241bb36026c21/maintainability" /></a>

# MTGDeckDownloader

A web scraper that finds, parses, and downloads (in parallel) data from decks played in events of "Magic: The Gathering" (TM Wizards of the Coast) stored in mtgtop8.com. This data includes the date on which the decks were played, the deck type, the event and player names, and the composition of the decks.

The web scraper can be deployed as a serverless application in AWS Lamba, called from the command line, or imported into other Python scripts.

## Deploy as AWS SAM application

NOTE: Although simple, this process assumes some familiarity with serverless applications and AWS Lambda. 

This web scraper can be deployed as a serverless application using AWS Lambda. Specifically, as two Lambda functions, one acting as a job producer and another as a job consumer. The producer sends the jobs to an SQS queue. This queue can be configured to trigger the consumer function when jobs are received from the producer. The consumer function leverages the AWS Lambda concurrency to be massively parallel (but care must be taken not to overload mtgtop8.com, so a limit to the number of concurrent calls should be imposed from the AWS Lambda configuration dashboard). The results from the consumer (the downloaded data corresponding to decks) are then sent to another SQS queue, from where they could be further processed as part of a data pipeline, written to a database, downloaded as files, etc. You can see this in the following image, which ilustrates the structure of the web scraper and also where it fits within a broader project:

![map](https://user-images.githubusercontent.com/5737365/141975072-56ae8f85-a1e3-4d21-98a9-22692fd3744f.jpg)

The configuration files for the producer function are located in `lambdas/deck_producer`, and for the consumer in `lambdas/deck_consumer`. These functions are deployed as Docker containers, with Dockerfiles located in their respective directories. The lambda functions are created according to the `template.yml` files, which among other configurations, define the names of the mentioned SQS queues as environment variables. The user does not need to build any image themselves since this is fully managed by the AWS sam CLI, and no changes in the provided `template.yml` configurations should be needed. Thus, to build and deploy the functions, all that is needed is to call the project's makefile.

For the first deployment, you need to call `make lambda_deploy_guided name=deck_producer` and `make lambda_deploy_guided name=deck_consumer`. You will be prompted to define some configurations along the process, but the default values should work. (If you know what you are doing, you can fine-tune the deployment process at will here). After this call, the `samconfig.toml` files are updated with AWS-specific details (specifically, the ECR arn for each Lambda function and the S3 bucket with the sam data), and subsequent deployments will use them so no further user input will be needed. Thus, after the first deployment, further deployment can be done by calling `make lambda_deploy name=deck_producer` and `make lambda_deploy name=deck_consumer` (also, both lambdas can be deployed in a single call as `make lambda_deploy_all`).

NOTE1: during the deploy process, some errors might occur which are likely due to missing permissions. You will need permissions for S3, CloudFormation, Lambda and ECR. Extra permissions might be necessary, and will be indicated in error messages during the process.

NOTE2: if an error occurs during the process, you might need to delete the stacks associated with this application in CloudFormation before retrying.

## Command-line interface

To use this web scraper, you need to first install the dependencies defined in the file `requirements.txt`. You can do it by calling `make install`. It is recommended that before installing the dependencies you create a Python virtual environment. This can be done as `python3 -m venv .myvirtenv` and activated as `source .myvirtenv/bin/activate`.

The web scraper can be called from the command-line interface doing `python src/download_decks.py` (alternatively, it be made executable as `sudo chmod +x src/download_decks.py` and then called as `./src/download_decks.py`). The commands are:

      -h, --help            show this help message and exit
      -p PAYLOAD, --payload PAYLOAD
                            Payload for the search form. Example: '{"format":
                            "MO", "date_start": "25/09/2021", "date_end":
                            "27/09/2021"}'
      -n N, --n N           Number of parallel processes (warning: a high number may
                            cause the server to blacklist the IP address)

The results are printed to stdout in JSON format.

## Import into other scripts

Once you have installed the dependencies as explained in the previous section, you can import the functions of the module `src/download_decks.py` and have fine control over the web scraper.

## Documentation
MTGDeckDownload source files are fully documented with docstrings.


## License
MTGDeckDownload is open source. You can freely use it, redistribute it, and/or modify it
under the terms of the Creative Commons Attribution 4.0 International Public 
License. The full text of the license can be found in the file LICENSE at the top level of the MTGDeckDownload distribution.
 
Copyright (C) 2021  - David Fernández Castellanos.
