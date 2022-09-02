# Flink Delta Connector end-to-end tests

## Design

Connector end-to-end tests are executed on AWS infrastructure in order to identify potential bugs automatically at the
early development stage. The test infrastructure is created with Terraform:

* An S3 bucket containing test Delta tables.
* An AWS EKS service.

The Flink cluster runs
in [Session Mode](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/overview/#session-mode),
that is, there is a standalone cluster which can accept multiple jobs during its lifecycle. The infrastructure is
created once before running the tests, and destroyed when the tests finish.

Tests are triggered with `run-end-to-end-tests.sh` script. The script covers all steps, including:

1. Builds test artifact. `flink/end-to-end-tests-fatjar` module contains test jobs definitions.
2. Creates AWS infrastructure with Terraform.
3. Creates Kubernetes infrastructure.
4. Deploys Flink Kubernetes Operator
5. Runs tests. Test implementations are located in `flink/end-to-end-tests` module.
6. Destroys infrastructure after tests finish.

## Manual run

1. Install [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/aws-get-started). 

2. Install [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl).

3. Add permissions for the IAM user. You can either assign `AdministratorAccess` AWS managed policy (discouraged)
      or assign AWS managed policies in a more granular way:
   * `IAMFullAccess`
   * `AmazonVPCFullAccess`
   * `AmazonS3FullAccess`
   * `AmazonEC2FullAccess`
   * `AmazonEC2ContainerRegistryFullAccess`
   * Allow all `eks:*` actions.
 
4. Ensure that your AWS CLI is configured. You should either have valid credentials in shared credentials file (
   e.g. `~/.aws/credentials`)
   ```
   [default]
   aws_access_key_id = anaccesskey
   aws_secret_access_key = asecretkey
   ```
   or export keys as environment variables:
   ```bash
   export AWS_ACCESS_KEY_ID="anaccesskey"
   export AWS_SECRET_ACCESS_KEY="asecretkey"
   ```

5. Create Terraform variable file `flink/end-to-end-tests/terraform/terraform.tfvars` and fill in variable values.
   For example:
   ```tf
   region                = "us-west-2"
   availability_zone1    = "us-west-2a"
   availability_zone2    = "us-west-2b"
   test_data_bucket_name = "delta-flink-connector-e2e"
   eks_workers           = 1
   tags                  = {
     key1 = "value1"
     key2 = "value2"
   }
   ```
   Please check `variables.tf` to learn more about each parameter.

6. Run `run-end-to-end-tests.sh` from project root directory. Sample run command:
   ```bash 
   ./flink/end-to-end-tests/run-end-to-end-tests.sh \
       --scala-version 2.12.8
   ```
   By default, the script removes all infrastructure components, including test Delta Lake table in S3.
   If you would like to preserve them, you can specify additional flags:
   ```bash
   ./flink/end-to-end-tests/run-end-to-end-tests.sh \
       --s3-bucket-name delta-flink-connector-e2e \
       --scala-version 2.12.8 \
       --preserve-s3-data
   ```
   You can still delete them by removing S3 bucket contents manually and then running:
   ```bash
   cd flink/end-to-end-tests/terraform/
   terraform destroy -auto-approve
   ```
