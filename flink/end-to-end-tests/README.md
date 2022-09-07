# Flink Delta Connector end-to-end tests

## Implementation plan
In order to make code review easier, the implementation of end-to-end tests should be divided into multiple pull requests. We plan to do it in the following steps (some of them can be done in parallel):

PR-01: Project skeleton.
- Shows how the project structure looks like.
- Add sbt modules (tests + fatjar).
- Add a smoke test (does not schedule any flink job actually).
- Add terraform skeleton (almost empty).
- Add run-end-to-end-tests.sh script.

PR-02-A: Terraform.
- Add terraform creating all necessary infrastructure in AWS.

PR-02-B: Flink REST Client.
- Java implementation of a client for job scheduling.

PR-03-A: Add bounded reader test scenarios.
- Add corresponding Flink job.
- Implement job scenarios. Create multiple pull requests if there are lots of test scenarios.

PR-03-B: Add bounded writer test scenarios.
- Add corresponding Flink job.
- Implement job scenarios. Create multiple pull requests if there are lots of test scenarios.

PR-04-A: Add unbounded reader test scenarios.
- Add corresponding Flink job.
- Implement job scenarios. Create multiple pull requests if there are lots of test scenarios.

PR-04-B: Add unbounded writer test scenarios.
- Add corresponding Flink job.
- Implement job scenarios. Create multiple pull requests if there are lots of test scenarios.

PR-05: Enable to run tests in parallel.

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

## Test scenarios
### Sink
1. Batch job successfully writes records to a \[partitioned / non-partitioned] Delta Lake table.
2. Streaming job successfully writes records to a \[partitioned / non-partitioned] Delta Lake table.
3. Batch job successfully writes records to a \[partitioned / non-partitioned] Delta Lake even if the job has recovered after a failure.
4. Streaming job successfully writes records to a \[partitioned / non-partitioned] Delta Lake even if the job has recovered after a failure from a checkpoint.
5. Streaming job creates Delta Lake checkpoint successfully.


### Source
1. Batch job correctly reads the latest Delta Lake snapshot.
2. Batch job correctly reads specified Delta Lake snapshot version (startingVersion()).
3. Batch job reads correctly the latest Delta Lake snapshot created before a given timestamp (startingTimestamp()).
4. Batch job correctly reads partitioned Delta Lake table.
5. Batch job correctly reads only selected column names (columnNames()).
6. Batch job correctly reads Delta Lake snapshot even if the job recovered from a failure.
7. Streaming job reads head snapshot and subsequent new changes to the Delta Lake table.
8. Streaming job reads given snapshot version (startingVersion()) and subsequent new changes to the Delta Lake table.
9. Streaming job reads the earliest Delta Lake snapshot created after a given timestamp (startingTimestamp()) and subsequent new changes to the Delta Lake table.
10. Streaming job correctly reads a partitioned Delta Lake table.
11. Streaming job correctly reads only selected column names (columnNames()).
12. Streaming job fails when ignoreDeletes(false) and there were deletes in the Delta Lake table.
13. Streaming job fails when ignoreChanges(false) and there were deletes or updates in the Delta Lake table.
14. Streaming job correctly reads Delta Lake table when ignoreDeletes(true) and there were deletes in the Delta Lake table.
15. Streaming job fails when ignoreChanges(true) and there were deletes or updates in the Delta Lake table.
16. Streaming job correctly reads Delta Lake snapshot and subsequent new changes even if the job recovered from a failure.
17. \[Streaming/Batch] job fails when reading the Delta Lake table if startingVersion was previously removed using vacuum.

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
       --scala-version 2.12.8 \
       --preserve-s3-data
   ```
   You can still delete them by removing S3 bucket contents manually and then running:
   ```bash
   cd flink/end-to-end-tests/terraform/
   terraform destroy -auto-approve
   ```
