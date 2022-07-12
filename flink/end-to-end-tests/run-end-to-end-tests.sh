#!/usr/bin/env bash

#
# Copyright (2021) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

RELATIVE_SCRIPT_PATH=$(dirname -- "${BASH_SOURCE[0]:-$0}")
WORKDIR=$(realpath "$RELATIVE_SCRIPT_PATH")
PROJECT_ROOT_DIR="$WORKDIR/../../"
TERRAFORM_DIR="$WORKDIR/terraform/"

build_artifact() {
  cd "$PROJECT_ROOT_DIR" || exit
  build/sbt "++ $SCALA_VERSION" flinkEndToEndTestsFatJar/assembly
  local return_code=$?
  cd "$WORKDIR" || exit
  return $return_code
}

export_fat_jar_path() {
  local matching_jar
  matching_jar=$(find "$WORKDIR"/../end-to-end-tests-fatjar/target/scala-"$SHORT_SCALA_VERSION"/ -iname 'flink-end-to-end-tests-fatjar-assembly-*.jar' -type f)

  if [ -z "$matching_jar" ]; then
    echo "Cannot find artifact containing test jobs."
    exit 1
  else
    echo "Artifact found at path ${matching_jar[0]}."
    export JAR_PATH="${matching_jar[0]}"
  fi
}

create_terraform_infrastructure() {
  echo "Creating terraform infrastructure."
  terraform -chdir="$TERRAFORM_DIR" init &&
    terraform -chdir="$TERRAFORM_DIR" validate &&
    terraform -chdir="$TERRAFORM_DIR" apply -auto-approve
  local return_code=$?
  return $return_code
}

destroy_terraform_infrastructure() {
  local targets
  targets="-target=module.networking -target=module.flink-session-cluster"
  if [ "$PRESERVE_S3_DATA" != "yes" ]; then
    targets="$targets -target=module.storage"
  fi
  if [ "$PRESERVE_CLOUDWATCH_LOGS" != "yes" ]; then
    targets="$targets -target=module.cloudwatch"
  fi
  echo "Destroying terraform infrastructure."
  terraform -chdir="$TERRAFORM_DIR" destroy -auto-approve $targets
}

export_jobmanager_address() {
  local jobmanager_hostname
  local jobmanager_port
  jobmanager_hostname=$(terraform -chdir="$TERRAFORM_DIR" output jobmanager_hostname | tr -d '"')
  jobmanager_port=$(terraform -chdir="$TERRAFORM_DIR" output jobmanager_port)
  export JOBMANAGER_HOSTNAME=$jobmanager_hostname
  export JOBMANAGER_PORT=$jobmanager_port
}

run_end_to_end_tests() {
  echo "Running tests..."
  cd "$PROJECT_ROOT_DIR" || exit

  echo "JAR_PATH=$JAR_PATH"
  echo "S3_BUCKET_NAME=$S3_BUCKET_NAME"
  echo "PRESERVE_S3_DATA=$PRESERVE_S3_DATA"
  echo "AWS_REGION=$AWS_REGION"
  echo "JOBMANAGER_HOSTNAME=$JOBMANAGER_HOSTNAME"
  echo "JOBMANAGER_PORT=$JOBMANAGER_PORT"

  build/sbt "++ $SCALA_VERSION" \
    -DE2E_JAR_PATH="$JAR_PATH" \
    -DE2E_S3_BUCKET_NAME="$S3_BUCKET_NAME" \
    -DE2E_PRESERVE_S3_DATA="$PRESERVE_S3_DATA" \
    -DE2E_AWS_REGION="$AWS_REGION" \
    -DE2E_JOBMANAGER_HOSTNAME="$JOBMANAGER_HOSTNAME" \
    -DE2E_JOBMANAGER_PORT="$JOBMANAGER_PORT" \
    flinkEndToEndTests/test
  local return_code=$?
  cd "$WORKDIR" || exit
  return $return_code
}

main() {
  while [[ $# -gt 0 ]]; do
    case $1 in
    --s3-bucket-name)
      S3_BUCKET_NAME="$2"
      shift # past argument
      shift # past value
      ;;
    --aws-region)
      AWS_REGION="$2"
      shift # past argument
      shift # past value
      ;;
    --preserve-s3-data)
      PRESERVE_S3_DATA="yes"
      shift # past argument
      ;;
    --preserve-cloudwatch-logs)
      PRESERVE_CLOUDWATCH_LOGS="yes"
      shift # past argument
      ;;
    --scala-version)
      SCALA_VERSION="$2"
      # Take only the first four characters (e.g. 2.12).
      SHORT_SCALA_VERSION="${SCALA_VERSION:0:4}"
      shift # past argument
      shift # past value
      ;;
    -* | --*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      shift # past argument
      ;;
    esac
  done

  if ! build_artifact; then
    echo "[ERROR] Failed to build artifact."
    exit 1
  fi

  if ! export_fat_jar_path; then
    echo "[ERROR] Failed to find the test artifact path."
    exit 1
  fi

  if ! create_terraform_infrastructure; then
    echo "[ERROR] Failed to create test infrastructure."
    exit 1
  fi

  if ! export_jobmanager_address; then
    echo "[ERROR] Failed to extract Flink JobManager address."
    exit 1
  fi

  if ! run_end_to_end_tests; then
    echo "[ERROR] Failed to run tests."
    exit 1
  fi
}

cleanup() {
  echo "Clean up..."
  cd "$WORKDIR" || exit 1
  destroy_terraform_infrastructure
}

trap cleanup EXIT
main "$@"
