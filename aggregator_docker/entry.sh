#!/bin/bash

MA_GUID=$1
BUCKET=$2
BRANCH_NAME=$3
METHODS=$4  # Expected to be a space-separated list of methods
CLI_FLAGS=$5

# Function to clone and build a repository
# Usage: clone_and_build branch_name
clone_and_build() {
  local branch_name=$1
  local repo_url="https://github.com/broadinstitute/dig-aggregator-methods"
  local dir_name="dig-aggregator-methods"

  echo "Cloning $repo_url on branch $branch_name..."
  git clone -b $branch_name $repo_url $dir_name
  if [ $? -ne 0 ]; then
    echo "Failed to clone repository."
    exit 1
  fi

  pushd "$dir_name" || exit 1
  sbt compile || exit 1
}


run_stage() {
  local method_and_stage=$1
  local cli_flags=$2
  local method="${method_and_stage%%:*}"  # Extract method name before ':'
  local stage="${method_and_stage##*:}"  # Extract stage name after ':', if any

  if [ "$method" != "$stage" ]; then
    ./run.sh "$method" --stage "$stage" "$cli_flags"
  else
    ./run.sh "$method" "$cli_flags"
  fi
}

clone_and_build "$BRANCH_NAME"
IFS=' ' read -ra ADDR <<< "$METHODS"
for method in "${ADDR[@]}"; do
  run_stage "$method" "$CLI_FLAGS"
done

python3 plotMetaAnalysis.py --guid "$MA_GUID" --bucket "$BUCKET"
