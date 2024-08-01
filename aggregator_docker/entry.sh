#!/bin/bash

BRANCH_NAME=$1
METHOD=$2
CLI_FLAGS=$3

clone_and_build() {
  local repo_url="https://github.com/broadinstitute/dig-aggregator-methods"
  local dir_name="dig-aggregator-methods"
  local branch=$1
  local method=$2
  local cli_flags=$3

  echo "Cloning $repo_url on branch $branch..."
  git clone -b $branch $repo_url $dir_name
  if [ $? -ne 0 ]; then
    echo "Failed to clone repository."
    exit 1
  fi

  pushd "$dir_name" || exit 1
  sbt compile || exit 1

  if [ -d "$method" ]; then
    pushd "$method" || exit 1
    sbt "run -c ../config.json $cli_flags" || exit 1
    popd
  else
    echo "Method directory '$method' does not exist."
    exit 1
  fi
  popd
}
clone_and_build "$BRANCH_NAME" "$METHOD" "$CLI_FLAGS"
