#!/bin/bash

# Default to master if no branch is specified
BRANCH_NAME=${1:-master}

# Function to clone and build a repository
clone_and_build() {
  local repo_url="https://github.com/broadinstitute/dig-aggregator-methods"
  local dir_name="dig-aggregator-methods"
  local branch=$1

  echo "Cloning $repo_url on branch $branch..."
  git clone -b $branch $repo_url $dir_name
  if [ $? -ne 0 ]; then
    echo "Failed to clone repository."
    exit 1
  fi

  cd $dir_name
  sbt compile
  cd intake
  sbt "run -c ../config.json --no-insert-runs --reprocess"
}
clone_and_build "$BRANCH_NAME"
