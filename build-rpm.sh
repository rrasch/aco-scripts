#!/bin/bash

if [ -z "$1" ]; then
    echo "Error: You must specify a git tag to build."
    echo "Usage: $0 TAG"
    exit 1
fi

TAG="$1"

GIT_URL="https://github.com/rrasch/aco-scripts"

COMMIT=$(git ls-remote $GIT_URL refs/tags/$TAG | cut -f1 | cut -c1-7)

echo "Building aco-scripts:"
echo "  Repo:   $GIT_URL"
echo "  Tag:    $TAG"
echo "  Commit: $COMMIT"

rpmbuild -ba aco-scripts.spec \
  --define "git_tag $TAG" \
  --define "git_commit $COMMIT"
