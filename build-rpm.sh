#!/bin/bash

set -euo pipefail

TAG=${1:-}

if [ -z "$TAG" ]; then
    echo "Error: You must specify a git tag to build."
    echo "Usage: $0 TAG"
    exit 1
fi

REPO_HOST=${REPO_HOST:-}

if [ -z "$REPO_HOST" ]; then
    echo "Error: You must set REPO_HOST."
    exit 1
fi

GIT_NAME="aco-scripts"

GIT_URL="https://github.com/rrasch/$GIT_NAME"

COMMIT=$(git ls-remote $GIT_URL refs/tags/$TAG | cut -f1 | cut -c1-7)

if [ -z "$COMMIT" ]; then
	echo "ERROR: Tag '$TAG' not found in repository '$GIT_URL'" >&2
	exit 1
fi

echo "Building $GIT_NAME:"
echo "  Repo:   $GIT_URL"
echo "  Tag:    $TAG"
echo "  Commit: $COMMIT"

source /etc/os-release

VERSION="$(echo ${VERSION_ID} | grep -Eo '^[0-9]')"

OSVER="${ID}${VERSION}"

REPO_DIR=/content/prod/rstar/repo/publishing/$VERSION

RPM_DIR=$REPO_DIR/RPMS/noarch

rm -vf $RPM_DIR/$GIT_NAME-*rpm

pushd ~/work/$GIT_NAME
git pull
popd

rpmbuild --bb $GIT_NAME.spec \
  --define "git_tag $TAG" \
  --define "git_commit $COMMIT" 2>&1 | tee build-${OSVER}.log

sudo dnf -y remove $GIT_NAME
sudo dnf -y install $RPM_DIR/$GIT_NAME-*rpm

rsync -avz -e ssh $RPM_DIR/$GIT_NAME-*.rpm $REPO_HOST:$RPM_DIR

ssh $REPO_HOST createrepo --update $REPO_DIR
