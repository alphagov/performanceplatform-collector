#!/bin/bash

function get_sha(){
    REF=$(git log -n 1 -- performanceplatform/collector/__init__.py --name-only)
    SHA=$(echo $REF | awk '{ print $2 }')

    git checkout $SHA
}

function get_version(){
    VERSION=$(python setup.py --version)
}

function publish_or_die(){
    TAG_EXISTS=$(git tag | grep -G "^${VERSION}$")
    if [ "$TAG_EXISTS" ]; then
        exit 0
    else
        publish $VERSION
    fi
}

function publish(){
    VERSION=$1
    git tag -a $VERSION -m "Automatically published from jenkins"
    python setup.py sdist bdist_wheel upload
}

function main(){
    get_sha
    get_version
    publish_or_die
}

main
