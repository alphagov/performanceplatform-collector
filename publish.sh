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
        pypi_check $VERSION
    fi
}

function pypi_check(){
    python setup.py sdist upload -r pypitest
    publish
}

function publish(){
    VERSION=$1
    git tag -a $VERSION -m "Automatically published from jenkins"
    git push origin --tags
    python setup.py register -r pypi
    python setup.py sdist upload -r pypi
}

function main(){
    get_sha
    get_version
    publish_or_die
}

main
