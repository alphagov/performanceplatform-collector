#!/bin/bash

set -e

basedir=$(dirname $0)
pep8 --ignore=E201,E202,E241,E251 --exclude=tests,config,build,src,venv,*.egg "$basedir"
pep8 --ignore=E201,E202,E241,E251,E501 "$basedir/tests"
