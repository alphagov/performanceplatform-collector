#!/bin/bash -e

set -o pipefail

BASEDIR=$(dirname $0)
OUTDIR="$BASEDIR/out"

function display_result {
    RESULT=$1
    EXIT_STATUS=$2
    TEST_NAME=$3

    if [ $RESULT -ne 0 ]; then
      echo
      echo -e "\033[31m$TEST_NAME failed\033[0m"
      echo
      exit $EXIT_STATUS
    else
      echo
      echo -e "\033[32m$TEST_NAME passed\033[0m"
      echo
    fi
}

function clean_output_directory {
    mkdir -p "$OUTDIR"
    rm -f "$OUTDIR/*"
    rm -f ".coverage"
}

function activate_virtualenv_if_exists {
    if [ -d "venv" ]; then
        source venv/bin/activate
    fi
}

function clean_up_python_bytecode {
    find $BASEDIR/performanceplatform -iname '*.pyc' -exec rm {} \+
    find $BASEDIR/performanceplatform -iname '__pycache__' -exec rmdir {} \+
}

function run_style_checks {
    pip install pep8
    $BASEDIR/pep-it.sh | tee "$OUTDIR/pep8.out"
    display_result $? 3 "Code style check"
}

function run_unit_tests {
    python setup.py test
    display_result $? 1 "Unit tests"
}

clean_output_directory
activate_virtualenv_if_exists
clean_up_python_bytecode
run_style_checks
run_unit_tests
