#!/bin/bash --login
set -e

ROOT=$(git rev-parse --show-toplevel)
BOLT_THRIFT=$1

if [[ ${BOLT_THRIFT} == '' ]]; then
    echo "Please specify the bolt.thrift file as first argument"
    exit 1
fi

if [[ $(which thrift) != '' ]]; then
    rm -rf ${ROOT}/concord_cli/generated
    mkdir -p ${ROOT}/concord_cli/
    thrift --gen py:json,utf8strings -o ${ROOT}/concord_cli/ $BOLT_THRIFT
    mv ${ROOT}/concord_cli/gen-py ${ROOT}/concord_cli/generated
    echo "DONE!"
else
    echo "Sorry you don't have apache thrift installed"
    exit 1
fi
