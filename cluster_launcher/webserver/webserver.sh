#!/bin/bash


# PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

CLUSTER_NAME=webserver-cluster

peg up ${PEG_ROOT}/examples/webserver/master.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
