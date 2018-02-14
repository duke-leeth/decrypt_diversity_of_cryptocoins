#!/bin/bash


# PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

PEG_ROOT=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../..

CLUSTER_NAME=cassandra-cluster

peg up ${PEG_ROOT}/examples/cassandra/master.yml &
peg up ${PEG_ROOT}/examples/cassandra/workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} cassandra
