#!/bin/bash


# PEG_ROOT=$(dirname ${BASH_SOURCE})/../..

PEG_ROOT=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/../..

CLUSTER_NAME=kafka-cluster

peg up ${PEG_ROOT}/examples/kafka/master.yml &
peg up ${PEG_ROOT}/examples/kafka/workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} zookeeper
peg install ${CLUSTER_NAME} kafka
peg install ${CLUSTER_NAME} kafka-manager
