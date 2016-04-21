#!/bin/bash

ccm remove

ccm create mpp-cluster-five --install-dir=/Users/mareklewandowski/Magisterka/mmp/cassandra-trunk

ccm populate -n 5

ccm start

echo "Cluster has started"
ccm status | cat

echo "Loading test schema"
ccm node1 cqlsh -e "SOURCE '/Users/mareklewandowski/Magisterka/mmp/cassandra-trunk/mpp/test-schema'"

echo "Cluster is ready for tests!"