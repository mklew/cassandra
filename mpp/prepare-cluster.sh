#!/bin/bash

sudo ifconfig lo0 alias 127.0.0.2
sudo ifconfig lo0 alias 127.0.0.3

ccm create mpp-cluster --install-dir=/Users/mareklewandowski/Magisterka/mmp/cassandra-trunk

ccm updateconf

ccm populate -n 3

# Cluster of 5 nodes

sudo ifconfig lo0 alias 127.0.0.4
sudo ifconfig lo0 alias 127.0.0.5

ccm create mpp-cluster-five --install-dir=/Users/mareklewandowski/Magisterka/mmp/cassandra-trunk

ccm populate -n 5