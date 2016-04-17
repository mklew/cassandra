#!/bin/bash

echo "Greping Node 1"
cat /Users/mareklewandowski/.ccm/mpp-cluster-five/node1/logs/system.log | grep $1
echo "Greping Node 2"
cat /Users/mareklewandowski/.ccm/mpp-cluster-five/node2/logs/system.log | grep $1
echo "Greping Node 3"
cat /Users/mareklewandowski/.ccm/mpp-cluster-five/node3/logs/system.log | grep $1
echo "Greping Node 4"
cat /Users/mareklewandowski/.ccm/mpp-cluster-five/node4/logs/system.log | grep $1
echo "Greping Node 5"
cat /Users/mareklewandowski/.ccm/mpp-cluster-five/node5/logs/system.log | grep $1