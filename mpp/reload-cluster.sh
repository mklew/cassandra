#!/bin/bash

echo "Reloading cluster"
ant && ccm stop && ccm start
echo "Cluster is ready"
ccm status | cat

#echo "Shutting node2 down"
#ccm node2 stop

#echo "Cluster is ready for tests"
