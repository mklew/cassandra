#!/bin/bash

echo "Reloading cluster"
ant && ccm stop && ccm start
echo "Cluster is ready"
ccm status | cat