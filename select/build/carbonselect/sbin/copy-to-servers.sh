#!/usr/bin/env bash

./remove-servers.sh
scp -p -r /data1/carbonselect root@node1:/data1/carbonselect
scp -p -r /data1/carbonselect root@node2:/data1/carbonselect
scp -p -r /data1/carbonselect root@node3:/data1/carbonselect
