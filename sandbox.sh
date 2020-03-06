#!/bin/bash

clowdr local \
    examples/bids-example/descriptor_d.json \
    examples/bids-example/invocation.json \
    examples/bids-example/task/ \
    -bV \
    -v /data/:/data/ \
    -g 4

# clowdr cloud \
#     examples/descriptor_d.json \
#     examples/invocation.json \
#     s3://misiclab/demo-clowdr/ \
#     s3://misiclab/demo/ \
#     aws \
#     ~/Dropbox/keys/misiclab_aws.csv \
#     -bV
