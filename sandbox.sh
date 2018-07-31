#!/bin/bash

clowdr local \
    examples/descriptor_d.json \
    examples/invocation.json \
    examples/task/ \
    -bV \
    -g 4 \
    -v /data/ds114:/data/ds114
