#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/task.py
# Created by Greg Kiar on 2018-02-28.
# Email: gkiar@mcin.ca

from argparse import ArgumentParser
import os.path as op
import json
import os
import re

import boutiques as bosh
from clowdr import utils


def processTask(metadata, clowdrloc=None, **kwargs):
    # Get metadata
    if clowdrloc is None:
        localtaskdir = "/task/"
    else:
        localtaskdir = clowdrloc
    print("Fetching metadata...")
    metadata = utils.get(metadata, localtaskdir)[0]

    # Parse metadata
    metadata   = json.load(open(metadata))
    descriptor = metadata['tool']
    invocation = metadata['invocation']
    input_data = metadata['dataloc']
    output_loc = utils.truepath(metadata['taskloc'])

    print("Fetching descriptor and invocation...")
    # Get descriptor and invocation
    desc_local = utils.get(descriptor, localtaskdir)[0]
    invo_local = utils.get(invocation, localtaskdir)[0]

    task_loc   = op.dirname(invocation)
    invo_id    = invo_local.split('.')[0].split('-')[-1]
    # The above grabs an ID from the form: fname-#.ext

    print("Fetching input data...")
    # Get input data
    local_data_dir = "/clowdata/"
    for dataloc in input_data:
        utils.get(dataloc, local_data_dir)

    # Move to correct location
    os.chdir(local_data_dir)

    print("Beginning execution...")
    # Launch task
    try:
        std = bosh.execute('launch',  desc_local, invo_local)
        # graph_dir = '{}clowprov/'.format(local_data_dir)
        # graph_name = '{}clowdrgraph-{}.rpz'.format(graph_dir, invo_id)

        # cmd = 'reprozip trace -w --dir={} bosh exec launch {} {}'
        # os.system(cmd.format(graph_dir, desc_local, invo_local))

        # cmd = 'reprozip pack --dir={} {}'
        # os.system(cmd.format(graph_dir, graph_name))

        # print("{} --> {}".format(graph_name, op.join(task_loc, op.basename(graph_name))))
        # utils.post(graph_name, op.join(task_loc, op.basename(graph_name)))
    except ImportError:
        print("(Reprozip not installed, no provenance tracing)")
        std = bosh.execute('launch',  desc_local, invo_local)

    # Get list of bosh exec outputs
    with open(desc_local) as fhandle:
        outputs_all = json.load(fhandle)["output-files"]

    outputs_present = []
    outputs_all = bosh.evaluate(desc_local, invo_local, 'output-files/')
    for outfile in outputs_all.values():
        outputs_present += [outfile] if op.exists(outfile) else []

    print("Uploading outputs...")
    # Push outputs
    for local_output in outputs_present:
        print("{} --> {}".format(local_output, output_loc))
        utils.post(local_output, output_loc)


def main(args=None):
    parser = ArgumentParser(description="Entrypoint for Clowdr-task")
    parser.add_argument("metadata", action="store", help="S3 URL to metadata")
    results = parser.parse_args() if args is None else parser.parse_args(args)

    process_task(results.metadata)


if __name__ == "__main__":
    main()

