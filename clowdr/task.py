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
import time
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
    remotetaskdir = op.dirname(metadata)
    metadata = utils.get(metadata, localtaskdir)[0]
    task_id = metadata.split('.')[0].split('-')[-1]
    # The above grabs an ID from the form: fname-#.ext

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

    print("Fetching input data...")
    # Get input data
    localdatadir = "/clowdata"
    for dataloc in input_data:
        utils.get(dataloc, localdatadir)

    # Move to correct location
    os.chdir(localdatadir)

    print("Beginning execution...")
    # Launch task
    start_time = time.time()
    stdout, stderr, ecode, _ = bosh.execute('launch',  desc_local, invo_local)
    duration = time.time() - start_time

    # Get list of bosh exec outputs
    with open(desc_local) as fhandle:
        outputs_all = json.load(fhandle)["output-files"]

    outputs_present = []
    outputs_all = bosh.evaluate(desc_local, invo_local, 'output-files/')
    for outfile in outputs_all.values():
        outputs_present += [outfile] if op.exists(outfile) else []

    # Write stdout to file
    stdoutf = "stdout-{}.txt".format(task_id)
    with open(op.join(localtaskdir, stdoutf), "w") as fhandle:
        fhandle.write(stdout.decode("utf-8"))
    utils.post(op.join(localtaskdir, stdoutf), op.join(remotetaskdir, stdoutf))

    # Write sterr to file
    stderrf = "stderr-{}.txt".format(task_id)
    with open(op.join(localtaskdir, stderrf), "w") as fhandle:
        fhandle.write(stderr.decode("utf-8"))
    utils.post(op.join(localtaskdir, stderrf), op.join(remotetaskdir, stderrf))

    # Write summary values to file, including:
    summary = {"duration": duration,
               "exitcode": ecode,
               "outputs": [],
               "stdout": op.join(remotetaskdir, stdoutf),
               "stderr": op.join(remotetaskdir, stderrf)}

    print("Uploading outputs...")
    # Push outputs
    for local_output in outputs_present:
        print("{} --> {}".format(local_output, output_loc))
        summary["outputs"] += utils.post(local_output, output_loc)

    summarf = "summary-{}.json".format(task_id)
    with open(op.join(localtaskdir, summarf), "w") as fhandle:
        fhandle.write(json.dumps(summary) + "\n")
    utils.post(op.join(localtaskdir, summarf), op.join(remotetaskdir, summarf))


def main(args=None):
    parser = ArgumentParser(description="Entrypoint for Clowdr-task")
    parser.add_argument("metadata", action="store", help="S3 URL to metadata")
    results = parser.parse_args() if args is None else parser.parse_args(args)

    process_task(results.metadata)


if __name__ == "__main__":
    main()

