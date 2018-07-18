#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/task.py
# Created by Greg Kiar on 2018-02-28.
# Email: gkiar@mcin.ca

from argparse import ArgumentParser
from memory_profiler import memory_usage
import os.path as op
import subprocess
import cProfile
import pstats
import time
import json
import csv
import os
import re

import boutiques as bosh
from clowdr import utils


class TaskHandler:
    def __init__(self, taskfile, **kwargs):
        self.manageTask(taskfile, **kwargs)

    def manageTask(self, taskfile, provdir=None, verbose=False, **kwargs):
        # Get metadata
        if provdir is None:
            self.localtaskdir = "/clowtask/"
        else:
            self.localtaskdir = provdir

        # The below grabs an ID from the form: /some/path/to/fname-#.ext
        self.task_id = taskfile.split('.')[0].split('-')[-1]

        self.localtaskdir = op.join(self.localtaskdir, "clowtask_"+self.task_id)
        if not op.exists(self.localtaskdir):
            os.makedirs(self.localtaskdir)

        if(verbose):
            print("Fetching metadata...")
        remotetaskdir = op.dirname(taskfile)
        taskfile = utils.get(taskfile, self.localtaskdir)[0]

        # Parse metadata
        taskinfo = json.load(open(taskfile))
        descriptor = taskinfo['tool']
        invocation = taskinfo['invocation']
        input_data = taskinfo['dataloc']
        output_loc = utils.truepath(taskinfo['taskloc'])

        if(verbose):
            print("Fetching descriptor and invocation...")
        # Get descriptor and invocation
        desc_local = utils.get(descriptor, self.localtaskdir)[0]
        invo_local = utils.get(invocation, self.localtaskdir)[0]

        # Get input data, if running remotely
        if not kwargs.get("local") and \
           any([dl.startswith("s3://") for dl in input_data]):
            if(verbose):
                print("Fetching input data...")
            localdatadir = op.join(self.localtaskdir, "data")
            for dataloc in input_data:
                utils.get(dataloc, localdatadir)
            # Move to correct location
            os.chdir(localdatadir)
        else:
            if(verbose):
                print("Skipping data fetch (local execution)...")
            if kwargs.get("workdir") and op.exists(kwargs.get("workdir")):
                os.chdir(kwargs["workdir"])

        if(verbose):
            print("Beginning execution...")
        # Launch task
        copts = ['launch', desc_local, invo_local]
        if kwargs.get("volumes"):
            volumes = " ".join(kwargs.get("volumes"))
            copts += ['-v', volumes]
        if kwargs.get("user"):
            copts += ['-u']

        start_time = time.time()
        self.provLaunch(copts, verbose=verbose, **kwargs)
        if(verbose):
            print(self.output)
        duration = time.time() - start_time

        # Get list of bosh exec outputs
        with open(desc_local) as fhandle:
            outputs_all = json.load(fhandle)["output-files"]

        outputs_present = []
        outputs_all = bosh.evaluate(desc_local, invo_local, 'output-files/')
        for outfile in outputs_all.values():
            outputs_present += [outfile] if op.exists(outfile) else []

        # Write memory to file
        memoutf = "task-{}-memory.csv".format(self.task_id)
        with open(op.join(self.localtaskdir, memoutf), "w") as fhandle:
            writer = csv.writer(fhandle, delimiter=',')
            for row in self.memory:
                writer.writerow(row)
        utils.post(op.join(self.localtaskdir, memoutf), remotetaskdir)

        # Write cpu timing to file
        cpuoutf = "task-{}-cputiming.csv".format(self.task_id)
        with open(op.join(self.localtaskdir, cpuoutf), "w") as fhandle:
            writer = csv.writer(fhandle, delimiter=',')
            for row in self.cpu:
                writer.writerow(row)
        utils.post(op.join(self.localtaskdir, cpuoutf), remotetaskdir)

        # Write stdout to file
        stdoutf = "task-{}-stdout.txt".format(self.task_id)
        with open(op.join(self.localtaskdir, stdoutf), "w") as fhandle:
            fhandle.write(self.output.stdout)
        utils.post(op.join(self.localtaskdir, stdoutf), remotetaskdir)

        # Write sterr to file
        stderrf = "task-{}-stderr.txt".format(self.task_id)
        with open(op.join(self.localtaskdir, stderrf), "w") as fhandle:
            fhandle.write(self.output.stderr)
        utils.post(op.join(self.localtaskdir, stderrf), remotetaskdir)

        # Write summary values to file, including:
        summary = {"duration": duration,
                   "exitcode": self.output.exit_code,
                   "outputs": [],
                   "mem": op.join(remotetaskdir, memoutf),
                   "cpu": op.join(remotetaskdir, cpuoutf),
                   "stdout": op.join(remotetaskdir, stdoutf),
                   "stderr": op.join(remotetaskdir, stderrf)}

        if not kwargs.get("local"):
            if(verbose):
                print("Uploading outputs...")
            # Push outputs
            for local_output in outputs_present:
                if(verbose):
                    print("{} --> {}".format(local_output, output_loc))
                summary["outputs"] += utils.post(local_output, output_loc)
        else:
            if(verbose):
                print("Skipping uploading outputs (local execution)...")
            summary["outputs"] = outputs_present

        summarf = "task-{}-summary.json".format(self.task_id)
        with open(op.join(self.localtaskdir, summarf), "w") as fhandle:
            fhandle.write(json.dumps(summary, indent=4, sort_keys=True) + "\n")
        utils.post(op.join(self.localtaskdir, summarf), remotetaskdir)

    def execWrapper(self, *options, **kwargs):
        # if reprozip: use it
        if not subprocess.Popen("type reprozip 2>/dev/null", shell=True).wait():
            if kwargs.get("verbose"):
                print("Reprozip found; will use to record provenance!")
            cmd = 'reprozip usage_report --disable'
            p = subprocess.Popen(cmd, shell=True).wait()

            cmd = 'reprozip trace -w --dir={}/task-{}-reprozip/ bosh exec {}'
            p = subprocess.Popen(cmd.format(self.localtaskdir,
                                            self.task_id,
                                            " ".join(options)),
                                 shell=True).wait()

            cmd = ('reprozip pack --dir={0}/task-{1}-reprozip/ '
                   '{0}/task-{1}-reprozip'.format(self.localtaskdir,
                                                  self.task_id))
            p = subprocess.Popen(cmd, shell=True).wait()
        else:
            if kwargs.get("verbose"):
                print("Reprozip not found; install to record more provenance!")
            self.output = bosh.execute(*options)

    def provLaunch(self, options, **kwargs):
        pr = cProfile.Profile()
        pr.enable()
        mem_usage = memory_usage((self.execWrapper, options, kwargs),
                                 interval=0.5,
                                 include_children=True,
                                 multiprocess=True,
                                 timestamps=True)
        pr.disable()
        ps = pstats.Stats(pr).sort_stats("cumulative").reverse_order()
        headings = ["process", "ncall", "norecall",
                    "totaltime", "cumulativetime", "subcalls"]
        reformed_timing = [["{0}#{1}({2})".format(*key),
                            ps.stats[key][0],
                            ps.stats[key][1],
                            ps.stats[key][2],
                            ps.stats[key][3],
                            ps.stats[key][4]]
                           for key in ps.stats.keys()]
        sorted_timing = sorted(reformed_timing, key=lambda i: i[-2])
        timing_table = [headings] + sorted_timing
        # To sync with RAM recording, look for rows with 'memory_profiler.py'

        self.cpu = timing_table
        self.memory = [("RAM (MB)", "systemtime")] + mem_usage
