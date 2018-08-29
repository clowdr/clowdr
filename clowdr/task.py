#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/task.py
# Created by Greg Kiar on 2018-02-28.
# Email: gkiar@mcin.ca

from argparse import ArgumentParser
from datetime import datetime
from time import mktime, localtime
from subprocess import PIPE
import multiprocessing as mp
import numpy as np
import os.path as op
import subprocess
import psutil
import time
import json
import csv
import os
import re
import warnings


warnings.filterwarnings("ignore", message="numpy.dtype size changed")

import pandas as pd

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
            print("Fetching metadata...", flush=True)
        remotetaskdir = op.dirname(taskfile)
        taskfile = utils.get(taskfile, self.localtaskdir)[0]

        # Parse metadata
        taskinfo = json.load(open(taskfile))
        descriptor = taskinfo['tool']
        invocation = taskinfo['invocation']
        input_data = taskinfo['dataloc']
        output_loc = utils.truepath(taskinfo['taskloc'])

        if(verbose):
            print("Fetching descriptor and invocation...", flush=True)
        # Get descriptor and invocation
        desc_local = utils.get(descriptor, self.localtaskdir)[0]
        invo_local = utils.get(invocation, self.localtaskdir)[0]

        # Get input data, if running remotely
        if not kwargs.get("local") and \
           any([dl.startswith("s3://") for dl in input_data]):
            if(verbose):
                print("Fetching input data...", flush=True)
            localdatadir = op.join("/data")
            local_input_data = []
            for dataloc in input_data:
                local_input_data += utils.get(dataloc, localdatadir)
            # Move to correct location
            os.chdir(localdatadir)
        else:
            if(verbose):
                print("Skipping data fetch (local execution)...", flush=True)
            if kwargs.get("workdir") and op.exists(kwargs.get("workdir")):
                os.chdir(kwargs["workdir"])

        if(verbose):
            print("Beginning execution...", flush=True)
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
            print(self.output, flush=True)
        duration = time.time() - start_time

        # Get list of bosh exec outputs
        with open(desc_local) as fhandle:
            outputs_all = json.load(fhandle)["output-files"]

        outputs_present = []
        outputs_all = bosh.evaluate(desc_local, invo_local, 'output-files/')
        for outfile in outputs_all.values():
            outputs_present += [outfile] if op.exists(outfile) else []

        # Write memory/cpu stats to file
        usagef = "task-{}-usage.csv".format(self.task_id)
        self.cpu_ram_usage.to_csv(op.join(self.localtaskdir, usagef),
                                  sep=',', index=False)
        utils.post(op.join(self.localtaskdir, usagef), remotetaskdir)

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

        start_time = datetime.fromtimestamp(mktime(localtime(start_time)))
        summary = {"duration": duration,
                   "launchtime": str(start_time),
                   "exitcode": self.output.exit_code,
                   "outputs": [],
                   "usage": op.join(remotetaskdir, usagef),
                   "stdout": op.join(remotetaskdir, stdoutf),
                   "stderr": op.join(remotetaskdir, stderrf)}

        if not kwargs.get("local"):
            if(verbose):
                print("Uploading outputs...", flush=True)
            # Push outputs
            for local_output in outputs_present:
                if(verbose):
                    print("{} --> {}".format(local_output, output_loc),
                          flush=True)
                tmpouts = utils.post(local_output, output_loc)
                print(tmpouts)
                summary["outputs"] += tmpouts
        else:
            if(verbose):
                print("Skipping uploading outputs (local execution)...",
                      flush=True)
            summary["outputs"] = outputs_present

        summarf = "task-{}-summary.json".format(self.task_id)
        with open(op.join(self.localtaskdir, summarf), "w") as fhandle:
            fhandle.write(json.dumps(summary, indent=4, sort_keys=True) + "\n")
        utils.post(op.join(self.localtaskdir, summarf), remotetaskdir)

        # If not local, delete all: inputs, outputs, and summaries
        if not kwargs.get("local"):
            for local_output in outputs_present:
                utils.remove(local_output)
            utils.remove(self.localtaskdir)
            for local_input in local_input_data:
                utils.remove(local_input)

    def execWrapper(self, sender):
        # if reprozip: use it
        if not subprocess.Popen("type reprozip 2>/dev/null", shell=True).wait():
            if self.runner_kwargs.get("verbose"):
                print("Reprozip found; will use to record provenance!",
                      flush=True)
            cmd = 'reprozip usage_report --disable'
            p = subprocess.Popen(cmd, shell=True).wait()

            cmd = 'reprozip trace -w --dir={}/task-{}-reprozip/ bosh exec {}'
            p = subprocess.Popen(cmd.format(self.localtaskdir,
                                            self.task_id,
                                            " ".join(self.runner_args)),
                                 shell=True).wait()

            cmd = ('reprozip pack --dir={0}/task-{1}-reprozip/ '
                   '{0}/task-{1}-reprozip'.format(self.localtaskdir,
                                                  self.task_id))
            p = subprocess.Popen(cmd, shell=True).wait()
        else:
            if self.runner_kwargs.get("verbose"):
                print("Reprozip not found; install to record more provenance!",
                      flush=True)
            sender.send(bosh.execute(*self.runner_args))

    def provLaunch(self, options, **kwargs):
        self.runner_args = options
        self.runner_kwargs = kwargs
        timing, cpu, ram = self.monitor(self.execWrapper, **kwargs)

        basetime = timing[0]

        total_df = pd.DataFrame(columns=['time', 'cpu', 'ram'])
        for ttime, tcpu, tram in zip(timing, cpu, ram):
            total_df.loc[len(total_df)] = (ttime-basetime, tcpu, tram)

        self.cpu_ram_usage = total_df

    def monitor(self, target, **kwargs):
        ram_lut = {'B': 1/1024/1024,
                   'KiB': 1/1024,
                   'MiB': 1,
                   'GiB': 1024}
        self.output, sender = mp.Pipe(False)
        worker_process = mp.Process(target=target, args=(sender,))
        worker_process.start()
        p = psutil.Process(worker_process.pid)

        log_time = []
        log_cpu = []
        log_mem = []
        while worker_process.is_alive():
            try:
                cpu = p.cpu_percent()
                ram = p.memory_info()[0]*ram_lut['B']

                for subproc in p.children(recursive=True):
                    if not subproc.is_running():
                        continue

                    subproc_dict = subproc.as_dict(attrs=['pid',
                                                          'name',
                                                          'cmdline',
                                                          'cpu_percent',
                                                          'memory_info'])
                    if subproc_dict['name'] == 'docker':
                        call = subproc_dict['cmdline'][-1]
                        tcmd = psutil.Popen(["docker", "ps", "-q"], stdout=PIPE)
                        running = tcmd.communicate()[0].decode('utf-8')
                        running = running.split('\n')
                        tcmd = psutil.Popen(["docker", "inspect"] + running,
                                            stdout=PIPE, stderr=PIPE)
                        tinf = json.loads(tcmd.communicate()[0].decode('utf-8'))
                        for tcon in tinf:
                            if (tcon.get("Config") and
                               tcon.get("Config").get("Cmd") and
                               call in tcon['Config']['Cmd']):
                                tid = tcon['Id']
                                tcmd = psutil.Popen([
                                                     "docker",
                                                     "stats",
                                                     tid,
                                                     "--no-stream",
                                                     "--format",
                                                     "'{{.MemUsage}} " +\
                                                     "{{.CPUPerc}}'"
                                                    ],
                                                    stdout=PIPE)
                                tout = tcmd.communicate()[0].decode('utf-8')
                                tout = tout.strip('\n').replace("'", "")

                                _ram, _, _, _cpu = tout.split(' ')
                                _ram, ending = re.match('([0-9.]+)([MGK]?i?B)',
                                                        _ram).groups()
                                ram += float(_ram) * ram_lut[ending]
                                cpu += float(_cpu.strip('%'))

                    else:
                        cpu += subproc_dict['cpu_percent']
                        ram += subproc_dict['memory_info'][0] * ram_lut['B']

                    if kwargs.get('verbose'):
                        print(cpu, ram)

                tim = time.time()
                log_time.append(tim)
                log_cpu.append(cpu)
                log_mem.append(ram)
                time.sleep(0.1)

            except (psutil._exceptions.AccessDenied,
                    psutil._exceptions.NoSuchProcess,
                    TypeError, ValueError, AttributeError) as e:
                continue

        worker_process.join()
        self.output = self.output.recv()
        return log_time, log_cpu, log_mem
