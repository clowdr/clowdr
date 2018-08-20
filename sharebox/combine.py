#!/usr/bin/env python

import os.path as op
import pandas as pd
import numpy as np
import json
import os
import re

# Set input directory, get list of tasks
indir = '/Users/gkiar/code/gkiar/clowdr/examples/task/2018-08-20_15-56-38-SUFVLD26/clowdr'
tasks = [op.join(indir, f)
         for f in os.listdir(indir)
         if re.match(r'task-[0-9]+.json', f)]

experiment = []

# For each task...
for idx, task_file in enumerate(tasks):
    task_id = re.findall(r'.+task-([0-9]+).json', task_file)[0]
    task_dict = {}

    # Load task file...
    with open(task_file) as task_fhandle:
        # ... and extract the descriptor and invocation
        tmp_task = json.load(task_fhandle)
        descriptor_file = op.join(indir, op.basename(tmp_task['tool']))
        invocation_file = op.join(indir, op.basename(tmp_task['invocation']))

    # Load descriptor file...
    with open(descriptor_file) as descriptor_fhandle:
        # ... and extract the tool name and list of inputs
        tmp_desc = json.load(descriptor_fhandle)
        tmp_name = tmp_desc['name']
        tmp_inps = {inp['id']:inp['name'] for inp in tmp_desc['inputs']}

    # Load the invocation file...
    with open(invocation_file) as invocation_fhandle:
        # ... and extract the invocation parameters used
        tmp_invo = json.load(invocation_fhandle)
        tmp_invo_dict = {}
        for inp in tmp_inps:
            key = 'Param: {}'.format(tmp_inps[inp])
            if tmp_invo.get(inp):
                tmp_invo_dict[key] = tmp_invo[inp]

    # Load the summary file...
    summary_file = op.join(indir, 'task-' + task_id + '-summary.json')
    with open(summary_file) as summary_fhandle:
        # ... and extract the exit code and duration
        tmp_summ = json.load(summary_fhandle)
        tmp_time = tmp_summ['launchtime']
        tmp_ecod = tmp_summ['exitcode']
        tmp_durr = tmp_summ['duration']

    # Load stdout...
    stdout_file = op.join(indir, 'task-' + task_id + '-stdout.txt')
    with open(stdout_file) as stdout_fhandle:
        # ... and save it
        tmp_sout = stdout_fhandle.read()

    # Load stderr...
    stderr_file = op.join(indir, 'task-' + task_id + '-stderr.txt')
    with open(stderr_file) as stderr_fhandle:
        # ... and save it
        tmp_serr = stderr_fhandle.read()

    # Load the usage file...
    usage_file = op.join(indir, 'task-' + task_id + '-usage.csv')
    tmp_df = pd.read_csv(usage_file)
    # ... and extract the RAM time series and summary of it
    tmp_df = tmp_df.loc[tmp_df['ram'] > -1][['time', 'ram']].reset_index(drop=True)
    tmp_tim = [w for v in tmp_df[['time']].values.tolist() for w in v]
    tmp_ram = [w for v in tmp_df[['ram']].values.tolist() for w in v]
    tmp_max = np.max(tmp_ram)

    # Put useful pieces into dictionary
    task_dict['Tool Name'] = tmp_name
    task_dict['Task ID'] = task_id
    task_dict['Exit Code'] = tmp_ecod
    task_dict['RAM: Max (MB)'] = tmp_max
    task_dict['RAM: Series (MB)'] = tmp_ram
    task_dict['Time: Total (s)'] = tmp_durr
    task_dict['Time: Series (s)'] = tmp_tim
    task_dict['Time: Start'] = tmp_time
    task_dict.update(tmp_invo_dict)
    task_dict['Log: Output'] = tmp_sout
    task_dict['Log: Error'] = tmp_serr

    # Add task to the experiment
    experiment += [task_dict]

# Save experiment to file
with open('./sharebox/clowdr-summary.json', 'w') as fhandle:
    fhandle.write(json.dumps(experiment, indent=4, sort_keys=True))
