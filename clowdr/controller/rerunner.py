#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/controller/rerunner.py
# Created by Greg Kiar on 2018-06-11.
# Email: gkiar@mcin.ca

import os.path as op
import json
import sys
import os
import re

from clowdr import utils


def getTasks(provdir, runid, rerun_mode):
    runpath = utils.truepath(op.join(provdir, runid, 'clowdr'))

    files = os.listdir(runpath)
    r_all = re.compile('^.*task-([0-9]+)[.]json$')
    all_tasks = sorted([op.join(runpath, f)
                        for f in filter(r_all.match, files)])

    if rerun_mode == "all":
        return all_tasks

    all_ids = set([r_all.match(f).group(1)
                   for f in all_tasks
                   if r_all.match(f)])

    task_dict = {tid:task for (tid, task) in zip(sorted(all_ids), all_tasks)}

    r_complete = re.compile('^.*task-([0-9]+)-summary[.]json$')
    complete_ids = set([r_complete.match(f).group(1)
                        for f in files
                        if r_complete.match(f)])

    incomplete_ids = sorted(all_ids - complete_ids)

    if rerun_mode == "incomplete":
        return [task_dict[tid] for tid in incomplete_ids]

    complete_summaries = sorted([op.join(runpath, f)
                                 for f in filter(r_complete.match, files)])

    failed_ids = []
    for (tid, summary) in zip(sorted(complete_ids), complete_summaries):
        with open(summary, 'r') as fhandle:
            task_summary = json.load(fhandle)
            if task_summary["exitcode"]:
                failed_ids += [tid]

    return [task_dict[tid] for tid in failed_ids]
