#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/controller/metadata.py
# Created by Greg Kiar on 2018-02-28.
# Email: gkiar@mcin.ca

from copy import deepcopy
import os.path as op
import datetime
import time
import string
import json
import sys
import os

from clowdr import utils


def consolidateTask(tool, invocation, clowdrloc, dataloc, bids=False, sweep=[],
                    verbose=False, **kwargs):
    """consolidateTask
    Creates Clowdr task JSON files and Boutiques invocations which summarize all
    associated metadata with the tasks being launched.

    Parameters
    ----------
    tool : str
        Path to a boutiques descriptor for the tool to be run.
    invocation : str
        Path to a boutiques invocation for the tool and parameters to be run.
    clowdrloc : str
        Path for storing Clowdr intermediate files and output logs.
    dataloc : str
        Path for accessing input data on an S3 bucket (must include s3://) or
        localhost for non-cloud hosted data.
    bids : bool (default = False)
        Flag toggling BIDS-aware metadata preparation.
    sweep : list (default = [])
        List of parameters to sweep over in the provided invocations.
    verbose : bool (default = False)
        Flag toggling verbose output printing.
    **kwargs : dict
        Arbitrary additional keyword arguments which may be passed.

    Returns
    -------
    tuple: (list, list)
        The task dictionary JSONs, and associated Boutiques invocation files.
    """

    ts = time.time()
    dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d_%H-%M-%S')
    randx = utils.randstring(8)
    modif = "{}-{}".format(dt, randx)

    # Scrub inputs
    tool = utils.truepath(tool)
    invocation = utils.truepath(invocation)
    clowdrloc = utils.truepath(clowdrloc)
    dataloc = utils.truepath(dataloc)

    # Initialize task dictionary
    taskdict = {}
    with open(tool) as fhandle:
        toolname = json.load(fhandle)["name"].replace(' ', '-')
    taskloc = op.join(clowdrloc, modif, 'clowdr')
    os.makedirs(taskloc)

    taskdict["taskloc"] = op.join(clowdrloc, modif, toolname)
    taskdict["dataloc"] = [dataloc]
    taskdict["invocation"] = utils.get(invocation, taskloc)[0]
    taskdict["tool"] = utils.get(tool, taskloc)[0]

    # Case 1: User supplies directory of invocations
    if op.isdir(invocation):
        tempinvocations = os.listdir(invocation)
        taskdicts = []
        invocations = []
        for invoc in tempinvocations:
            tempdict = deepcopy(taskdict)
            tempinvo = utils.get(op.join(invocation, invoc), taskloc)
            tempdict["invocation"] = utils.truepath(tempinvo[0])
            invocations += tempinvo
            taskdicts += [tempdict]

    # Case 2: User supplies a single invocation
    else:
        # Case 2a: User is running a BIDS app
        if bids:
            taskdicts, invocations = bidsTasks(taskloc, taskdict)

        # Case 2b: User is quite simply just launching a single invocation
        else:
            taskdicts = [taskdict]
            invocations = [taskdict["invocation"]]

    # Post-case: User is performing a parameter sweep over invocations
    if sweep:
        for sweep_param in sweep:
            taskdicts, invocations = sweepTasks(taskdicts, invocations,
                                                sweep_param)

    # Store task definition files to disk
    taskdictnames = []
    for idx, taskdict in enumerate(taskdicts):
        taskfname = op.join(taskloc, "task-{}.json".format(idx))
        taskdictnames += [taskfname]
        with open(taskfname, 'w') as fhandle:
            fhandle.write(json.dumps(taskdict, indent=4, sort_keys=True))

    return (taskdictnames, invocations)


def sweepTasks(taskdicts, invocations, sweep_param):
    """sweepTasks
    Sweeps through provided fields for creating more tasks than specified.

    Parameters
    ----------
    taskdicts : str
        Dictionary of the tasks
    invocations : str
        Corresponding invocations for each task dictionary
    sweep_param : str
        Parameter to be swept over in each invocation

    Returns
    -------
    tuple: (list, list)
        The task dictionary JSONs, and associated Boutiques invocation files.
    """
    tdicts = []
    invos = []

    for ttdict, tinvo in zip(taskdicts, invocations):
        invo = json.load(open(tinvo))
        sweep_vals = invo.get(sweep_param)

        for sval in sweep_vals:
            tempdict = deepcopy(ttdict)
            invo[sweep_param] = sval

            invofname = op.join(tinvo.replace(".json", "_sweep-"
                                "{0}-{1}.json".format(sweep_param, sval)))
            with open(invofname, 'w') as fhandle:
                fhandle.write(json.dumps(invo, indent=4, sort_keys=True))

            tempdict["invocation"] = invofname
            invos += [invofname]
            tdicts += [tempdict]
    return (tdicts, invos)


def bidsTasks(clowdrloc, taskdict):
    """bidsTask
    Scans through BIDS app fields for creating more tasks than specified.

    Parameters
    ----------
    clowdrloc : str
        Path for storing Clowdr intermediate files and outputs
    taskdict : str
        Dictionary of the tasks (pre-BIDS-ification)

    Returns
    -------
    tuple: (list, list)
        The task dictionary JSONs, and associated Boutiques invocation files.
    """

    dataloc = taskdict["dataloc"][0]
    invocation = taskdict["invocation"]

    invo = json.load(open(invocation))
    participants = invo.get("participant_label")
    sessions = invo.get("session_label")

    # Case 1: User is running BIDS group-level analysis
    if invo.get("analysis_level") == "group":
        return ([taskdict], [invocation])

    # Case 2: User is running BIDS participant- or session-level analysis
    #       ... and specified neither participant(s) nor session(s)
    elif not participants and not sessions:
        return ([taskdict], [invocation])

    # Case 3: User is running BIDS participant- or session-level analysis
    #       ... and specified participant(s) but not session(s)
    elif participants and not sessions:
        taskdicts = []
        invos = []
        for part in participants:
            partstr = "sub-{}".format(part)

            tempdict = deepcopy(taskdict)
            tempdict["dataloc"] = [op.join(dataloc, partstr)]

            invo["participant_label"] = [part]

            invofname = op.join(clowdrloc, "invocation_sub-{}.json".format(part))
            with open(invofname, 'w') as fhandle:
                fhandle.write(json.dumps(invo, indent=4, sort_keys=True))

            tempdict["invocation"] = invofname
            invos += [invofname]
            taskdicts += [tempdict]
        return (taskdicts, invos)

    # Case 4: User is running BIDS participant- or session-level analysis
    #       ... and specified participants(s) and session(s)
    elif participants and sessions:
        taskdicts = []
        invos = []
        for part in participants:
            partstr = "sub-{}".format(part)

            for sesh in sessions:
                seshstr = "ses-{}".format(sesh)

                tempdict = deepcopy(taskdict)
                tempdict["dataloc"] = [op.join(dataloc, partstr, seshstr)]

                invo["participant_label"] = [part]
                invo["session_label"] = [sesh]

                invofname = op.join(clowdrloc, "invocation_"
                                    "sub-{}_ses-{}.json".format(part, sesh))
                with open(invofname, 'w') as fhandle:
                    fhandle.write(json.dumps(invo, indent=4, sort_keys=True))

                tempdict["invocation"] = invofname
                invos += [invofname]
                taskdicts += [tempdict]
        return (taskdicts, invos)

    # Case 5: User is running BIDS participant- or session-level analysis
    #       ... and specified sessions(s) but not participant(s)
    elif sessions and not participants:
        taskdicts = []
        invos = []
        for sesh in sessions:
            seshstr = "ses-{}".format(sesh)

            tempdict = deepcopy(taskdict)
            tempdict["dataloc"] = [op.join(dataloc)]

            invo["session_label"] = [sesh]

            invofname = op.join(clowdrloc, "invocation_ses-{}.json".format(sesh))
            with open(invofname, 'w') as fhandle:
                fhandle.write(json.dumps(invo, indent=4, sort_keys=True))

            tempdict["invocation"] = invofname
            invos += [invofname]
            taskdicts += [tempdict]
        return (taskdicts, invos)


def prepareForRemote(tasks, tmploc, clowdrloc):
    """prepareForRemote
    Scans through BIDS app fields for creating more tasks than specified.

    Parameters
    ----------
    tasks : list
        List of task dictionaries on disk for Clowdr tasks.
    tmploc : str
        Temporary location where the invocations and task files are stored.
    clowdrloc : str
        Path for storing Clowdr intermediate files and outputs

    Returns
    -------
    tuple: (list, list)
        The task dictionary JSONs, and associated Boutiques invocation files,
        with paths corrected to eventual remote locations.
    """

    # Modify tasks
    for task in tasks:
        with open(task) as fhandle:
            task_dict = json.load(fhandle)

        task_dict["invocation"] = op.join(clowdrloc,
                                          op.relpath(task_dict["invocation"],
                                                     tmploc))
        task_dict["taskloc"] = op.join(clowdrloc,
                                       op.relpath(task_dict["taskloc"],
                                                  tmploc))
        task_dict["tool"] = op.join(clowdrloc, op.relpath(task_dict["tool"],
                                                          tmploc))

        with open(task, 'w') as fhandle:
            fhandle.write(json.dumps(task_dict, indent=4, sort_keys=True))
    return 0

