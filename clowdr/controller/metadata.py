#!/usr/bin/env python

from boutiques import bosh
from copy import deepcopy

import time, datetime
import os, os.path as op
import random as rnd
import string
import json


def consolidate(tool, invocation, clowdrloc, dataloc, **kwargs):
    ts = time.time()
    dt = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
    randx = "".join(rnd.choices(string.ascii_uppercase + string.digits, k=8))
    modif = "{}-{}".format(dt, randx)

    taskdict = {}
    taskdict["taskloc"] = op.join(clowdrloc, modif)

    taskdict["tool"] = tool
    # TODO: validate or retrieve descriptor
    # bosh.validate(tool)
    taskdict["dataloc"] = dataloc

    # Case 1: User supplies directory of invocations
    if op.isdir(invocation):
        invocations = os.listdir(invocation)
        taskdicts = []
        for invoc in invocations:
            tempdict = deepcopy(taskdict)
            tempdict["invocation"] = op.join(invocation, invoc)
            # TODO: validate invocations
            # bosh.invocation(tool, '-i', tempdict["invocation"])
            taskdicts += [tempdict]

    # Case 2: User supplies a single invocation
    else:

        # Case 2a: User is running a BIDS app
        if kwargs.get('bids'):
            tmpinvo = json.load(open(invocation))
            pref = "/".join(datapath.split('/')[3:])
            parties = tmpinvo.get("participant_label")
            if parties is None:
                party_info = s3.list_objects(Bucket=data_bucket, Prefix="{}sub-".format(pref), Delimiter="/")
                parties = [party['Prefix'].split('/')[-2].split('sub-')[-1]
                           for party in party_info['CommonPrefixes']]
            for party in parties:
                tmpinvo["participant_label"] = [party]
                taskdict["input_data"] = ["{}/sub-{}/".format(datapath.strip('/'), party)]
                # taskdict["input_data"] += dataset_list
                invocation = "/tmp/invocation-{}-{}.json".format(randx, party)
                with open(invocation, 'w') as fhandle:
                    fhandle.write(json.dumps(tmpinvo))
                ipath = "clowdrtask/{}-{}/invocation-{}.json".format(dt, randx, party)
                s3.upload_file(invocation, data_bucket, ipath)
                taskdict["invocation"] = "s3://{}/{}".format(data_bucket, ipath)
                metadata = '/tmp/metadata-{}-{}.json'.format(randx, party)
                with open(metadata, 'w') as fhandle:
                    fhandle.write(json.dumps(taskdict))
                s3.upload_file(metadata, data_bucket, "clowdrtask/{}-{}/metadata-{}.json".format(dt, randx, party))
                loc = "s3://{}/clowdrtask/{}-{}/metadata-{}.json".format(data_bucket, dt, randx, party)

        # Case 2b: User is quite simply just launching a single invocation
        else:
            taskdict["input_data"] = [dataloc]
            taskdict["invocation"] = invocation
            taskdicts = [taskdict]

    # Store task definition files to disk
    taskdictnames = []
    for taskdict in taskdicts:
        taskfname = 'somefilename'
        taskdictnames += [taskfname]
        with open(taskfname, 'w') as fhandle:
            fhandle.write(json.dumps(taskdict))

    return taskdictnames
