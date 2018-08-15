#!/usr/bin/env python

import os.path as op
import pandas as pd
import json
import os
import re

indir = 'examples/task/bids-example/clowdr/'
files = [op.join(indir, f) for f in os.listdir(indir) if f.endswith('usage.csv')]

for idx, f in enumerate(files):
    tsk_id = re.findall(r'.+task-([0-9])-usage.csv', f)[0]

    f1 = op.join(indir, 'task-' + tsk_id + '.json')
    with open(f1) as fhandle1:
        tmp_task = json.load(fhandle1)
        tmp_desc = op.basename(tmp_task['tool'])
        tmp_name = json.load(open(op.join(indir, tmp_desc)))['name']
        f2 = op.join(indir, op.basename(tmp_task['invocation']))
        with open(f2) as fhandle2:
            tmp_invo = json.load(fhandle2)

    tmp_df = pd.read_csv(f)
    tmp_df = tmp_df.loc[tmp_df['ram'] > -1][['time', 'ram']].reset_index(drop=True)

    tmp_cl = pd.DataFrame({'task': [tsk_id] * len(tmp_df),
                           'tool': [tmp_name] * len(tmp_df)})
    tmp_df = tmp_cl.join(tmp_df)

    invo_list = []
    for tmp_key in tmp_invo:
        invo_list += [{'task': tsk_id,
                       'tool': tmp_name,
                       'Invocation Param': tmp_key,
                       'Invocation Value': tmp_invo[tmp_key]}]

    tmp_inv = pd.read_json(json.dumps(invo_list))
    tmp_df = pd.concat([tmp_inv, tmp_df], axis=0, ignore_index=True, sort=False)

    if idx == 0:
        big_df = tmp_df
    else:
        big_df = big_df.append(tmp_df, ignore_index = True)

big_df.to_csv("clowdr-summary.csv", index=False)
