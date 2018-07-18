#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/share/server.py
# Created by Greg Kiar on 2018-03-01.
# Email: gkiar@mcin.ca

from flask import Flask, render_template, redirect
import os.path as op
import datetime
import tempfile
import boto3
import json
import sys
import re
import os
from botocore.handlers import disable_signing

from clowdr import utils


shareapp = Flask(__name__)


@shareapp.route("/")
def index():
    with open(shareapp.config.get("datapath")) as fhandle:
        data = json.load(fhandle)
    return render_template("index.html", data=data)


@shareapp.route("/refresh")
def update():
    updateIndex()
    return redirect("/")


def parseJSON(outdir, objlist, s3bool=True, **kwargs):
    tmplist = []
    for obj in objlist:
        tmpdict = {}
        key = obj["key"]
        for key2 in obj.keys():
            tmpdict[key2] = obj[key2]

        with open(obj["fname"]) as fhandle:
            tmpdict["contents"] = json.load(fhandle)

        if kwargs.get("descriptor"):
            tmpdict["name"] = tmpdict["contents"]["name"]
        elif kwargs.get("invocation"):
            tmpdict["name"] = op.basename(key)
        elif kwargs.get("summary"):
            tmpid = op.splitext(op.basename(obj["fname"]))[0].split('-')[1]
            tmpdict["id"] = tmpid
            outfname = op.basename(tmpdict["contents"]["stdout"])
            with open(op.join(outdir, outfname)) as fhandle:
                tmpdict["out"] = fhandle.read()
            errfname = op.basename(tmpdict["contents"]["stderr"])
            with open(op.join(outdir, errfname)) as fhandle:
                tmpdict["err"] = fhandle.read()
            tmpdict["exitcode"] = tmpdict["contents"]["exitcode"]
            tmpdur = tmpdict["contents"]["duration"]
            tmpdict["duration"] = str(datetime.timedelta(seconds=tmpdur))
            tmpdict["outputs"] = tmpdict["contents"]["outputs"]
        elif kwargs.get("task"):
            tmpid = op.splitext(op.basename(obj["fname"]))[0].split('-')[1]
            tmpdict["id"] = tmpid
            if kwargs.get("data").get("invocation"):
                invocs = kwargs["data"]["invocation"]
                invname = op.basename(tmpdict["contents"]["invocation"])
                invoc = [inv for inv in invocs if inv["name"] == invname][0]
                tmpdict["invoc"] = invoc
            if kwargs.get("data").get("summary"):
                summars = kwargs["data"]["summary"]
                summar = [summ
                          for summ in summars
                          if summ["id"] == tmpdict["id"]]
                if len(summar) > 0:
                    tmpdict["summary"] = summar[0]
                else:
                    tmpdict["summary"] = {"exitcode": None}

        tmplist.append(tmpdict)
    return tmplist


def getRecords(clowdrloc, outdir, **kwargs):
    # get remote records, open local records
    try:
        bucket, rpath = utils.splitS3Path(clowdrloc)
        s3bool = True
    except AttributeError:
        s3bool = False
        hostname = kwargs.get("hostname")

    if s3bool:
        s3 = boto3.resource("s3")  # ,config=Config(signature_version=UNSIGNED))
        cli = boto3.client("s3")  # ,config=Config(signature_version=UNSIGNED))
        buck = s3.Bucket(bucket)
        presurl = cli.generate_presigned_url("get_object",
                                             Params={"Bucket": obj.bucket_name,
                                                     "Key": obj.key},
                                             ExpiresIn=None)
        objs = buck.objects.filter(Prefix=rpath)
        objs = [{"key": obj.key,
                 "bucket_name": obj.bucket_name,
                 "date": obj.last_modified.strftime("%b %d, %Y (%T)"),
                 "url": presurl}
                for obj in objs]
        for idx, obj in enumerate(objs):
            outfname = op.join(outdir, op.basename(obj["key"]))
            buck.download_file(obj["key"], Filename=outfname)
            objs[idx]["fname"] = outfname
    else:
        tsp = datetime.datetime.fromtimestamp
        objs = [{"key": op.join(clowdrloc, f),
                 "hostname": hostname,
                 "date": tsp(op.getmtime(op.join(clowdrloc,
                                         f))).strftime("%b %d, %Y (%T)"),
                 "fname": op.join(clowdrloc, f),
                 "url": None}
                for f in os.listdir(clowdrloc)]

    task = [obj for obj in objs
            if ("task-" in obj["key"] and
                obj["key"].endswith('json') and
                "summary" not in obj["key"])]
    summ = [obj for obj in objs if "-summary" in obj["key"]]
    desc = [obj for obj in objs if "descriptor" in obj["key"]]
    invo = [obj for obj in objs if "invocation" in obj["key"]]
    outs = [obj for obj in objs if ".txt" in obj["key"]]
    return (s3bool, task, summ, desc, invo, outs)


def updateIndex(**kwargs):
    clowdrloc = shareapp.config.get("clowdrloc")
    tmpdir = shareapp.config.get("tmpdir")
    s3bool, task, summ, desc, invo, outs = getRecords(clowdrloc, tmpdir)

    if not s3bool:
        tmpdir = clowdrloc

    desc = parseJSON(tmpdir, desc, s3bool, descriptor=True)[0]
    invo = parseJSON(tmpdir, invo, s3bool, invocation=True)
    summ = parseJSON(tmpdir, summ, s3bool, summary=True,
                     data={"outs": outs})
    task = parseJSON(tmpdir, task, s3bool, task=True,
                     data={"invocation": invo,
                           "summary": summ})

    data = {"clowdrloc": clowdrloc,
            "tool": desc,
            "tasks": task}

    fname = op.join(tmpdir, "clowdrsitedata.json")
    with open(fname, "w") as fhandle:
        fhandle.write(json.dumps(data, indent=2))

    shareapp.config["datapath"] = fname
