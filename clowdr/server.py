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
from botocore import UNSIGNED
from botocore.client import Config

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
    cli = boto3.client("s3") # , config=Config(signature_version=UNSIGNED))
    tmplist = []
    for obj in objlist:
        tmpdict = {}
        key = obj["key"]
        bucket = obj["bucket_name"]
        for key2 in obj.keys():
            tmpdict[key2] = obj[key2]

        with open(obj["fname"]) as fhandle:
            tmpdict["contents"] = json.load(fhandle)

        if kwargs.get("descriptor"):
            tmpdict["name"] = tmpdict["contents"]["name"]
        elif kwargs.get("invocation"):
            tmpdict["name"] = op.basename(key)
        elif kwargs.get("summary"):
            tmpdict["id"] = op.splitext(op.basename(obj["fname"]))[0].split('-')[1]
            _, key = utils.splitS3Path(tmpdict["contents"]["stdout"])
            with open(op.join(outdir, op.basename(key))) as fhandle:
                tmpdict["out"] = fhandle.read()
            _, key = utils.splitS3Path(tmpdict["contents"]["stderr"])
            with open(op.join(outdir, op.basename(key))) as fhandle:
                tmpdict["err"] = fhandle.read()
        elif kwargs.get("task"):
            tmpdict["id"] = op.splitext(op.basename(obj["fname"]))[0].split('-')[1]
            if kwargs.get("data").get("invocation"):
                invocs = kwargs["data"]["invocation"]
                invname = op.basename(tmpdict["contents"]["invocation"])
                invoc = [inv for inv in invocs if inv["name"] == invname][0]
                tmpdict["invocname"] = invoc["name"]
                tmpdict["invocurl"] = invoc["url"]
                tmpdict["invocontents"] = invoc["contents"]
            if kwargs.get("data").get("summary"):
                summars = kwargs["data"]["summary"]
                summar = [summ for summ in summars if summ["id"] == tmpdict["id"]]
                if len(summar) > 0:
                    tmpdict["exitcode"] = str(summar[0]["contents"]["exitcode"])
                    tmpdur = float(summar[0]["contents"]["duration"])
                    tmpdict["duration"] = str(datetime.timedelta(seconds=tmpdur))
                    tmpdict["outputs"] = summar[0]["contents"]["outputs"]
                    tmpdict["stdout"] = summar[0]["out"]
                    tmpdict["stderr"] = summar[0]["err"]

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
        s3 = boto3.resource("s3") # , config=Config(signature_version=UNSIGNED))
        cli = boto3.client("s3") # , config=Config(signature_version=UNSIGNED))
        buck = s3.Bucket(bucket)
        objs = buck.objects.filter(Prefix=rpath)
        objs = [{"key": obj.key,
                 "bucket_name": obj.bucket_name,
                 "date": obj.last_modified.strftime("%b %d, %Y (%T)"),
                 "url": cli.generate_presigned_url("get_object",
                                                   Params={"Bucket": obj.bucket_name,
                                                           "Key": obj.key},
                                                   ExpiresIn=None)}
                for obj in objs]
        for idx, obj in enumerate(objs):
            outfname = op.join(outdir, op.basename(obj["key"]))
            buck.download_file(obj["key"], Filename=outfname)
            objs[idx]["fname"] = outfname
    else:
        objs = [{"key": op.join(dp, f),
                 "hostname": hostname,
                 "date": op.getmtime(op.join(dp, f)).strftime("%b %d, %Y (%T)"),
                 "fname": op.join(dp, f),
                 "url": None}
                for dp, dn, fnames in os.walk(clowdrloc)
                for f in fnames]

    task = [obj for obj in objs if "task-" in obj["key"]]
    summ = [obj for obj in objs if "summary-" in obj["key"]]
    desc = [obj for obj in objs if "descriptor" in obj["key"]]
    invo = [obj for obj in objs if "invocation" in obj["key"]]
    return (s3bool, task, summ, desc, invo)


def updateIndex(**kwargs):
    clowdrloc = shareapp.config.get("clowdrloc")
    tmpdir = shareapp.config.get("tmpdir")
    print(tmpdir)
    s3bool, task, summ, desc, invo = getRecords(clowdrloc, tmpdir)

    print("updating!")

    desc = parseJSON(tmpdir, desc, s3bool, descriptor=True)[0]
    invo = parseJSON(tmpdir, invo, s3bool, invocation=True)
    summ = parseJSON(tmpdir, summ, s3bool, summary=True)
    task = parseJSON(tmpdir, task, s3bool, task=True,
                     data={"invocation": invo,
                           "summary"   : summ})

    data = {"clowdrloc" : clowdrloc,
            "tool"      : desc,
            "tasks"     : task}

    fname = op.join(tmpdir, "clowdrsitedata.json")
    with open(fname, "w") as fhandle:
        fhandle.write(json.dumps(data))

    shareapp.config["datapath"] = fname
