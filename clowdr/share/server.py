#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/share/server.py
# Created by Greg Kiar on 2018-03-01.
# Email: gkiar@mcin.ca

from flask import Flask, render_template
import os.path as op
import datetime
import tempfile
import boto3
import json
import sys
import re
import os

app = Flask(__name__)


@app.route("/clowdr/")
def index():
    return render_template('index.html', data=app.config.get("data"))


def parseJSON(outdir, objlist, buck, cli, **kwargs):
    tmplist = []
    for obj in objlist:
        tmpdict = {}
        key = obj.key
        bucket = obj.bucket_name
        outfname = op.join(outdir, op.basename(key))
        buck.download_file(key, Filename=outfname)
        tmpdict["fname"] = outfname
        tmpdict["bucket"] = bucket
        tmpdict["key"] = key
        tmpdict["url"] = cli.generate_presigned_url("get_object",
                                                    Params={"Bucket": bucket,
                                                            "Key": key},
                                                    ExpiresIn=None)
        tmpdict["date"] = obj.last_modified.strftime("%b %d, %Y (%T)")

        with open(outfname) as fhandle:
            tmpdict["contents"] = json.load(fhandle)

        if kwargs.get("descriptor"):
            tmpdict["name"] = tmpdict["contents"]["name"]
        elif kwargs.get("invocation"):
            tmpdict["name"] = op.basename(key)
        elif kwargs.get("summary"):
            tmpdict["id"] = op.splitext(op.basename(outfname))[0].split('-')[1]
        elif kwargs.get("task"):
            tmpdict["id"] = op.splitext(op.basename(outfname))[0].split('-')[1]
            if kwargs.get("data").get("descriptor"):
                tmpdict["toolname"] = kwargs["data"]["descriptor"]["name"]
                tmpdict["toolurl"] = kwargs["data"]["descriptor"]["url"]
                tmpdict["toolcontents"] = kwargs["data"]["descriptor"]["contents"]
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
                    tmpdict["exitcode"] = summar[0]["contents"]["exitcode"]
                    tmpdur = float(summar[0]["contents"]["duration"])
                    tmpdict["duration"] = str(datetime.timedelta(seconds=tmpdur))
                    tmpdict["outputs"] = summar[0]["contents"]["outputs"]

        tmplist.append(tmpdict)
    return tmplist


def updateRecord():
    clowdrloc = app.config.get("clowdrloc")
    tmpdir = tempfile.mkdtemp()

    s3 = boto3.resource("s3")
    cli = boto3.client("s3")
    bucket, offset = re.match('^s3:\/\/([\w\-\_]+)/([\w\-\_\/]+)',
                              clowdrloc).group(1, 2)
    buck = s3.Bucket(bucket)
    objs = buck.objects.filter(Prefix=offset)
    task_objs = [obj for obj in objs if "task-" in obj.key]
    summary_objs = [obj for obj in objs if "summary-" in obj.key]
    descriptor_obj = [obj for obj in objs if "descriptor" in obj.key]
    invocation_obj = [obj for obj in objs if "invocation" in obj.key]

    descriptor = parseJSON(tmpdir, descriptor_obj, buck, cli, descriptor=True)[0]
    invocation = parseJSON(tmpdir, invocation_obj, buck, cli, invocation=True)
    summary = parseJSON(tmpdir, summary_objs, buck, cli, summary=True)
    tasks = parseJSON(tmpdir, task_objs, buck, cli, task=True,
                      data={"descriptor": descriptor,
                            "invocation": invocation,
                            "summary": summary})

    app.config["data"] = {"bucket": bucket,
                          "offset": offset,
                          "tasks" : tasks}


def main():
    clowdrloc = sys.argv[1]
    app.config["clowdrloc"] = clowdrloc
    updateRecord()
    app.run(host='0.0.0.0', debug=True)


if __name__ == "__main__":
    main()

