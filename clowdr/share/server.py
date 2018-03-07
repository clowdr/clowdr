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
import tempfile
import boto3
import json
import sys
import re
import os

app = Flask(__name__)


@app.route("/clowdr/")
def index():
    return render_template('index.html',
                           data=app.config.get("data"),
                           bucket=app.config.get("bucket"),
                           session=app.config.get("offset"),
                           tasks=app.config.get("tasks"))


def getJSON(outdir, objlist, buck, cli, **kwargs):
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
        elif kwargs.get("task"):
            print(kwargs.get("data").keys())
            if kwargs.get("data").get("descriptor"):
                tmpdict["toolname"] = kwargs["data"]["descriptor"]["name"]
                tmpdict["toolurl"] = kwargs["data"]["descriptor"]["url"]
            if kwargs.get("data").get("invocation"):
                invocs = kwargs["data"]["invocation"]
                invname = op.basename(tmpdict["contents"]["invocation"])
                print(invname)
                invoc = [inv for inv in invocs if inv["name"] == invname]
                tmpdict["invocname"] = invoc[0]["name"]
                tmpdict["invocurl"] = invoc[0]["url"]

        tmplist.append(tmpdict)
    return tmplist


def main():
    clowdrloc = sys.argv[1]
    tmpdir = tempfile.mkdtemp()

    s3 = boto3.resource("s3")
    cli = boto3.client("s3")
    bucket, offset = re.match('^s3:\/\/([\w\-\_]+)/([\w\-\_\/]+)',
                              clowdrloc).group(1, 2)
    buck = s3.Bucket(bucket)
    objs = buck.objects.filter(Prefix=offset)
    task_objs = [obj for obj in objs if "task-" in obj.key]
    descriptor_obj = [obj for obj in objs if "descriptor" in obj.key]
    invocation_obj = [obj for obj in objs if "invocation-" in obj.key]

    descriptor = getJSON(tmpdir, descriptor_obj, buck, cli, descriptor=True)[0]
    invocation = getJSON(tmpdir, invocation_obj, buck, cli, invocation=True)
    tasks = getJSON(tmpdir, task_objs, buck, cli, task=True,
                    data={"descriptor": descriptor, "invocation": invocation})
    # dbuck, dkey = re.match('^s3:\/\/([\w\-\_]+)/([\w\-\_\/\.]+)',
    #                        task["contents"]["tool"]).group(1, 2)
    # task["contents"]["tool"] = cli.generate_presigned_url('get_object',
    #                                                       Params={'Bucket': dbuck,
    #                                                               'Key': dkey},
    #                                                       ExpiresIn=None)
    # ibuck, ikey = re.match('^s3:\/\/([\w\-\_]+)/([\w\-\_\/\.]+)',
    #                        task["contents"]["invocation"]).group(1, 2)
    # task["contents"]["invocation"] = cli.generate_presigned_url('get_object',
    #                                                             Params={'Bucket': ibuck,
    #                                                                     'Key': ikey},
    #                                                             ExpiresIn=None)
    # tasks.append(task)

    app.config["data"] = {"bucket": bucket,
                          "offset": offset,
                          "tasks" : tasks}
    app.run(host='0.0.0.0', debug=True)


if __name__ == "__main__":
    main()

