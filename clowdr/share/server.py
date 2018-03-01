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
                           files=app.config.get("files"))


def main():
    clowdrloc = sys.argv[1]
    tmpdir = tempfile.mkdtemp()

    s3 = boto3.resource("s3")
    bucket, offset = re.match('^s3:\/\/([\w\-\_]+)/([\w\-\_\/]+)',
                              clowdrloc).group(1, 2)
    buck = s3.Bucket(bucket)
    objs = buck.objects.filter(Prefix=offset)
    for obj in objs:
        key = obj.key
        buck.download_file(key, Filename=op.join(tmpdir, op.basename(key)))
    files = os.listdir(tmpdir)
    
    app.config["data"] = {"bucket": bucket, "offset": offset, "files": files}
    app.run(host='0.0.0.0', debug=True)


if __name__ == "__main__":
    main()

