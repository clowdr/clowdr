#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/share/server.py
# Created by Greg Kiar on 2018-03-01.
# Email: gkiar@mcin.ca

from flask import Flask
import os.path as op
import tempfile
import boto3
import sys
import re
import os

app = Flask(__name__)


@app.route("/clowdr/")
def index():
    return """
    <html>
    <body>
        <h1>Test site running under Flask</h1>
        <p>Bucket: {0}</p>
        <p>Offset: {1}</p>
        <p>Files: {2}</p>
    </body>
    </html>
    """.format(app.config.get("bucket"),
               app.config.get("offset"),
               app.config.get("files"))

def main():
    clowdrloc = sys.argv[1]
    tmpdir = tempfile.mkdtemp()

    s3 = boto3.resource("s3")
    bucket, offset = re.match('^s3:\/\/([\w\-\_]+)/([\w\-\_\/]+)',
                              clowdrloc).group(1, 2)
    buck = s3.Bucket(bucket)
    objs = buck.objects.filter(Prefix=offset)
    for obj in objs:
        buck.download_file(obj.key,
                           Filename=op.join(tmpdir, op.basename(obj.key)))
    files = os.listdir(tmpdir)
    
    app.config["bucket"] = bucket
    app.config["offset"] = offset
    app.config["files"] = files
    app.run(host='0.0.0.0', debug=True)



if __name__ == "__main__":
    main()

