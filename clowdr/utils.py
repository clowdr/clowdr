#!/usr/bin/env python

from shutil import copy, copytree
import boto3
import os
import os.path as op


def get(remote, local):
    if remote.startswith("s3://"):
        return aws_get(remote, local)
    elif op.isdir(remote):
        copytree(remote, local)
    else:
        copy(remote, local)


def post(local, remote):
    if "s3://" in remote:
        return aws_post(local, remote)
    elif op.isdir(local):
        copytree(local, remote)
    else:
        copy(local, remote)


def aws_get(remote, local):
    s3 = boto3.resource("s3")

    bucket, rpath = remote.split('/')[2], remote.split('/')[3:]
    rpath = "/".join(rpath)

    buck = s3.Bucket(bucket)
    files = [obj.key for obj in buck.objects.filter(Prefix=rpath) if not os.path.isdir(obj.key)]
    files_local = []
    for fl in files:
        fl_local = op.join(local, fl)
        files_local += [fl_local]
        os.makedirs(op.dirname(fl_local), exist_ok=True)
        if fl_local.strip('/') == op.dirname(fl_local).strip('/'):
            continue;  # Create, but don't try to download directories
        buck.download_file(fl, fl_local)

    return files_local


def aws_post(local, remote):
    # Credit: https://github.com/boto/boto3/issues/358#issuecomment-346093506
    if not op.isfile(local):
        local_files = [op.join(root, f)
                       for root, dirs, files in os.walk(local)
                       for f in files]
    else:
        local_files = [local]

    s3 = boto3.client("s3")
    bucket, rpath = remote.split('/')[2], remote.split('/')[3:]
    rpath = "/".join(rpath)

    for flocal in local_files:
        rempat = rpath if local == flocal else op.join(rpath,
                                                       op.relpath(flocal,
                                                                  local))
        s3.upload_file(flocal, bucket, rempat, {'ACL': 'public-read'})
