#!/usr/bin/env python

from shutil import copy, copytree, SameFileError
import os.path as op
import string
import random as rnd
import boto3
import csv
import os
import re


def truepath(path):
    if path.startswith("s3://"):
        return path
    else:
        return op.realpath(path)


def randstring(k):
    return "".join(rnd.choices(string.ascii_uppercase + string.digits, k=k))


def splitS3Path(path):
    return re.match('^s3:\/\/([\w\-\_]+)/([\w\-\_\/\.]+)', path).group(1, 2)


def get(remote, local, **kwargs):
    try:
        if remote.startswith("s3://"):
            return _awsget(remote, local)
        elif op.isdir(remote):
            return [op.realpath(copytree(remote, local))]
        else:
            return [op.realpath(copy(remote, local))]
    except SameFileError as e:
        if kwargs.get("verbose"):
            print("SameFileWarning: some files may not have been moved")
        if op.isdir(local) and op.isfile(remote):
            return [op.realpath(op.join(local, op.basename(remote)))]
        else:
            return [op.realpath(local)] 
    except FileExistsError as e:
        if kwargs.get("verbose"):
            print("FileExistsWarning: some files may not have been moved")
        if op.isdir(local) and op.isfile(remote):
            return [op.realpath(op.join(local, op.basename(remote)))]
        else:
            return [op.realpath(local)] 


def post(local, remote, **kwargs):
    try:
        if remote.startswith("s3://"):
            return _awspost(local, remote)
        elif op.isdir(local):
            return [op.realpath(copytree(local, remote))]
        elif op.isdir(remote):
            return [op.realpath(copy(local,
                                     op.join(remote, op.basename(local))))]
        else:
            return [op.realpath(copy(local, remote))]
    except SameFileError as e:
        if kwargs.get("verbose"):
            print("SameFileWarning: some files may not have been moved")
        if op.isdir(remote) and op.isfile(local):
            return [op.realpath(op.join(remote, op.basename(local)))]
        else:
            return [op.realpath(remote)] 


def _awsget(remote, local):
    s3 = boto3.resource("s3")

    bucket, rpath = splitS3Path(remote)

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


def _awspost(local, remote):
    # Credit: https://github.com/boto/boto3/issues/358#issuecomment-346093506
    if not op.isfile(local):
        local_files = [op.join(root, f)
                       for root, dirs, files in os.walk(local)
                       for f in files]
    else:
        local_files = [local]

    s3 = boto3.client("s3")
    bucket, rpath = splitS3Path(remote)

    rempats = []
    for flocal in local_files:
        if local == flocal:
            rempat = op.join(rpath, op.basename(flocal))
        else:
            rempat = op.join(rpath, op.relpath(flocal, local))

        s3.upload_file(flocal, bucket, rempat, {'ACL': 'public-read'})
        rempats += [op.join('s3://', bucket, rempat)]

    return rempats
