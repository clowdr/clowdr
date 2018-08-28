#!/usr/bin/env python

from shutil import copy, copytree, rmtree, SameFileError
from subprocess import Popen, PIPE, CalledProcessError
import os.path as op
import random as rnd
import string
import boto3
import time
import csv
import sys
import os
import re


def backoff(function, posargs, optargs, backoff_time=36000, **kwargs):
    fib_lo = 0
    fib_hi = 1
    while True:
        try:
            value = function(*posargs, **optargs)
            return (0, value)
        except Exception as e:
            if kwargs.get("verbose"):
                print("Failed ({}). Retrying in: {}s".format(type(e).__name__,
                                                             fib_hi))
            if fib_hi > backoff_time:
                if kwargs.get("verbose"):
                    print("Failed. Skipping!")
                return (-1, str(e))
            time.sleep(fib_hi)
            fib_lo, fib_hi = fib_hi, fib_lo + fib_hi


def getContainer(savedir, container, **kwargs):
    if container["type"] == "singularity":
        name = container.get("image")
        local = name.replace("/", "-").replace(":", "-")
        index = container.get("index")
        if not index:
            index = "shub://"
        elif not index.endswith("://"):
            index = index + "://"
        if kwargs.get("simg"):
            return get(kwargs["simg"], local + ".simg")
        else:
            cmd = "singularity pull --name \"{}.simg\" {}{}".format(local,
                                                                    index,
                                                                    name)
            if kwargs.get("verbose"):
                print(cmd)
            p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
            stdout = p.communicate()
            if kwargs.get("verbose"):
                try:
                    print(stdout.decode('utf-8'))
                except Exception as e:
                    print(stdout)
            return stdout


def truepath(path):
    if path.startswith("s3://"):
        return path
    else:
        return op.realpath(path)


def randstring(k):
    return "".join([rnd.choice(string.ascii_uppercase + string.digits)
                    for _ in range(k)])


def splitS3Path(path):
    return re.match('^s3://([a-zA-Z0-9_-]+)/([a-zA-Z0-9_/.-]+)',
                    path).group(1, 2)


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


def remove(local):
    try:
        if op.isfile(local):
            os.remove(local)
        elif op.isdir(local):
            rmtree(local)
    except FileNotFoundError as e:
        pass


def _awsget(remote, local):
    s3 = boto3.resource("s3")

    bucket, rpath = splitS3Path(remote)

    buck = s3.Bucket(bucket)
    files = [obj.key
             for obj in buck.objects.filter(Prefix=rpath)
             if not os.path.isdir(obj.key)]
    files_local = []
    for fl in files:
        fl_local = op.join(local, fl)
        files_local += [fl_local]
        os.makedirs(op.dirname(fl_local), exist_ok=True)
        if fl_local.strip('/') == op.dirname(fl_local).strip('/'):
            continue  # Create, but don't try to download directories
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
