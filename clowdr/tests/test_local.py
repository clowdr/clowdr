#!/usr/bin/env python

from unittest import TestCase
from contextlib import redirect_stdout
import os.path as op
import subprocess
import pytest
import json
import re
import os

from clowdr import __file__ as cfile
from clowdr import driver


class TestLocal(TestCase):

    cdir = op.abspath(op.join(op.dirname(cfile), op.pardir))

    def provide_local_call(self, groups=3, container="d"):
        call_local = ["local",
                      op.join(self.cdir,
                              "examples/descriptor_{}.json".format(container)),
                      op.join(self.cdir, "examples/invocation.json"),
                      op.join(self.cdir, "examples/task/"),
                      "-v", "/data/ds114/:/data/ds114",
                      "-bdV",
                      "-g", "{}".format(groups)]
        return call_local

    def evaluate_output(self, fname, call_local, groups):
        with open(fname, 'w') as f:
            with redirect_stdout(f):
                status = driver.main(args=call_local)

        stdout = []
        with open(fname, 'r') as f:
            for line in f:
                stdout += [line.strip('\n')]

        print("\n".join(stdout))
        self.assertFalse(status)
        r = re.compile(".+ Processing task(s): (.+)")

        tasknames = [r.match(o) for o in stdout if r.match(o) is not None][0]
        tasks = tasknames.group(1).split(", ")

        print(tasks)
        self.assertTrue(len(tasks) == groups)

        task = tasks[0]
        taskmeta = task.strip('.json') + '-summary.json'
        with open(taskmeta, 'r') as f:
            taskmetadata = json.load(f)
        self.assertFalse(taskmetadata["exitcode"])

    @pytest.mark.skipif(subprocess.Popen("type docker", shell=True).wait(),
                        reason="Docker not installed")
    def test_local_via_cli_docker(self):
        os.chdir(self.cdir)
        groups = 3
        call_local = self.provide_local_call(groups=groups)
        fname = op.join(op.dirname(__file__), 'test_stdout_docker.txt')
        self.evaluate_output(fname, call_local, groups)

    @pytest.mark.skipif(subprocess.Popen("type singularity", shell=True).wait(),
                        reason="Singularity not installed")
    def test_local_via_cli_singularity(self):
        os.chdir(self.cdir)
        groups = 1
        call_local = self.provide_local_call(groups=groups, container="s")
        fname = op.join(op.dirname(__file__), 'test_stdout_singularity1.txt')
        self.evaluate_output(fname, call_local, groups)

        call_local += ["--simg", op.join(self.cdir,
                                         "examples/bids-example.simg")]
        fname = op.join(op.dirname(__file__), 'test_stdout_singularity2.txt')
        self.evaluate_output(fname, call_local, groups)
