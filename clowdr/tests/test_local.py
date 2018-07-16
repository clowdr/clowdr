#!/usr/bin/env python

from unittest import TestCase
from subprocess import Popen, PIPE
from contextlib import redirect_stdout
import os.path as op
import sys
import re
import io

from clowdr import driver


class TestLocal(TestCase):

    def provide_local_call(self, groups=3):
        call_local = ["local",
                      "examples/descriptor_d.json",
                      "examples/invocation.json",
                      "examples/task/",
                      "-v", "/data/ds114/:/data/ds114",
                      "-bdV",
                      "-g {}".format(groups)]
        return call_local

    def test_local_via_cli(self):
        groups = 3
        call_local = self.provide_local_call(groups=groups)
        fname = op.join(op.dirname(__file__), 'test_stdout.txt')

        with open(fname, 'w') as f:
            with redirect_stdout(f):
                status = driver.main(args=call_local)

        stdout = []
        with open(fname, 'r') as f:
            for line in f:
                stdout += [line.strip('\n')]

        print(stdout)
        self.assertTrue(not status)
        r = re.compile("\.\.\. Processing task: \['(.+)'\]")

        task = [r.match(o) for o in stdout if r.match(o) is not None][0]
        tasks = task.group(1).split("', '")

        print(tasks)
        self.assertTrue(len(tasks) == groups)
