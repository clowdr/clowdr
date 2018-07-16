#!/usr/bin/env python

from unittest import TestCase
from contextlib import redirect_stdout
import os.path as op
import json
import re

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
        groups = 1
        call_local = self.provide_local_call(groups=groups)
        fname = op.join(op.dirname(__file__), 'test_stdout.txt')

        with open(fname, 'w') as f:
            with redirect_stdout(f):
                status = driver.main(args=call_local)

        stdout = []
        with open(fname, 'r') as f:
            for line in f:
                stdout += [line.strip('\n')]

        print("\n".join(stdout))
        self.assertFalse(status)
        r = re.compile(".+ Processing task: (.+)")

        tasknames = [r.match(o) for o in stdout if r.match(o) is not None][0]
        tasks = tasknames.group(1).split(", ")

        print(tasks)
        self.assertTrue(len(tasks) == groups)

        task = tasks[0]
        taskmeta = task.strip('.json') + '-summary.json'
        with open(taskmeta, 'r') as f:
            taskmetadata = json.load(f)

        self.assertFalse(taskmetadata["exitcode"])
