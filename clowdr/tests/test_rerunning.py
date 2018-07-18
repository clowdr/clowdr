#!/usr/bin/env python

from unittest import TestCase
from contextlib import redirect_stdout
import os.path as op
import json
import re

from clowdr import __file__ as cfile
from clowdr import driver


class TestRerunner(TestCase):

    cdir = op.abspath(op.join(op.dirname(cfile), op.pardir))

    def provide_local_call(self, groups=3):
        call_local = ["local",
                      op.join(self.cdir, "examples/descriptor_d.json"),
                      op.join(self.cdir, "examples/invocation.json"),
                      op.join(self.cdir, "examples/task/"),
                      "-v", "/data/ds114/:/data/ds114",
                      "-bdV",
                      "-g", "{}".format(groups)]
        return call_local

    def test_rerun_all(self):
        os.chdir(cdir)
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

        provdir = stdout[-1]
        print(provdir)

        call_local += ["--run_id", provdir.split('/')[-2], "-R", "placeholder"]
        for remode in ["all", "failed", "incomplete"]:
            # TODO: for all, verify task length is same length as original
            # TODO: for failed, verify task length is same length as failed
            # TODO: for incomplete, verify .......
            call_local[-1] = remode
            with open(fname, 'w') as f:
                with redirect_stdout(f):
                    status = driver.main(args=call_local)
