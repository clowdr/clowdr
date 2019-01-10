#!/usr/bin/env python

from unittest import TestCase
from contextlib import redirect_stdout
import os.path as op
import os

from clowdr import __file__ as cfile
from clowdr import driver
from clowdr.share import consolidate


class TestShare(TestCase):

    cdir = op.abspath(op.join(op.dirname(cfile), op.pardir))

    def provide_local_call(self, groups=3):
        call_local = ["local",
                      op.join(self.cdir, "examples/bids-example/"
                                         "descriptor_d.json"),
                      op.join(self.cdir, "examples/bids-example/"
                                         "invocation.json"),
                      op.join(self.cdir, "examples/bids-example/task/"),
                      "-v", "/data/ds114/:/data/ds114",
                      "-bdV",
                      "-g", "{}".format(groups)]
        return call_local

    def test_consolidate_generation(self):
        os.chdir(self.cdir)
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

        summary = op.join(provdir, 'clowdr-summary.json')
        experiment_dict = consolidate.summary(provdir, summary)
        assert(isinstance(experiment_dict, list))
        assert(isinstance(experiment_dict[0], dict))
