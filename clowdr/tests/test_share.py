#!/usr/bin/env python

from unittest import TestCase
import os.path as op

from clowdr import __file__ as cfile
from clowdr import driver


class TestShare(TestCase):

    cdir = op.abspath(op.join(op.dirname(cfile), op.pardir))

    def test_simple_share(self):
        status = driver.main(args=["share", "-d", "-t",
                                   op.join(self.cdir,
                                           "examples/task/bids-example/clowdr")
                                  ])
