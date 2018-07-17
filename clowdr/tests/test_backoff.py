#!/usr/bin/env python

from unittest import TestCase
from clowdr.utils import backoff
from subprocess import CalledProcessError


class TestBackoff(TestCase):

    def myfunction(self, num1, num2, flag=True):
        mystr = "Num1: {}, Num2: {}, Flag: {}".format(num1, num2, flag)
        if flag:
            raise CalledProcessError(42, 'someExecution')
        return mystr

    def test_backoff_to_failure(self):
        num1, num2, flag = 1, 5, True
        code, value = backoff(self.myfunction, [num1, num2], {'flag': flag},
                              verbose=True, backoff_time=25)

        self.assertTrue(code == -1)
        err = "Command 'someExecution' returned non-zero exit status 42."
        print(value)
        print(err)
        self.assertTrue(value == err)

    def test_no_backoff(self):
        num1, num2, flag = 1, 5, False
        code, value = backoff(self.myfunction, [num1, num2], {'flag': flag},
                              verbose=True, backoff_time=2)
        self.assertTrue(code == 0)

        expected = self.myfunction(num1, num2, flag=flag)
        self.assertTrue(value == expected)
