#!/usr/bin/env python

from argparse import ArgumentParser
import sys

import clowdr.task as task
from clowdr.controller import metadata  # , launcher, sendMetadata, launchTask


def dev(tool, invocation, clowdrloc, dataloc, **kwargs):
    """dev
    Launches a pipeline locally through the Clowdr wrappers.

    Parameters
    ----------
    tool : str
        Path to a boutiques descriptor for the tool to be run
    invocation : str
        Path to a boutiques invocation for the tool and parameters to be run
    clowdrloc : str
        Path for storing Clowdr intermediate files and outputs
    dataloc : str
        Path for accessing input data
    **kwargs : dict
        Arbitrary keyword arguments (i.e. {'verbose': True})

    Returns
    -------
    int
        The exit-code returned by the task being executed
    """
    # TODO: scrub inputs
    task = metadata.consolidate(tool, invocation, clowdrloc, dataloc)
    if len(task) > 1: task = task[0]  # Just launch the first task in dev mode
    code = process_task(task)
    return code


def deploy(tool, invocation, location, auth, **kwargs):
    # TODO: scrub inputs
    # tasks_local  = metadata.consolidate(tool, invocation, location)
    # tasks_remote = metadata.upload(tasks_remote, auth)

    # launcher.submit(resource, auth, tasks_remote)
    print(tool, invocation, location, auth, kwargs)
    return 0


def share(location, **kwargs):
    # TODO: scrub inputs
    print(location, kwargs)
    return 0


def main(args=None):
    desc = "Interface for launching Boutiques task locally and in the cloud"
    parser = ArgumentParser("Clowdr CLI", description=desc)
    parser.add_argument("--verbose", "-v", action="store_true")
    subparsers = parser.add_subparsers(help="Modes of operation", dest="mode")

    parser_dev = subparsers.add_parser("dev")
    parser_dev.add_argument("tool", help="boutiques descriptor for a tool")
    parser_dev.add_argument("invocation", help="input(s) for the tool")
    parser_dev.add_argument("clowdrloc", help="local output location")
    parser_dev.add_argument("dataloc", help="local or S3 input data location")

    parser_dpy = subparsers.add_parser("deploy")
    parser_dpy.add_argument("tool",  help="boutiques descriptor for a tool")
    parser_dpy.add_argument("invocation", help="input(s) for the tool")
    parser_dpy.add_argument("location", help="local or s3 location for clowdr")
    parser_dpy.add_argument("auth", help="credentials for the remote resource")

    parser_shr = subparsers.add_parser("share")
    parser_shr.add_argument("location", help="local or s3 location for clowdr")

    inps = parser.parse_args(args) if args is not None else parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    mode = inps.mode
    del inps.mode

    if mode == "dev":
        dev(**vars(inps))
    elif mode == "deploy":
        deploy(**vars(inps))
    elif mode == "share":
        share(**vars(inps))


if __name__ == "__main__":
    main()

