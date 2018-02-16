#!/usr/bin/env python

from argparse import ArgumentParser
import sys

from clowdr.controller import metadata  # , launcher, sendMetadata, launchTask
# from clowdr.endpoint import local, aws, kubernetes, azure, etc.
from clowdr.task import process_task


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
    task = metadata.consolidate(tool, invocation, clowdrloc, dataloc, **kwargs)
    if len(task) > 1: task = task[0]  # Just launch the first task in dev mode
    code = process_task(task, clowdrloc)
    return code


def deploy(tool, invocation, location, auth, **kwargs):
    # TODO: document
    # TODO: scrub inputs
    # tasks_local  = metadata.consolidate(tool, invocation, location)
    # tasks_remote = metadata.upload(tasks_remote, auth)

    # launcher.submit(resource, auth, tasks_remote)
    print(tool, invocation, location, auth, kwargs)
    return 0


def share(location, **kwargs):
    # TODO: document
    # TODO: scrub inputs
    print(location, kwargs)
    return 0


def main(args=None):
    desc = "Interface for launching Boutiques task locally and in the cloud"
    parser = ArgumentParser("Clowdr CLI", description=desc)
    subparsers = parser.add_subparsers(help="Modes of operation", dest="mode")

    parser_dev = subparsers.add_parser("dev")
    parser_dev.add_argument("tool", help="boutiques descriptor for a tool")
    parser_dev.add_argument("invocation", help="input(s) for the tool")
    parser_dev.add_argument("clowdrloc", help="local output location")
    parser_dev.add_argument("dataloc", help="local or S3 input data location")
    parser_dev.add_argument("--verbose", "-v", action="store_true")
    parser_dev.add_argument("--bids", "-b", action="store_true")
    parser_dev.set_defaults(func=dev)

    parser_dpy = subparsers.add_parser("deploy")
    parser_dpy.add_argument("tool",  help="boutiques descriptor for a tool")
    parser_dpy.add_argument("invocation", help="input(s) for the tool")
    parser_dpy.add_argument("location", help="local or s3 location for clowdr")
    parser_dpy.add_argument("auth", help="credentials for the remote resource")
    parser_dpy.add_argument("--verbose", "-v", action="store_true")
    parser_dpy.add_argument("--bids", "-b", action="store_true")
    parser_dpy.set_defaults(func=deploy)

    parser_shr = subparsers.add_parser("share")
    parser_shr.add_argument("location", help="local or s3 location for clowdr")
    parser_shr.set_defaults(func=share)

    parser_shr = subparsers.add_parser("run")
    parser_shr.add_argument("metadata", help="task metadata file")
    parser_shr.add_argument("--clowdrloc", "-l", action="store",
                            help="task output directory")
    parser_shr.set_defaults(func=process_task)

    inps = parser.parse_args(args) if args is not None else parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    else:
        inps.func(**vars(inps))


if __name__ == "__main__":
    main()

