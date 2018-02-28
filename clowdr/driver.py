#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/driver.py
# Created by Greg Kiar on 2018-02-28.
# Email: gkiar@mcin.ca

from argparse import ArgumentParser
import tempfile
import sys

from clowdr.controller import metadata, launcher
# from clowdr.endpoint import aws, kubernetes, cbrain
from clowdr.task import processTask
from clowdr import utils


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
    [tasks, invocs] = metadata.consolidateTask(tool, invocation, clowdrloc,
                                               dataloc, **kwargs)
    if len(tasks) > 1: tasks = tasks[0]  # Just launch the first task in dev
    code = processTask(tasks, clowdrloc)
    return code


def deploy(tool, invocation, clowdrloc, dataloc, endpoint, auth, **kwargs):
    """deploy
    Launches a pipeline locally at scale through Clowdr.

    Parameters
    ----------
    tool : str
        Path to a boutiques descriptor for the tool to be run
    invocation : str
        Path to a boutiques invocation for the tool and parameters to be run
    clowdrloc : str
        Path on S3 for storing Clowdr intermediate files and outputs
    dataloc : str
        Path on S3 for accessing input data
    endpoint : str
        Which endpoint to use for deployment
    auth : str
        Credentials for Amazon with access to dataloc, clowdrloc, and Batch
    **kwargs : dict
        Arbitrary keyword arguments (i.e. {'verbose': True})

    Returns
    -------
    int
        The exit-code returned by the task being executed
    """
    # TODO: scrub inputs better
    clowdrloc = clowdrloc.strip('/')

    # Create temp dir for clowdrloc 
    tmploc = utils.truepath(tempfile.mkdtemp())

    [tasks, invocs] = metadata.consolidateTask(tool, invocation, tmploc,
                                               dataloc, **kwargs)
    metadata.prepareForRemote(tasks, tmploc, clowdrloc)
    tasks_remote = [task for task in utils.post(tmploc, clowdrloc)
                    if "task-" in task]

    resource = launcher.configureResource(endpoint, auth, **kwargs)
    jids = []
    for task in tasks_remote:
        jids += [resource.launchJob(task)]

    return (tasks_remote, jids)


def share(clowdrloc, **kwargs):
    """share
    Launches a simple web server which showcases all runs at the clowdrloc.

    Parameters
    ----------
    clowdrloc : str
        Path with Clowdr intermediate files and outputs
    **kwargs : dict
        Arbitrary keyword arguments (i.e. {'verbose': True})

    Returns
    -------
    None
    """
    # TODO: scrub inputs
    print(clowdrloc, kwargs)
    return 0


def main(args=None):
    """main
    Command-line API wrapper for Clowdr as a CLI, not Python API.

    Parameters
    ----------
    args: list
        List of all command-line arguments being passed

    Returns
    -------
    int
        The exit-code returned by the task being executed
    """
    desc = "Interface for launching Boutiques task locally and in the cloud"
    parser = ArgumentParser("Clowdr CLI", description=desc)
    subparsers = parser.add_subparsers(help="Modes of operation", dest="mode")

    parser_dev = subparsers.add_parser("dev")
    parser_dev.add_argument("tool", help="boutiques descriptor for a tool")
    parser_dev.add_argument("invocation", help="input(s) for the tool")
    parser_dev.add_argument("clowdrloc", help="location locally for clowdr")
    parser_dev.add_argument("dataloc", help="location locally or s3 of data")
    parser_dev.add_argument("--verbose", "-v", action="store_true")
    parser_dev.add_argument("--bids", "-b", action="store_true")
    parser_dev.set_defaults(func=dev)

    parser_dpy = subparsers.add_parser("deploy")
    parser_dpy.add_argument("tool",  help="boutiques descriptor for a tool")
    parser_dpy.add_argument("invocation", help="input(s) for the tool")
    parser_dpy.add_argument("clowdrloc", help="location on s3 for clowdr")
    parser_dpy.add_argument("dataloc", help="location on s3 of data")
    parser_dpy.add_argument("endpoint", help="cloud endpoint", choices=["aws"])
    parser_dpy.add_argument("auth", help="credentials for the remote resource")
    parser_dpy.add_argument("--verbose", "-v", action="store_true")
    parser_dpy.add_argument("--region", "-r", action="store")
    parser_dpy.add_argument("--bids", "-b", action="store_true")
    parser_dpy.set_defaults(func=deploy)

    parser_shr = subparsers.add_parser("share")
    parser_shr.add_argument("location", help="local or s3 location for clowdr")
    parser_shr.set_defaults(func=share)

    parser_run = subparsers.add_parser("run")
    parser_run.add_argument("metadata", help="task metadata file")
    parser_run.add_argument("--clowdrloc", "-l", action="store",
                            help="task output directory")
    parser_run.set_defaults(func=processTask)

    inps = parser.parse_args(args) if args is not None else parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    else:
        inps.func(**vars(inps))


if __name__ == "__main__":
    main()

