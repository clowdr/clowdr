#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/driver.py
# Created by Greg Kiar on 2018-02-28.
# Email: gkiar@mcin.ca

from argparse import ArgumentParser
import os.path as op
import tempfile
import json
import sys
import os

from clowdr.controller import metadata, launcher
# from clowdr.endpoint import aws, kubernetes, cbrain
from clowdr.task import processTask
from clowdr import utils


def local(tool, invocation, clowdrloc, dataloc, **kwargs):
    """local
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
        Path for accessing input data. If local, provide the hostname and
        optionally a path. If on S3, provide an S3 path.
    **kwargs : dict
        Arbitrary keyword arguments. Currently supported arguments:
        - verbose : bool
            Toggle verbose output printing
        - dev : bool
            Toggle dev mode (only runs first execution in the specified set)

        Additionally, transfers all keyword arguments accepted by "processTask"

    Returns
    -------
    int
        The exit-code returned by the task being executed
    """
    # TODO: scrub inputs
    [tasks, invocs] = metadata.consolidateTask(tool, invocation, clowdrloc,
                                               dataloc, **kwargs)
    if kwargs.get("dev"):
        tasks = [tasks[0]]  # Just launch the first task in dev

    taskdir = op.dirname(utils.truepath(tasks[0]))
    os.chdir(taskdir)
    for task in tasks:
        processTask(task, taskdir, local=True, **kwargs)

    print(taskdir)
    return taskdir


def cluster(tool, invocation, clowdrloc, dataloc, cluster, **kwargs):
    """cluster
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
        Path for accessing input data. If local, provide the hostname and
        optionally a path. If on S3, provide an S3 path.
    cluster : str
        Scheduler on the cluster being used. Currently, the only supported mode
        is slurm.
    **kwargs : dict
        Arbitrary keyword arguments. Currently supported arguments:
        - account : str
            Account for the cluster scheduler
        - jobname : str
            Base-name for the jobs as they will appear in the scheduler
        - verbose : bool
            Toggle verbose output printing
        - dev : bool
            Toggle dev mode (only runs first execution in the specified set)

        Additionally, transfers all keyword arguments accepted by both of
        "controller.metadata.consolidateTask" and "task.processTask"

    Returns
    -------
    int
        The exit-code returned by the task being executed
    """
    # TODO: scrub inputs
    tool = utils.truepath(tool)


    from slurmpy import Slurm

    if kwargs.get("verbose"):
        print("Consolidating metadata...")
    [tasks, invocs] = metadata.consolidateTask(tool, invocation, clowdrloc,
                                               dataloc, **kwargs)
    if kwargs.get("dev"):
        tasks = [tasks[0]]  # Just launch the first task in dev

    taskdir = op.dirname(utils.truepath(tasks[0]))
    os.chdir(taskdir)

    with open(tool) as fhandle:
        container = json.load(fhandle).get("container-image")
    if container:
        if kwargs.get("verbose"):
            print("Getting conatainer...")
        outp = utils.getContainer(taskdir, container)
        if kwargs.get("verbose"):
            print("\n".join(elem.decode("utf-8") for elem in outp))

    jobname = kwargs.get("jobname") if kwargs.get("jobname") else "clowdrtask"
    job = Slurm(jobname, {"account": kwargs.get("account")})

    script = "clowdr run {} -c {} --local"
    if kwargs.get("workdir"):
        script += " -w {}".format(kwargs["workdir"])
    if kwargs.get("volumes"):
        script += " ".join([" -v {}".format(vol)
                            for vol in kwargs.get("volumes")])

    for task in tasks:
        job.run(script.format(task, taskdir))

    print(taskdir)
    return taskdir


def cloud(tool, invocation, clowdrloc, dataloc, endpoint, auth, **kwargs):
    """cloud
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

    taskdir = op.dirname(utils.truepath(tasks_remote[0]))
    print(taskdir)
    return taskdir, jids


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

    # Local Parser
    parser_loc = subparsers.add_parser("local")
    parser_loc.add_argument("tool", help="boutiques descriptor for a tool")
    parser_loc.add_argument("invocation", help="input(s) for the tool")
    parser_loc.add_argument("clowdrloc", help="location locally for clowdr")
    parser_loc.add_argument("dataloc", help="location locally or s3 for data")
    parser_loc.add_argument("--workdir", "-w", action="store")
    parser_loc.add_argument("--volumes", "-v", action="append")

    parser_loc.add_argument("--verbose", "-V", action="store_true")
    parser_loc.add_argument("--bids", "-b", action="store_true")
    parser_loc.add_argument("--dev", "-d", action="store_true")

    parser_loc.set_defaults(func=local)

    # Cluster Parser
    parser_cls = subparsers.add_parser("cluster")
    parser_cls.add_argument("tool", help="boutiques descriptor for a tool")
    parser_cls.add_argument("invocation", help="input(s) for the tool")
    parser_cls.add_argument("clowdrloc", help="location locally for clowdr")
    parser_cls.add_argument("dataloc", help="location locally or s3 for data")
    parser_cls.add_argument("cluster", help="cluster type", choices=["slurm"])
    parser_cls.add_argument("--jobname", "-n", action="store")
    parser_cls.add_argument("--account", "-a", action="store")
    parser_cls.add_argument("--workdir", "-w", action="store")
    parser_cls.add_argument("--volumes", "-v", action="append")

    parser_cls.add_argument("--verbose", "-V", action="store_true")
    parser_cls.add_argument("--bids", "-b", action="store_true")
    parser_cls.add_argument("--dev", "-d", action="store_true")

    parser_cls.set_defaults(func=cluster)

    # Cloud Parser
    parser_cld = subparsers.add_parser("cloud")
    parser_cld.add_argument("tool",  help="boutiques descriptor for a tool")
    parser_cld.add_argument("invocation", help="input(s) for the tool")
    parser_cld.add_argument("clowdrloc", help="location on s3 for clowdr")
    parser_cld.add_argument("dataloc", help="location on s3 of data")
    parser_cld.add_argument("cloud", help="cloud endpoint", choices=["aws"])
    parser_cld.add_argument("auth", help="credentials for the remote resource")
    parser_cld.add_argument("--region", "-r", action="store")

    parser_cld.add_argument("--verbose", "-V", action="store_true")
    parser_cld.add_argument("--bids", "-b", action="store_true")
    parser_cld.add_argument("--dev", "-d", action="store_true")

    parser_cld.set_defaults(func=cloud)

    # Share Parser
    parser_shr = subparsers.add_parser("share")
    parser_shr.add_argument("location", help="local or s3 location for clowdr")
    parser_shr.set_defaults(func=share)

    # Run Parser
    parser_run = subparsers.add_parser("run")
    parser_run.add_argument("metadata", help="task metadata file")
    parser_run.add_argument("--clowdrloc", "-c", action="store",
                            help="task output directory")
    parser_run.add_argument("--local", "-l", action="store_true")
    parser_run.add_argument("--workdir", "-w", action="store")
    parser_run.add_argument("--volumes", "-v", action="append")

    parser_run.add_argument("--verbose", "-V", action="store_true")

    parser_run.set_defaults(func=processTask)

    # Parse arguments
    inps = parser.parse_args(args) if args is not None else parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    else:
        inps.func(**vars(inps))


if __name__ == "__main__":
    main()

