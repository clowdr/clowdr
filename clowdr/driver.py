#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/driver.py
# Created by Greg Kiar on 2018-02-28.
# Email: gkiar@mcin.ca

from argparse import ArgumentParser, RawTextHelpFormatter
import argparse
import os.path as op
import tempfile
import json
import sys
import os

from clowdr.controller import metadata, launcher
from clowdr.task import TaskHandler
from clowdr.server import shareapp, updateIndex
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

        Additionally, transfers all keyword arguments accepted by the "TaskHandler"

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
        run(task, clowdrloc=taskdir, local=True, **kwargs)

    if kwargs.get("verbose"):
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
        "controller.metadata.consolidateTask" and "task.TaskHandler"

    Returns
    -------
    int
        The exit-code returned by the task being executed
    """
    # TODO: scrub inputs
    tool = utils.truepath(tool)
    if kwargs.get("simg"):
        kwargs["simg"] = utils.truepath(kwargs["simg"])


    from slurmpy import Slurm

    if kwargs.get("verbose"):
        print("Consolidating metadata...")
    [tasks, invocs] = metadata.consolidateTask(tool, invocation, clowdrloc,
                                               dataloc, **kwargs)
    if kwargs.get("dev"):
        tasks = [tasks[0]]  # Just launch the first task in dev

    taskdir = op.dirname(utils.truepath(tasks[0]))
    try:
        os.mkdir(taskdir)
    except FileExistsError:
        pass
    os.chdir(taskdir)

    with open(tool) as fhandle:
        container = json.load(fhandle).get("container-image")
    if container:
        if kwargs.get("verbose"):
            print("Getting container...")
        outp = utils.getContainer(taskdir, container, **kwargs)

    jobname = kwargs.get("jobname") if kwargs.get("jobname") else "clowdrtask"
    slurm_args = {}
    if kwargs.get("slurm_args"):
        for opt in kwargs.get("slurm_args").split(","):
            k, v = opt.split(":")[0], opt.split(":")[1:]
            v = ":".join(v)
            slurm_args[k] = v
    job = Slurm(jobname, slurm_args)

    script = "clowdr task {} -c {} --local"
    if kwargs.get("workdir"):
        script += " -w {}".format(kwargs["workdir"])
    if kwargs.get("volumes"):
        script += " ".join([" -v {}".format(vol)
                            for vol in kwargs.get("volumes")])

    for task in tasks:
        job.run(script.format(task, taskdir))

    if kwargs.get("verbose"):
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


def run(metadata, **kwargs):
    handler = TaskHandler(metadata, **kwargs)


def share(clowdrloc, **kwargs):
    """share
    Launches a simple web server which showcases all runs at the clowdrloc.

    Parameters
    ----------
    clowdrloc : str
        Path with Clowdr metdata files (returned from "local" and "deploy")
    **kwargs : dict
        Arbitrary keyword arguments (i.e. {'verbose': True})

    Returns
    -------
    None
    """
    # TODO: scrub inputs
    shareapp.config["clowdrloc"] = clowdrloc
    shareapp.config["tmpdir"] = tempfile.mkdtemp()

    updateIndex()

    host = kwargs["host"] if kwargs.get("host") else "0.0.0.0"
    shareapp.run(host=host, debug=kwargs.get("debug"))


def main(args=None):
    """main
    Command-line API wrapper for Clowdr as a CLI, not Python API.
    For information about the command-line wrapper and arguments it accepts,
    please try running "clowdr --help".

    Parameters
    ----------
    args: list
        List of all command-line arguments being passed.

    Returns
    -------
    int
        The exit-code returned by the driver.
    """

    # Create an outer argparser which can wrap subparsers for each function.
    desc = """
Scalable deployment and provenance-rich wrapper for Boutiques tools locally,
on clusters, and in the cloud. For more information, go to our website:

     https://github.com/clowdr/clowdr.
"""
    parser = ArgumentParser("clowdr", description=desc,
                            formatter_class=RawTextHelpFormatter)

    htext = """Clowdr has several distinct modes of operation:
  - local:  This mode allows you to develop your Clowdr execution, deploy
            analyses on your local system, and deploy them on clusters.
  - cloud:  This mode allows you to deploy your Clowdr exectuion on a cloud
            resource. Currently, this only supports Amazon Web Services.
  - share:  This mode launches a lightweight webserver for you to explore your
            executions, monitor job progress, and share your results.
  - task:   This mode is generally only for super-users. It is used by Clowdr
            to launch your tasks and record provenance information from them
            without you needing to call this option yourself. It can be useful
            when debugging or re-running failed executions.
"""
    subparsers = parser.add_subparsers(dest="mode",
                                       help=htext)

    # Create the subparser for local/cluster execution.
    desc = ("Manages local and cluster deployment. Ideal for development, "
            "testing, executing on local resources, or deployment on a "
            "computing cluster environment.")
    parser_loc = subparsers.add_parser("local", description=desc)
    parser_loc.add_argument("descriptor", type=argparse.FileType('r'),
                            help="Local path to Boutiques descriptor for the "
                                 "tool you wish to run. To learn about "
                                 "descriptors and Boutiques, go to: "
                                 "https://boutiques.github.io.")
    parser_loc.add_argument("invocation",
                            help="Local path to Boutiques invocation (or "
                                 "directory containing multiple invocations) "
                                 "for the analysis you wish to run. To learn "
                                 "about invocations and Boutiques, go to: "
                                 "https://boutiques.github.io.")
    parser_loc.add_argument("provdir",
                            help="Local directory for Clowdr provenance records"
                                 " and other captured metadata to be stored. "
                                 "This directory needs to exist prior to "
                                 "running Clowdr.")

    parser_loc.add_argument("--verbose", "-V", action="store_true",
                            help="Toggles verbose output statements.")
    parser_loc.add_argument("--dev", "-d", action="store_true",
                            help="Launches only the first created task. This "
                                 "is intended for development purposes.")
    parser_loc.add_argument("--workdir", "-w", action="store",
                            help="Specifies the working directory to be used "
                                 "by the tasks created.")
    parser_loc.add_argument("--volumes", "-v", action="append",
                            help="Specifies any volumes to be mounted to the "
                                 "container. This is usually related to the "
                                 "path of any data files as specified in your "
                                 "invocation(s).")
    parser_loc.add_argument("--cluster", "-c", choices=["slurm"],
                            help="If you wish to submit your local tasks to a "
                                 "scheduler, you must specify it here. "
                                 "Currently this only supports SLURM clusters.")
    parser_loc.add_argument("--clusterargs", "-a", action="store",
                            help="This allows users to supply arguments to the "
                                 "cluster, such as specifying RAM or requesting"
                                 " a certain amount of time on CPU. These are "
                                 "provided in the form of key:value pairs, and "
                                 "separated by commas. For example: "
                                 "--clusterargs time:4:00,mem:2048,account:ABC")
    parser_loc.add_argument("--jobname", "-n", action="store",
                            help="If running on a cluster, and you wish to "
                                 "specify a unique identifier to appear in the"
                                 "submitted tasks, you can specify it with "
                                 "this flag.")
    parser_loc.add_argument("--simg", "-s", action="store",
                            help="If the Boutiques descriptor summarizes a "
                                 "tool wrapped in Singularity, and the image "
                                 "has already been downloaded, this option "
                                 "allows you to specify that image file.")
    parser_loc.add_argument("--user", "-u", action="store_true",
                            help="If the Boutiques descriptor summarizes a "
                                 "tool wrapped in Docker, toggles propagating "
                                 "the current user within the container.")
    parser_loc.add_argument("--s3", action="store",
                            help="Amazon S3 bucket and path for remote data. "
                                 "Accepted in the format: s3://{bucket}/{path}")
    parser_loc.add_argument("--bids", "-b", action="store_true",
                            help="Indicates that the tool being launched is a "
                                 "BIDS app. BIDS is a data organization format"
                                 " in neuroimaging. For more information about"
                                 " this, go to https://bids.neuroimaging.io.")

    parser_loc.set_defaults(func=local)
    # parser_cls.set_defaults(func=cluster)

    # Create the subparser for cloud execution.
    desc = ("Manages local and cluster deployment. Ideal for development, "
            "testing, executing on local resources, or deployment on a "
            "computing cluster environment.")
    parser_cld = subparsers.add_parser("cloud", description=desc)
    parser_cld.add_argument("descriptor", type=argparse.FileType('r'),
                            help="Local path to Boutiques descriptor for the "
                                 "tool you wish to run. To learn about "
                                 "descriptors and Boutiques, go to: "
                                 "https://boutiques.github.io.")
    parser_cld.add_argument("invocation",
                            help="Local path to Boutiques invocation (or "
                                 "directory containing multiple invocations) "
                                 "for the analysis you wish to run. To learn "
                                 "about invocations and Boutiques, go to: "
                                 "https://boutiques.github.io.")
    parser_cld.add_argument("provdir",
                            help="Local directory for Clowdr provenance records"
                                 " and other captured metadata to be stored. "
                                 "This directory needs to exist prior to "
                                 "running Clowdr.")
    parser_cld.add_argument("s3",
                            help="Amazon S3 bucket and path for remote data. "
                                 "Accepted in the format: s3://{bucket}/{path}")
    parser_cld.add_argument("cloud", choices=["aws"],
                            help="cloud endpoint")
    parser_cld.add_argument("auth",
                            help="credentials for the remote resource")

    parser_cld.add_argument("--verbose", "-V", action="store_true",
                            help="Toggles verbose output statements.")
    parser_cld.add_argument("--dev", "-d", action="store_true",
                            help="Launches only the first created task. This "
                                 "is intended for development purposes.")
    parser_cld.add_argument("--region", "-r", action="store",
                            help="")
    parser_cld.add_argument("--bids", "-b", action="store_true",
                            help="Indicates that the tool being launched is a "
                                 "BIDS app. BIDS is a data organization format"
                                 " in neuroimaging. For more information about"
                                 " this, go to https://bids.neuroimaging.io.")

    parser_cld.set_defaults(func=cloud)

    # Share Parser
    parser_shr = subparsers.add_parser("share")
    parser_shr.add_argument("clowdrloc", help="local or s3 location for clowdr")
    parser_shr.add_argument("--debug", "-d", action="store_true")

    parser_shr.set_defaults(func=share)

    # Task Parser
    parser_task = subparsers.add_parser("task")
    parser_task.add_argument("metadata", help="task metadata file")
    parser_task.add_argument("--clowdrloc", "-c", action="store",
                             help="task output directory")
    parser_task.add_argument("--local", "-l", action="store_true")
    parser_task.add_argument("--workdir", "-w", action="store")
    parser_task.add_argument("--volumes", "-v", action="append")

    parser_task.add_argument("--verbose", "-V", action="store_true")

    parser_task.set_defaults(func=run)

    # Parse arguments
    inps = parser.parse_args(args) if args is not None else parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()
    else:
        inps.func(**vars(inps))


if __name__ == "__main__":
    main()

