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

from clowdr.controller import metadata, launcher, rerunner
from clowdr.task import TaskHandler
# from clowdr.server import shareapp, updateIndex
from clowdr.share import consolidate, portal
from clowdr import utils


def local(descriptor, invocation, provdir, backoff_time=36000, **kwargs):
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
        - backoff_time: int
            Time limit for wait times when resubmitting jobs to a scheduler
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
    descriptor = descriptor.name
    tool = utils.truepath(descriptor)
    if kwargs.get("simg"):
        kwargs["simg"] = utils.truepath(kwargs["simg"])

    if kwargs.get("verbose"):
        print("Consolidating metadata...")

    dataloc = kwargs.get("s3") if kwargs.get("s3") else "localhost"
    if kwargs.get("rerun"):
        if not kwargs.get("run_id"):
            raise SystemExit("**Error: Option --rerun requires --run_id")
        tasks = rerunner.getTasks(provdir, kwargs["run_id"], kwargs["rerun"])
        if not len(tasks):
            if kwargs.get("verbose"):
                print("No tasks to run.")
            return 0

    else:
        [tasks, invocs] = metadata.consolidateTask(descriptor, invocation,
                                                   provdir, dataloc, **kwargs)

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

    if kwargs.get("cluster"):
        from slurmpy import Slurm
        jobname = kwargs.get("jobname") if kwargs.get("jobname") else "clowdr"
        slurm_args = {}
        if kwargs.get("slurm_args"):
            for opt in kwargs.get("slurm_args").split(","):
                k, v = opt.split(":")[0], opt.split(":")[1:]
                v = ":".join(v)
                slurm_args[k] = v
        job = Slurm(jobname, slurm_args)

        script = "clowdr task {} -p {} --local"
        if kwargs.get("workdir"):
            script += " -w {}".format(kwargs["workdir"])
        if kwargs.get("volumes"):
            script += " ".join([" -v {}".format(vol)
                                for vol in kwargs.get("volumes")])

    # Groups tasks into collections to be run together (default size = 1)
    gsize = kwargs["groupby"] if kwargs.get("groupby") else 1
    taskgroups = [tasks[i:i+gsize] for i in range(0, len(tasks), gsize)]

    if kwargs.get("dev"):
        taskgroups = [taskgroups[0]]  # Just launch the first in dev mode

    if kwargs.get("verbose"):
        print("Launching tasks...")

    for taskgroup in taskgroups:
        if kwargs.get("verbose"):
            print("... Processing task(s): {}".format(", ".join(taskgroup)))

        if kwargs.get("cluster"):
            tmptaskgroup = " ".join(taskgroup)
            func = job.run
            args = [script.format(tmptaskgroup, taskdir)]
            # Submit. If submission fails, retry with fibonnaci back-off
            utils.backoff(func, args, backoff_time=backoff_time, **kwargs)
        else:
            runtask(taskgroup, provdir=taskdir, local=True, **kwargs)

    if kwargs.get("verbose"):
        print(taskdir)
    return taskdir


def cloud(descriptor, invocation, provdir, s3, cloud, credentials, **kwargs):
    """cloud
    Launches a pipeline locally at scale through Clowdr.

    Parameters
    ----------
    descriptor : str
        Path to a boutiques descriptor for the tool to be run
    invocation : str
        Path to a boutiques invocation for the tool and parameters to be run
    provdir : str
        Path on S3 for storing Clowdr intermediate files and outputs
    s3 : str
        Path on S3 for accessing input data
    cloud : str
        Which endpoint to use for deployment
    credentials : str
        Credentials for Amazon with access to dataloc, clowdrloc, and Batch
    **kwargs : dict
        Arbitrary keyword arguments (i.e. {'verbose': True})

    Returns
    -------
    int
        The exit-code returned by the task being executed
    """
    # TODO: scrub inputs better
    descriptor = descriptor.name
    provdir = provdir.strip('/')

    # Create temp dir for clowdrloc
    tmploc = utils.truepath(tempfile.mkdtemp())

    [tasks, invocs] = metadata.consolidateTask(descriptor, invocation, tmploc,
                                               s3, **kwargs)
    metadata.prepareForRemote(tasks, tmploc, provdir)
    resource = launcher.configureResource(cloud, credentials, **kwargs)

    tasks_remote = [task for task in utils.post(tmploc, provdir)
                    if "task-" in task]

    if kwargs.get("dev"):
        tasks_remote = [tasks_remote[0]]  # Just launch the first in dev mode

    jids = []
    for task in tasks_remote:
        jids += [resource.launchJob(task)]

    taskdir = op.dirname(utils.truepath(tasks_remote[0]))
    print(taskdir)
    return taskdir, jids


def runtask(tasklist, **kwargs):
    for task in tasklist:
        handler = TaskHandler(task, **kwargs)


def share(provdir, **kwargs):
    """share
    Launches a simple web server which showcases all runs at the clowdrloc.

    Parameters
    ----------
    provdir : str
        Path with Clowdr metdata files (returned from "local" and "deploy")
    **kwargs : dict
        Arbitrary keyword arguments (i.e. {'verbose': True})

    Returns
    -------
    None
    """
    if provdir.startswith("s3://"):
        # Create temp dir for clowdrloc
        tmploc = utils.truepath(tempfile.mkdtemp())
        utils.get(provdir, tmploc, **kwargs)
        provdir = tmploc
        if kwargs.get("verbose"):
            print("Local cache of directory: {}".format(provdir))

    summary = op.join(provdir, 'clowdr-summary.json')
    experiment_dict = consolidate.summary(provdir, summary)

    customDash = portal.CreatePortal(experiment_dict)
    app = customDash.launch()

    host = kwargs["host"] if kwargs.get("host") else "0.0.0.0"
    app.run_server(host=host, debug=kwargs.get("debug"))


def makeparser():
    """makeparser
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
    parser_loc.add_argument("--groupby", "-g", type=int,
                            help="If you wish to run tasks in batches, specify "
                                 "the number of tasks to group here. For "
                                 "imperfect multiples, the last group will be "
                                 "the remainder.")
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
    parser_loc.add_argument("--rerun", "-R",
                            choices=["all", "failed", "incomplete"],
                            help="Allows user to re-run jobs in a previous "
                                 "execution that either failed or didn't "
                                 "finish, etc. This requires the --run_id "
                                 "argument to also be supplied. Three choices "
                                 "are: 'all' to re-run all tasks, 'failed' to "
                                 "re-run tasks which finished with a non-zero "
                                 "exit-code, 'incomplete' to re-run tasks "
                                 "which have not yet indicated job completion."
                                 " While the descriptor and invocations will be"
                                 " adopted from the previous executions, other "
                                 "options such as clusterargs or volume can "
                                 "be set to different values, if they were the "
                                 "source or errors. Pairing the incomplete mode"
                                 " with the --dev flag allows you to walk "
                                 "through your dataset one group at a time.")
    parser_loc.add_argument("--run_id", action="store",
                            help="Pairs with --rerun. This ID is the directory"
                                 " within the supplied provdir which contains "
                                 "execution you wish to relaunch. These IDs/"
                                 "directories are in the form: year-month-day_"
                                 "hour-minute-second-8digitID.")
    parser_loc.add_argument("--s3", action="store",
                            help="Amazon S3 bucket and path for remote data. "
                                 "Accepted in the format: s3://{bucket}/{path}")
    parser_loc.add_argument("--bids", "-b", action="store_true",
                            help="Indicates that the tool being launched is a "
                                 "BIDS app. BIDS is a data organization format"
                                 " in neuroimaging. For more information about"
                                 " this, go to https://bids.neuroimaging.io.")

    parser_loc.set_defaults(func=local)

    # Create the subparser for cloud execution.
    desc = ("Manages cloud deployment. Ideal for running jobs at scale on data "
            "stored in Amazon Web Services S3 buckets (or similar object "
            "store).")
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
                            help="Specifies which cloud endpoint you'd like to"
                                 " use. Currently, only AWS is supported.")
    parser_cld.add_argument("credentials",
                            help="Your credentials file for the resource.")

    parser_cld.add_argument("--verbose", "-V", action="store_true",
                            help="Toggles verbose output statements.")
    parser_cld.add_argument("--dev", "-d", action="store_true",
                            help="Launches only the first created task. This "
                                 "is intended for development purposes.")
    parser_cld.add_argument("--region", "-r", action="store",
                            help="The Amazon region to use for processing.")
    parser_cld.add_argument("--bids", "-b", action="store_true",
                            help="Indicates that the tool being launched is a "
                                 "BIDS app. BIDS is a data organization format"
                                 " in neuroimaging. For more information about"
                                 " this, go to https://bids.neuroimaging.io.")

    parser_cld.set_defaults(func=cloud)

    # Create the subparser for sharing outputs
    desc = ("Launches light-weight web service for exploring, managing, and "
            "sharing the outputs and provenance recorded from Clowdr "
            "executed workflows.")
    parser_shr = subparsers.add_parser("share")
    parser_shr.add_argument("provdir",
                            help="Local or S3 directory where Clowdr provenance"
                                 "records and metadata are stored. This path "
                                 "was returned by running either clowdr cloud "
                                 "or clowdr local.")
    parser_shr.add_argument("--debug", "-d", action="store_true",
                            help="Toggles server messages and logging. This "
                                 "is intended for development purposes.")
    parser_shr.add_argument("--verbose", "-V", action="store_true",
                            help="Toggles verbose output statements.")

    parser_shr.set_defaults(func=share)

    # Create the subparser for launching tasks
    desc = ("Launches a list of tasks with provenance recording. This method "
            "is what specifically wraps tool execution, is called by other "
            "Clowdr modes, and can be used to re-execute or debug tasks.")
    parser_task = subparsers.add_parser("task")
    parser_task.add_argument("tasklist", nargs="+",
                             help="One or more Clowdr-created task.json files "
                                  "summarizing the jobs to be run. These task "
                                  "files are created by one of clowdr cloud or"
                                  " clowdr local.")

    parser_task.add_argument("--verbose", "-V", action="store_true",
                             help="Toggles verbose output statements.")
    parser_task.add_argument("--provdir", "-p", action="store",
                             help="Local or directory where Clowdr provenance "
                                  "records and metadata will be stored. This "
                                  "is optional here because it will be stored "
                                  "by default in a temporary location and "
                                  "moved, unless this is specified.")
    parser_task.add_argument("--local", "-l", action="store_true",
                             help="Flag indicator to identify whether the task"
                                  " is being launched on a cloud or local "
                                  "resource. This is important to ensure data "
                                  "is transferred off clouds before shut down.")
    parser_task.add_argument("--workdir", "-w", action="store",
                             help="Specifies the working directory to be used "
                                  "by the tasks created.")
    parser_task.add_argument("--volumes", "-v", action="append",
                             help="Specifies any volumes to be mounted to the "
                                  "container. This is usually related to the "
                                  "path of any data files as specified in your "
                                  "invocation(s).")

    parser_task.set_defaults(func=runtask)
    return parser


def main(args=None):
    parser = makeparser()

    # Parse arguments
    inps = parser.parse_args(args) if args is not None else parser.parse_args()

    # If no args are provided, print help
    if len(sys.argv) < 2 and args is None:
        parser.print_help()
        sys.exit()
    else:
        inps.func(**vars(inps))
        return 0


if __name__ == "__main__":
    main()
