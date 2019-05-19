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


def local(descriptor, invocation, provdir, backoff_time=36000, sweep=[],
          verbose=False, workdir=None, simg=None, rerun=None, run_id=None,
          task_ids=[], volumes=[], s3=None, cluster=None, jobname=None,
          clusterargs=None, dev=False, groupby=1, user=False, setup=False,
          bids=False, **kwargs):
    """cluster
    Launches a pipeline locally through the Clowdr wrappers.

    Parameters
    ----------
    descriptor : str
        Path to a boutiques descriptor for the tool to be run.
    invocation : str
        Path to a boutiques invocation for the tool and parameters to be run.
    provdir : str
        Path for storing Clowdr intermediate files and output logs.
    backoff_time : int (default = 36000)
        Maximum delay time before attempting resubmission of jobs that failed to
        be submitted to a scheduler, in seconds.
    sweep : list (default = [])
        List of parameters to sweep over in the provided invocations.
    verbose : bool (default = False)
        Flag toggling verbose output printing
    workdir : str (default = None)
        Working directory to be used in execution, if different from provdir.
    simg : str (default = None)
        Path to local copy of Singularity image to be used during execution.
    rerun : str (default = None)
        One of "all", "select", "failed", and "incomplete," which enables
        re-launching tasks from a previous execution either individually or in
        commonly-desired groups.
    run_id : str (default = None)
        Required when using rerun, above, this specifies the experiment ID to be
        re-run. This is the directory created for metadata, of the form:
            year-month-day_hour-minute-second-8digitID.
    task_ids : list (default = [])
        If re-running with the "select" mode, a list of task IDs within the
        directory specified by run_id which are to be re-run.
    volumes : list (default = [])
        List of volume mount-path strings, specified using the standard:
            /path/on/host/:/path/in/container/
    s3 : str (default = None)
        Path for accessing input data on an S3 bucket. Must include s3://.
    cluster : str (default = None)
        Scheduler on the cluster being used. Currently only slurm is supported.
    jobname : str (default = None)
        Base-name for the jobs as they will appear in the scheduler.
    clusterargs : str (default = None)
        Comma-separated list of arguments to be provided to the cluster on job
        submission. Such as: time:4:00,mem:2048,account:ABC
    dev : bool (default = False)
        Flag to toggle dev mode which only runs the first execution in the set.
    groupby : int (default = 1)
        Value which dictates the grouping of tasks. Particularly useful when
        tasks are short or a cluster restricts the number of unique jobs.
    user : bool (default = False)
        When running with Docker, toggles whether or not the host-user's UID is
        used within the container.
    setup : bool (default = False)
        Flag which prevents execution of tasks after the metadata task and
        invocation files are generated.
    bids : bool (default = False)
        Flag toggling BIDS-aware metadata preparation.
    **kwargs : dict
        Arbitrary additional keyword arguments which may be passed.

    Returns
    -------
    str
        The path to the created directory containing Clowdr experiment metadata.
    """
    descriptor = descriptor.name
    tool = utils.truepath(descriptor)
    if simg:
        simg = utils.truepath(simg)

    if verbose:
        print("Consolidating metadata...")

    dataloc = s3 if s3 else "localhost"
    if rerun:
        if not run_id:
            raise SystemExit("**Error: Option --rerun requires --run_id")
        if rerun == "select" and not task_ids:
            raise SystemExit("**Error: Option --rerun 'select' requires "
                             "--task_ids")

        tasks = rerunner.getTasks(provdir, run_id, rerun, task_ids=task_ids)
        if not len(tasks):
            if verbose:
                print("No tasks to run.")
            return 0

    else:
        [tasks, invocs] = metadata.consolidateTask(descriptor, invocation,
                                                   provdir, dataloc, bids=bids,
                                                   sweep=sweep, verbose=verbose)

    taskdir = op.dirname(utils.truepath(tasks[0]))
    try:
        os.mkdir(taskdir)
    except FileExistsError:
        pass
    os.chdir(taskdir)

    if setup:
        print(taskdir)
        return taskdir

    with open(tool) as fhandle:
        container = json.load(fhandle).get("container-image")

    if container:
        if verbose:
            print("Getting container...")
        outp = utils.getContainer(taskdir, container, verbose=verbose,
                                  simg=simg)

    if cluster:
        from slurmpy import Slurm
        jobname = jobname if jobname else "clowdr"
        cargs = {}
        if clusterargs:
            for opt in clusterargs.split(","):
                k, v = opt.split(":")[0], opt.split(":")[1:]
                v = ":".join(v)
                cargs[k] = v
        job = Slurm(jobname, cargs)

        script = "clowdr task {} -p {} --local"
        if workdir:
            script += " -w {}".format(workdir)
        if volumes:
            script += " ".join([" -v {}".format(vol)
                                for vol in volumes])
        if container:
            script += " --imagepath {}".format(outp)
        if verbose:
            script += " -V"

    # Groups tasks into collections to be run together (default size = 1)
    gsize = groupby if groupby else 1
    taskgroups = [tasks[i:i+gsize] for i in range(0, len(tasks), gsize)]

    if dev:
        taskgroups = [taskgroups[0]]  # Just launch the first in dev mode

    if verbose:
        print("Launching tasks...")

    for taskgroup in taskgroups:
        if verbose:
            print("... Processing task(s): {}".format(", ".join(taskgroup)))

        if cluster:
            tmptaskgroup = " ".join(taskgroup)
            func = job.run
            args = [script.format(tmptaskgroup, taskdir)]
            # Submit. If submission fails, retry with fibonnaci back-off
            utils.backoff(func, args, {},
                          backoff_time=backoff_time, **kwargs)
        else:
            runtask(taskgroup, provdir=taskdir, local=True, verbose=verbose,
                    workdir=workdir, volumes=volumes, user=user,  **kwargs)

    if verbose:
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
    print(kwargs)
    for task in tasklist:
        handler = TaskHandler(task, **kwargs)


def share(provdir, prepare=False, host="0.0.0.0", port=8050, verbose=False,
          debug=False, **kwargs):
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
        tmpdir = op.join(tmploc, utils.splitS3Path(provdir)[1])
        provdir = tmpdir
        if verbose:
            print("Local cache of directory: {}".format(provdir))

    if op.isfile(provdir):
        if verbose:
            print("Summary file provided - no need to generate.")
        summary = provdir
        with open(summary) as fhandle:
            experiment_dict = json.load(fhandle)
    else:
        summary = op.join(provdir, 'clowdr-summary.json')
        experiment_dict = consolidate.summary(provdir, summary)

    if prepare:
        if verbose:
            print("Summary file location: {}".format(summary))
        return summary

    customDash = portal.CreatePortal(experiment_dict, N=100)
    app = customDash.launch()

    app.run_server(host=host, debug=debug, port=port)


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
    parser_loc.add_argument("--sweep", type=str, action="append",
                            help="If you wish to perform a parameter sweep with"
                                 " Clowdr, you can use this flag and provide "
                                 "Boutiques parameter ID as the argument here. "
                                 "This requires: 1) the parameter exists in "
                                 "the provided invocation, and 2) that field "
                                 "contains a list of the parameter values to "
                                 "be used (if it is ordinarily a list, this "
                                 "means it must be a list of lists here). This"
                                 " option does not work with directories of "
                                 "invocations, but only single files.")
    parser_loc.add_argument("--setup", action="store_true",
                            help="If you wish to generate metadata but not "
                                 "launch tasks then you can use this mode.")
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
                            choices=["all", "select", "failed", "incomplete"],
                            help="Allows user to re-run jobs in a previous "
                                 "execution that either failed or didn't "
                                 "finish, etc. This requires the --run_id "
                                 "argument to also be supplied. Four choices "
                                 "are: 'all' to re-run all tasks, 'select' to "
                                 "re-run specific tasks, 'failed' to re-run "
                                 "tasks which finished with a non-zero "
                                 "exit-code, 'incomplete' to re-run tasks "
                                 "which have not yet indicated job completion. "
                                 "While the descriptor and invocations will be "
                                 "adopted from the previous executions, other "
                                 "options such as clusterargs or volume can "
                                 "be set to different values, if they were the "
                                 "source of errors. Pairing the incomplete mode"
                                 " with the --dev flag allows you to walk "
                                 "through your dataset one group at a time.")
    parser_loc.add_argument("--run_id", action="store",
                            help="Pairs with --rerun. This ID is the directory"
                                 " within the supplied provdir which contains "
                                 "execution you wish to relaunch. These IDs/"
                                 "directories are in the form: year-month-day_"
                                 "hour-minute-second-8digitID.")
    parser_loc.add_argument("--task_ids", action="store", nargs="+",
                            help="Pairs with --rerun. This list of task IDs are"
                                 " the task numbers within the directory "
                                 "supplied with --run_id and provdir. These "
                                 "IDs are integers greater than or equal to 0.")
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
    parser_cld.add_argument("--sweep", type=str, action="append",
                            help="If you wish to perform a parameter sweep with"
                                 " Clowdr, you can use this flag and provide "
                                 "Boutiques parameter ID as the argument here. "
                                 "This requires: 1) the parameter exists in "
                                 "the provided invocation, and 2) that field "
                                 "contains a list of the parameter values to "
                                 "be used (if it is ordinarily a list, this "
                                 "means it must be a list of lists here). This"
                                 " option does not work with directories of "
                                 "invocations, but only single files.")
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
                                 "or clowdr local. This can also be a clowdr-"
                                 "generated summary file.")
    parser_shr.add_argument("--prepare", "-p", action="store_true",
                            help="If provided, this prevents a server from "
                                 "being launched after metadata is consolidated"
                                 " into a single file, and the path to that "
                                 "file is returned.")
    parser_shr.add_argument("--host", action="store", default="0.0.0.0",
                            help="The host to broadcast the share service at. "
                                 "Default is 0.0.0.0.")
    parser_shr.add_argument("--port", action="store", type=int, default=8050,
                            help="The port to broadcast the share service at. "
                                 "Default is 8050.")
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
    parser_task.add_argument("--imagepath", action="store",
                             help="If the Boutiques descriptor summarizes a "
                                  "tool wrapped in Singularity, and the image "
                                  "has already been downloaded, this option "
                                  "allows you to specify that image file.")


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
