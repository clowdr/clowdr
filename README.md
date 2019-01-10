# Clowdr

Clowd is a command-line utility for iteratively developing pipelines, deploying them at scale, and sharing data and derivatives.

[![](https://img.shields.io/pypi/v/clowdr.svg)](https://pypi.python.org/pypi/clowdr)
[![Build Status](https://travis-ci.org/clowdr/clowdr.svg?branch=master)](https://travis-ci.org/clowdr/clowdr)
[![Coverage Status](https://coveralls.io/repos/github/clowdr/clowdr/badge.svg?branch=master)](https://coveralls.io/github/clowdr/clowdr?branch=master)
[![Documentation Status](https://readthedocs.org/projects/clowdr/badge/?version=latest)](https://clowdr.readthedocs.io/en/latest/?badge=latest)
[![DOI](https://zenodo.org/badge/121551982.svg)](https://zenodo.org/badge/latestdoi/121551982)

## Contents

- [Overview](#overview)
- [System Requirements](#system-requirements)
  - [Installation Instructions](#installation-instructions)
  - [Docker](#docker)
  - [Singularity](#singularity)
- [Usage](#usage)
  - [Local](#local)
  - [Cluster](#cluster)
  - [Cloud](#cloud)
  - [Share](#share)
- [Documentation](#documentation)
- [License](#license)
- [Issues](#issues)

## Overview
*Clowdr* can be thought of as a cloud execution utility for [Boutiques](http://boutiques.github.io), the JSON-based
descriptive command-line framework. As Boutiques and the [Boutiques tools](https://github.com/boutiques/boutiques) allow
the encapsulation, validation, evaluation, and deployment of command-line routines, *Clowdr* inherits and extends this 
functionality to remote datasets and computational resources.

*Clowdr* exposees several levels of evaluation: `local`, `cluster`, `cloud`, and `share`. The `local` runs tasks using the
system scheduler, and paired with the `-dev` flag can enable the rapid prototyping of tools, descriptors, and invocations.
The `cluster` mode generates the exact same executions as in `local` but submits them through a cluster's scheduler for parallel
execution. Similarly, `cloud` runs the tasks on a remote cloud such as Amazon. Finally, the `share` mode launches a light-weight
webserver, ultimately generating a static HTML page which can be stored and redistributed that documents provenance and run
information for the launched tasks.

## System Requirements
Clowdr requires Python3 and either Docker or Singularity. It has only been tested on Mac OSX and Linux, though no requirements
are specific to these operating systems and suggest that it may also function properly on Windows.

### Installation Instructions
Installation is quite simple - just run:

```
pip install clowdr
```

### Docker
Clowdr is available on Docker Hub, and can be downloaded with:

```
docker pull clowdr/clowdr
```

### Singularity
Clowdr is also available on Singularity Hub, and can be downloaded with:

```
singularity pull clowdr/clowdr
```

## Usage
(*For up to date command-lines please check out our [documentation](https://clowdr.rtfd.io)*)

Below we'll explore each of the main three modes of operation for Clowdr. If in doubt, always feel free to turn back to the help-text:

```
clowdr -h
```

### Local
From this directory, assuming the [BIDS dataset `ds114`](https://github.com/INCF/BIDS-examples) is installed at `/data/ds114`, and
your system has Docker installed, run:

```
clowdr local examples/descriptor_d.json examples/invocation.json examples/task/ /data/ds114/ -v /data/ds114/:/data/ds114 -b
```

What you just did was launched `clowdr` in `local` mode, with the tool `examples/descriptor_d.json`, invocation at `examples/invocation.json`,
where outputs will be stored in the Clowdr directory, `examples/task`, from data stored at `/data/ds114`, and being mounted to that same container
location, `-v /data/ds114:/data/ds114`, that happens to be organized according to the BIDS specification, `-b`. If you also wanted verbose output,
`-V`, or to develop, `-d`, as well as some other options, there are flags that can be discovered with the help flag, `-h`.

If the data wasn't organized in BIDS format, we could provide a directory of invocations in place of `examples/invocation.json`, or of course a
single invocation and omit the `-b` flag in both cases to run either a group of tasks or single task, respectively.

You can now look in the Clowdr directory to see the outputs of this pipeline.

### Cluster
IF you want to scale up your analysis, you can then turn to the cluster mode. The arguments supplied are exactly the same, with some minor
additions such as adding the hostname to your data location, specifying your cluster type, here `slurm`, and your account identifier, job
names, etc.

```
clowdr cluster ./examples/descriptor_s.json ./examples/invocation.json ./examples/task/ server.hostname.ca:/path/to/data/ slurm -v /path/to/data/:/data/ --account my-account-id --jobname clowdr-taskname -b
```

The execution takes place here exactly like in the local mode, where here we specified a singularity version of the descriptor. Flags such as
`-d` for development/single-execution mode also are consistent in this mode and helpful for prototyping analyses prior to large executions.


### Cloud
Presuming you ran locally and were happy with the results, but have larger collections of data you'd like to process, and don't have access to a
cluster, you can turn to the cloud. If you've uploaded the same dataset to Amazon Web Services S3 at `s3://mybucket/ds114/`, and have your
credentials stored in this directory at `credentials.csv`, run:

```
clowdr cloud examples/descriptor_d.json examples/invocation.json s3://mybucket/clowdr/ s3://mybucket/ds114/ aws credentials.csv -bv -r us-east-1
```

Here, you also did the same as above, except in `cloud` mode, with remote data on S3, specifying the Amazon endpoint, `aws`, and setting your Amazon
region to `us-east-1`.

### Share
Once Clowdr tasks are launched, they will return a directory which will be home to the output task information - either on Amazon S3 or local, depending
on the parameters provided. The share mode allows you to quickly inspect and explore the launched tasks, give updates on their status, and ultimately
provides a static HTML page which can be downloaded and shared with the processed derivatives as provenance information about the execution. You can
point the share service at either your Clowdr output directory, or in the case of an example packaged with the repository, the line below:

```
clowdr share ./examples/task/bids-example/clowdr/ -d
```

## Documentation
For detailed and up-to-date documentation, check out our read-the-docs page, at [clowdr.rtfd.io](http://clowdr.rtfd.io).

## License
This project is covered under the [MIT License](https://github.com/clowdr/clowdr/blob/master/LICENSE).

## Issues
If you're having trouble, notice a bug, or want to contribute (such as a fix to the bug you may have just found) feel free to open a
[git issue](https://github.com/clowdr/clowdr/issues/new) or pull request. Enjoy!

