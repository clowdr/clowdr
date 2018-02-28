# Clowdr

Clowd is a command-line utility for iteratively developing pipelines, deploying them at scale, and sharing data and derivatives.

[![](https://img.shields.io/pypi/v/clowdr.svg)](https://pypi.python.org/pypi/clowdr)
[![Build Status](https://travis-ci.org/clowdr/clowdr.svg?branch=master)](https://travis-ci.org/clowdr/clowdr)
![Docker Pulls](https://img.shields.io/docker/pulls/clowdr/clowdr.svg)
[![https://www.singularity-hub.org/static/img/hosted-singularity--hub-%23e32929.svg](https://www.singularity-hub.org/static/img/hosted-singularity--hub-%23e32929.svg)](https://singularity-hub.org/collections/663)

## Contents

- [Overview](#overview)
- [System Requirements](#system-requirements)
- [Installation Instructions](#installation-instructions)
- [Docker](#docker)
- [Demo](#demo)
- [Usage](#usage)
- [Documentation](#documentation)
- [License](#license)
- [Issues](#issues)

## Overview
*Clowdr* can be thought of as a cloud execution utility for [Boutiques](http://boutiques.github.io), the JSON-based
descriptive command-line framework. As Boutiques and the [Boutiques tools](https://github.com/boutiques/boutiques) allow
the encapsulation, validation, evaluation, and deployment of command-line routines, *Clowdr* inherits and extends this 
functionality to remote datasets and computational resources.

*Clowdr* exposees three levels of evaluation: `dev`, `deploy` and `share`. The `dev` mode allows the generation of task scripts,
and executes the first task of those submitted. The `deploy` mode generates the exact same executions as in `dev` but ships them
off to remote resources for parallel execution. The execution information and derivatives can then be explored and distributed
via the `share` mode, which launches a light-weight webserver.

## System Requirements
*Clowdr* requires either Python3 or Docker. It has only been tested on Mac OSX and Linux, though no requirements are specific to
these operating systems and suggest that it will also function properly on Windows.

### Installation Instructions
Installation is quite simple - just run:
```
pip install clowdr
```

### Docker
*Clowdr* is available on Docker Hub, and can be downloaded with:
```
docker pull clowdr/clowdr
```

### Singularity
*Clowdr* is also available on Singularity Hub, and can be downloaded with:
```
singularity pull clowdr/clowdr
```

## Demo

## Usage
From the this directory, assuming the dataset `ds114` is installed at `/clowdata/ds114`, run:

```
clowdr dev examples/descriptor.json examples/invocation.json examples/task/ /clowdata/ds114/ -b
```
 
OR

```
docker build -t gkiar/clowdr .
docker run -ti -v /clowdata/:/clowdata -v /var/run/docker.sock:/var/run/docker.sock -v ${PWD}:${PWD} -w ${PWD} --privileged gkiar/clowdr dev examples/descriptor.json examples/invocation.json examples/task/ /clowdata/ds114/ -b
```

## Documentation
Currently, the only documentation on this project is in this README file and file docstrings - this section will be updated when an explicit function reference has been generated.

## License
This project is covered under the [MIT License](https://github.com/clowdr/clowdr/blob/master/LICENSE).

## Issues
If you're having trouble, notice a bug, or want to contribute (such as a fix to the bug you may have just found) feel free to open a git issue or pull request. Enjoy!

