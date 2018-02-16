# clowdr
Command-line utility for iteratively developing pipelines, deploying them at scale, and sharing data

## Installation

```
pip install clowdr
```

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
