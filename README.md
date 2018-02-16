# clowdr
Command-line utility for iteratively developing pipelines, deploying them at scale, and sharing data

## Installation

```
pip install clowdr
```

## Usage
From the `./examples` directory, assuming the dataset `ds114` is installed at `/clowdata/ds114`, run:

```
clowdr dev descriptor.json invocation.json ./task/ /clowdata/ds114/ -b
```
