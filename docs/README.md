### Sphinx Documentation for Clowdr

Compiling this documentation requires installing the following:

    pip install sphinx sphinx-autobuild sphinx-argparse 

Then, the docs can be re-built running the following from this directory:

    sphinx-apidoc -o ./package ../clowdr/ -f --ext-viewcode -H "Clowdr Python Interface"
    make clean && make html
