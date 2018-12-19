## Release procedure

When releasing a new version of `clowdr`, the following files must be updated with the new version:

- setup.py (L4)
- build/Dockerfile (L21)
- clowdr/templates/AWS/jobDefinition.json (L6)

Then, once this is done, run the following:

    python setup.py sdist
    python setup.py bdist_wheel
    twine upload dist/clowdr*
    docker build -t clowdr/clowdr:<newversion> ./build/
    docker push clowdr/clowdr:<newversion>

