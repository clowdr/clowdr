#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/controller/launcher.py
# Created by Greg Kiar on 2018-02-28.
# Email: gkiar@mcin.ca


# from clowdr.endpoint import aws, kubernetes, cbrain

from clowdr import utils


def configureResource(endpoint, auth, **kwargs):
    # TODO: document
    if endpoint == "aws":
        from clowdr.endpoint.AWS import AWS
        resource = AWS(auth)
        resource.setCredentials(**kwargs)
        resource.startSession()
        resource.configureIAM(**kwargs)
        resource.configureBatch(**kwargs)
        return resource

    elif endpoint == "kubernetes":
        print("Kubernetes endpoint not yet supported - coming soon!")

    elif endpoint == "cbrain":
        print("CBRAIN endpoint not yet supported - coming soon!")

    else:
        print("Endpoint not currently supported. Try: aws".format(endpoint))

