#!/usr/bin/env python
#
# This software is distributed with the MIT license:
# https://github.com/gkiar/clowdr/blob/master/LICENSE
#
# clowdr/endpoint/AWS.py
# Created by Greg Kiar on 2018-02-28.
# Email: gkiar@mcin.ca

from botocore.exceptions import *
import time
import boto3
import os.path as op
import json
import warnings
import os
import re

warnings.filterwarnings("ignore", message="numpy.dtype size changed")

import pandas as pd

from clowdr.endpoint.remote import Endpoint
from clowdr import __path__ as clowfile


clowfile = clowfile[0]


class AWS(Endpoint):
    # TODO: document

    def setCredentials(self, **kwargs):
        # TODO: document

        creds = pd.read_csv(self.credentials)
        self.access_key = creds['Access key ID'][0]
        self.secret_access = creds['Secret access key'][0]
        os.environ["AWS_ACCESS_KEY_ID"] = self.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.secret_access

        if kwargs.get("region"):
            self.region = kwargs["region"]
        else:
            self.region = "us-east-1"

    def startSession(self):
        # TODO: document
        self.session = boto3.Session(aws_access_key_id=self.access_key,
                                     aws_secret_access_key=self.secret_access,
                                     region_name=self.region)
        self.iam = self.session.client("iam")
        self.ec2 = self.session.client("ec2")
        self.batch = self.session.client("batch")

    def configureIAM(self, **kwargs):
        # TODO: document
        template = op.join(op.realpath(clowfile), "templates",
                           "AWS", "userRoles.json")

        with open(template) as fhandle:
            roles = json.load(fhandle)
        basestr = "arn:aws:iam::aws:policy/service-role/"
        policy = {"batch": basestr + "AWSBatchServiceRole",
                  "ecs":   basestr + "AmazonEC2ContainerServiceforEC2Role"}

        for rolename in roles:
            role = roles[rolename]
            try:
                name = role["RoleName"]
                response = self.iam.get_role(RoleName=name)
                role["Arn"] = response["Role"]["Arn"]

            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchEntity":
                    if kwargs.get("verbose"):
                        print("Role '{}' not found- creating.".format(name),
                              flush=True)
                    role["AssumeRolePolicyDocument"] = json.dumps(role["AssumeRolePolicyDocument"])
                    response = self.iam.create_role(**role)
                    role["Arn"] = response["Role"]["Arn"]
                    self.iam.create_instance_profile(InstanceProfileName=name)
                    self.iam.add_role_to_instance_profile(InstanceProfileName=name,
                                                          RoleName=name)
                    self.iam.attach_role_policy(RoleName=name,
                                                PolicyArn=policy[rolename])
                    roles[rolename] = role
            if kwargs.get("verbose"):
                print("Role ARN: {}".format(roles[rolename]["Arn"]),
                      flush=True)
        self.roles = roles

    def configureBatch(self, **kwargs):
        # TODO: document
        sg = [sg["GroupId"]
              for sg in self.ec2.describe_security_groups()["SecurityGroups"]
              if sg["GroupName"] == "default"]
        net = [nets["SubnetId"]
               for nets in self.ec2.describe_subnets()["Subnets"]]

        def waitUntilDone(name, status):
            while True:
                env = self.batch.describe_compute_environments(computeEnvironments=[name])
                stat = env["computeEnvironments"][0]["status"]
                if stat != status:
                    return

        template = op.join(op.realpath(clowfile), "templates",
                           "AWS", "computeEnvironment.json")
        with open(template) as fhandle:
            compute = json.load(fhandle)

        try:
            name = compute["computeEnvironmentName"]
            response = self.batch.describe_compute_environments(computeEnvironments=[name])
            if len(response["computeEnvironments"]):
                if (response["computeEnvironments"][0]["status"] != "VALID" or
                   response["computeEnvironments"][0]["state"] != "ENABLED"):
                    raise ClientError({"Error": {"Code": "InvalidEnvironment"}},
                                      "describe_compute_environments")
                else:
                    compute["computeEnvironmentArn"] = response["computeEnvironments"][0]["computeEnvironmentArn"]
            else:
                raise ClientError({"Error": {"Code": "NoSuchEntity"}},
                                  "describe_compute_environments")

        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidEnvironment":
                if kwargs.get("verbose"):
                    print("Environment '{}' invalid- deleting.".format(name),
                          flush=True)
                response = self.batch.update_compute_environment(computeEnvironment=name,
                                                                 state="DISABLED")
                waitUntilDone(name, "UPDATING")
                response = self.batch.delete_compute_environment(computeEnvironment=name)
                waitUntilDone(name, "DELETING")

            if (e.response["Error"]["Code"] == "NoSuchEntity" or
               e.response["Error"]["Code"] == "InvalidEnvironment"):
                if kwargs.get("verbose"):
                    print("Environment '{}' not found- creating.".format(name),
                          flush=True)
                compute["computeResources"]["subnets"] = net
                compute["computeResources"]["securityGroupIds"] = sg
                compute["computeResources"]["instanceRole"] = self.roles["ecs"]["Arn"].replace("role", "instance-profile")
                compute["serviceRole"] = self.roles["batch"]["Arn"]

                response = self.batch.create_compute_environment(**compute)
                waitUntilDone(name, "CREATING")
                waitUntilDone(name, "UPDATING")
                time.sleep(2)
                compute["computeEnvironmentArn"] = response["computeEnvironmentArn"]

        if kwargs.get("verbose"):
            print("Compute Environment ARN: {}".format(compute["computeEnvironmentArn"]),
                  flush=True)

        template = op.join(op.realpath(clowfile), "templates",
                           "AWS", "jobQueue.json")
        with open(template) as fhandle:
            queue = json.load(fhandle)

        try:
            name = queue["jobQueueName"]
            response = self.batch.describe_job_queues()
            if not len(response["jobQueues"]):
                raise ClientError({"Error": {"Code": "NoSuchEntity"}},
                                  "describe_job_queues")
            else:
                queue_names = [response["jobQueues"][i]["jobQueueName"]
                               for i in range(len(response["jobQueues"]))]
                if name not in queue_names:
                    raise ClientError({"Error": {"Code": "NoSuchEntity"}},
                                      "describe_job_queues")
                queue["jobQueueArn"] = response["jobQueues"][0]["jobQueueArn"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                if kwargs.get("verbose"):
                    print("Queue '{}' not found- creating.".format(name),
                          flush=True)
                response = self.batch.create_job_queue(**queue)
                queue["jobQueueArn"] = response["jobQueueArn"]
                time.sleep(2)
        if kwargs.get("verbose"):
            print("Job Queue ARN: {}".format(queue["jobQueueArn"]),
                  flush=True)

        template = op.join(op.realpath(clowfile), "templates",
                           "AWS", "jobDefinition.json")
        with open(template) as fhandle:
            job = json.load(fhandle)

        try:
            name = job["jobDefinitionName"]
            response = self.batch.describe_job_definitions()
            if (not len(response["jobDefinitions"]) or
               response["jobDefinitions"][0]["status"] == "INACTIVE"):
                raise ClientError({"Error": {"Code": "NoSuchEntity"}},
                                  "describe_job_definitions")
            else:
                job["jobDefinitionArn"] = response["jobDefinitions"][0]["jobDefinitionArn"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                if kwargs.get("verbose"):
                    print("Job '{}' not found- creating.".format(name),
                          flush=True)
                response = self.batch.register_job_definition(**job)
                job["jobDefinitionArn"] = response["jobDefinitionArn"]

        if kwargs.get("verbose"):
            print("Job Definition ARN: {}".format(job["jobDefinitionArn"]),
                  flush=True)

    def launchJob(self, taskloc):
        # TODO: document
        orides = {"environment":[{"name": "AWS_ACCESS_KEY_ID",
                                  "value": self.access_key},
                                 {"name": "AWS_SECRET_ACCESS_KEY",
                                  "value": self.secret_access}],
                  "command":["task", taskloc, "-V"]}
        # p1, p2 = re.match('.+/.+-([0-9a-zA-Z_]+)/clowdr/task-([A-Za-z0-9]+).json', taskloc).group(1, 2)
        p1, p2 = re.match('.+\/.+-(\w+)\/clowdr\/task-([A-Za-z0-9]+).json',
                          taskloc).group(1, 2)
        response = self.batch.submit_job(jobName="clowdr_{}-{}".format(p1, p2),
                                         jobQueue="clowdr-q",
                                         jobDefinition="clowdr",
                                         containerOverrides=orides)
        jid = response['jobId']
        return jid
