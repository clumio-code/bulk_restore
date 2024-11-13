# Copyright 2024, Clumio, a Commvault Company.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import requests
from datetime import datetime, timedelta, timezone
import boto3
import time
import json
import random
import string
from botocore.exceptions import ClientError
import re
import urllib.parse

api_dict = {
    "001": {"name": "EC2BackupList", "api": "backups/aws/ec2-instances",
            "header": "application/api.clumio.backup-aws-ebs-volumes=v2+json", "version": "v2",
            "desc": "List EC2 instance backups", "type": "get", "success": 200,
            "query_parms": {"limit": 100, "start": 1,
                            "filter": {
                                "start_timestamp": [
                                    "$lte", "$gt"],
                                "instance_id": [
                                    "$eq"]},
                            "sort": [
                                "-start_timestamp",
                                "start_timestamp"]}},
    "002": {"name": "environment_id",
            "api": "datasources/aws/environments",
            "header": "application/api.clumio.aws-environments=v1+json",
            "version": "v1",
            "desc": "List AWS environments",
            "type": "get",
            "success": 200,
            "query_parms": {
                "limit": 100,
                "start": 1,
                "filter": {
                    "account_native_id": [
                        "$eq",
                        "$begins_with"],
                    "aws_region": ["$eq"],
                    "connection_status": [
                        "$eq"],
                    "services_enabled": [
                        "$contains"]
                },
                "embed": [
                    "read-aws-environment-ebs-volumes-compliance-stats",
                    "read-aws-environment-ec2-instances-compliance-stats",
                    "read-aws-environment-rds-resources-compliance-stats",
                    "read-aws-environment-dynamodb-tables-compliance-stats",
                    "read-aws-environment-protection-groups-compliance-stats",
                    "read-aws-environment-ec2-mssql-compliance-stats"
                ]
            }
            },
    "003": {"name": "RestoreEC2",
            "api": "restores/aws/ec2-instances",
            "header": "application/api.clumio.restored-aws-ec2-instances=v1+json",
            "version": "v1",
            "body_parms": {"source": None,
                           "target": ["ami_restore_target", "instance_restore_target", "volumes_restore_target"]},
            "PayloadTemplates": {
                "ebs_block_device_mappings": {
                    "condition": {"xor": [], "and": ["volume_native_id"]},
                    "volume_native_id": None,
                    "kms_key_native_id": None,
                    "name": None,
                    "tags": {}
                },
                "network_interfaces": {
                    "condition": {"xor": [], "and": ["device_index"]},
                    "device_index": None,
                    "network_interface_native_id": None,
                    "restore_default": False,
                    "restore_from_backup": False,
                    "security_group_native_ids": [None],
                    "subnet_native_id": None
                },
                "tags": {
                    "condition": {"xor": [], "and": ["key", "value"]},
                    "key": None,
                    "value": None
                },
                "ami_restore_target": {
                    "condition": {"xor": [], "and": ["ebs_block_device_mappings", "environment_id", "name"]},
                    "description": None,
                    "ebs_block_device_mappings": {},
                    "environment_id": None,
                    "name": None,
                    "tags": {}
                },
                "volumes_restore_target": {
                    "condition": {"xor": ["aws_az", "target_instance_native_id"],
                                  "and": ["ebs_block_device_mappings", "environment_id"]},
                    "aws_az": None,
                    "ebs_block_device_mappings": {},
                    "target_instance_native_id": None,
                    "environment_id": None
                },
                "instance_restore_target": {
                    "condition": {"xor": [],
                                  "and": ["ebs_block_device_mappings", "environment_id", "network_interfaces",
                                          "subnet_native_id", "vpc_native_id"]},
                    "should_power_on": False,
                    "ami_native_id": None,
                    "aws_az": None,
                    "ebs_block_device_mappings": {},
                    "environment_id": None,
                    "iam_instance_profile_name": None,
                    "key_pair_name": None,
                    "subnet_native_id": None,
                    "tags": {},
                    "vpc_native_id": None,
                    "network_interfaces": {}
                }
            },
            "desc": "Restore an EC2 instance",
            "type": "post",
            "success": 202,
            "query_parms": {
                "embed": [
                    "read-task"
                ]
            }
            },
    "004": {"name": "EBSBackupList",
            "api": "backups/aws/ebs-volumes",
            "header": "application/api.clumio.backup-aws-ebs-volumes=v2+json",
            "version": "v2",
            "desc": "List EBS volume backups",
            "type": "get",
            "success": 200,
            "query_parms": {
                "limit": 100,
                "start": 1,
                "filter": {
                    "start_timestamp": [
                        "$lte", "$gt"],
                    "volume_id": [
                        "$eq"]
                },
                "sort": [
                    "-start_timestamp",
                    "start_timestamp"]
            }
            },
    "005": {"name": "RestoreEBS",
            "api": "restores/aws/ebs-volumes",
            "header": "application/api.clumio.restored-aws-ebs-volumes=v2+json",
            "version": "v2",
            "body_parms": {"source": None,
                           "target": {"condition": {"xor": [], "and": ["aws_az", "environment_id"]},
                                      "aws_az": None,
                                      "environment_id": None,
                                      "iops": 0,
                                      "kms_key_native_id": None,
                                      "tags": {},
                                      "type": None
                                      }
                           },
            "desc": "Restore an EBS Volume",
            "type": "post",
            "success": 202,
            "query_parms": {
                "embed": [
                    "read-task"
                ]
            }
            },
    "006": {"name": "ListEC2Instances",
            "api": "datasources/aws/ec2-instances",
            "header": "application/api.clumio.aws-ec2-instances=v1+json",
            "version": "v1",
            "desc": "List EC2 instances",
            "type": "get",
            "success": 200,
            "query_parms": {
                "limit": 100,
                "start": 1,
                "filter": {
                    "environment_id": [
                        "$eq"],
                    "name": [
                        "$contains", "$eq"],
                    "instance_native_id": [
                        "$contains", "$eq"],
                    "account_native_id": [
                        "$eq"],
                    "compliance_status": [
                        {"$eq": ["compliant", "non_compliant"]}],
                    "protection_status": [
                        {"$eq": ["protected", "unprotected", "unsupported"]}],
                    "tags.id": [
                        "$all"],
                    "is_deleted": [
                        {"$eq": ["true", "false"]}],
                    "availability_zone": [
                        "$eq"]
                },
                "embed": [
                    "read-policy-definition"
                ]
            }
            },
    "007": {"name": "BackupEC2",
            "api": "backups/aws/ec2-instances",
            "header": "application/api.clumio.backup-aws-ec2-instances=v1+json",
            "version": "v2",
            "body_parms": {
                "type:": {
                    "condition": {"xor": ["clumio_backup", "aws_snapshot"], "and": []}
                },
                "instance_id": None,
                "setting": {
                    "retention_duration": {
                        "condition": {"xor": [], "and": ["unit", "value"]},
                        "unit": {"condition": {"xor": ["hours", "days", "weeks", "months", "years"], "and": []}},
                        "value": 0
                    },
                    "advanced_settings": {
                        "condition": {"xor": ["aws_ebs_volume_backup", "aws_ec2_instance_backup"], "and": []},
                        "aws_ebs_volume_backup": {"condition": {"xor": ["standard", "lite"], "and": []}},
                        "aws_ec2_instance_backup": {"condition": {"xor": ["standard", "lite"], "and": []}},
                    },
                    "backup_aws_region": None
                },
            },
            "desc": "Backup an EC2 instance on demand",
            "type": "post",
            "success": 202,
            "query_parms": {
                "embed": [
                    "read-task"
                ]
            }
            },
    "008": {"name": "Connections",
            "api": "connections/aws/connection-groups",
            "header": "application/api.clumio.aws-environments=v1+json",
            "version": "v1",
            "desc": "Add AWS Connections",
            "type": "post",
            "success": 200,
            "query_parms": {
                "limit": 100,
                "start": 1,
                "filter": {
                    "account_native_id": [
                        "$eq",
                        "$begins_with"],
                    "master_region": ["$eq"],
                    "aws_region": ["$eq"],
                    "asset_types_enabled": ["ebs", "rds", "DynamoDB", "EC2MSSQL", "S3", "ec2"],
                    "description": ["$eq"]
                }
            }
            },
    "010": {"name": "ListS3Bucket",
            "api": "datasources/aws/s3-buckets",
            "header": "application/api.clumio.aws-s3-buckets=v1+json",
            "version": "v1",
            "desc": "Returns a list of S3 buckets",
            "type": "get",
            "success": 200,
            "query_parms": {
                "limit": 100,
                "start": 1,
                "filter": {
                    "environment_id": [
                        "$eq"],
                    "name": [
                        "$contains", "$in"],
                    "account_native_id": [
                        "$eq"],
                    "aws_region": [
                        "$eq", "$in"],
                    "is_deleted": [
                        "$eq"],
                    "tags.id": [
                        "$all"],
                    "aws_tag": [
                        "$in", "$all"],
                    "excluded_aws_tag": [
                        "$all"],
                    "organizational_unit_id": [
                        "$in"],
                    "asset_id": [
                        "$in"],
                    "event_bridge_enabled": [
                        "$eq"],
                    "is_versioning_enabled": [
                        "$eq"],
                    "is_encryption_enabled": [
                        "$eq"],
                    "is_replication_enabled": [
                        "$eq"],
                    "is_supported": [
                        "$eq"],
                    "is_active": [
                        "$eq"]
                }
            }
            },
    "011": {"name": "RetrieveTask",
            "api": "tasks",
            "header": "application/api.clumio.tasks=v1+json",
            "version": "v1",
            "desc": "Retrieve a task",
            "type": "get",
            "success": 200,
            },
    "012": {"name": "DynamoDBBackupList",
            "api": "backups/aws/dynamodb-tables",
            "header": "application/api.clumio.backup-aws-dynamodb-tables=v1+json",
            "version": "v2",
            "desc": "Retrieves a list of DynamoDB table backups",
            "type": "get",
            "success": 200,
            "query_parms": {
                "limit": 100,
                "start": 1,
                "filter": {
                    "start_timestamp": [
                        "$lte", "$gt"],
                    "table_id": [
                        "$eq"],
                    "type": ["$all"],  # clumio_backup,aws_snapshot
                    "condition": {"type": {"xor": ["clumio_backup", "aws_snapshot"],
                                           "and": []}},
                },
                "sort": [
                    "-start_timestamp",
                    "start_timestamp"]
            }
            },
    "013": {"name": "RestoreDDN",
            "api": "restores/aws/dynamodb-tables",
            "header": "application/api.clumio.restored-aws-dynamodb-tables=v1+json",
            "version": "v1",
            "body_parms": {"source": {"condition": {"xor": ["securevault_backup", "continuous_backup"], "and": []},
                                    "continuous_backup":{
                                            "table_id":None,
                                            "timestamp":None,
                                            "use_latest_restorable_time":False
                                        },
                                    "securevault_backup":{
                                            "backup_id": None
                                        },
                           "target": {"condition": {"xor": [], "and": ["table_name", "environment_id"]},
                                      "table_name": None,
                                      "environment_id": None,
                                      "iops": 0,
                                      "kms_key_native_id": None,
                                      "tags": {},
                                      "type": None,
                                      "provisioned_throughput": {
                                            "read_capacity_units": None,
                                            "write_capacity_units": None
                                        },
                                       "sse_specification": {
                                            "kms_key_type": "DEFAULT",
                                            "kms_master_key_id": "null"
                                        },
                                       "billing_mode": None,
                                       "global_secondary_indexes": [
                                            {
                                                "projection": {
                                                    "projection_type": None,
                                                    "non_key_attributes": [None]
                                                },
                                                "provisioned_throughput": {
                                                    "read_capacity_units": None,
                                                    "write_capacity_units": None
                                                },
                                                "index_name": "a",
                                                "key_schema": [
                                                    {
                                                        "attribute_name": "a",
                                                        "key_type": "HASH"
                                                    }
                                                ]
                                            }
                                        ],
                                       "local_secondary_indexes": [
                                            {
                                                "projection": {
                                                    "projection_type": None,
                                                    "non_key_attributes": [None]
                                                },
                                                "index_name": None,
                                                "key_schema": [
                                                    {
                                                        "attribute_name": None,
                                                        "key_type": None
                                                    }
                                                ]
                                            }
                                        ],
                                       "table_class": None,
                                       "table_name_": None
                                      }
                           },
                    },
            "desc": "Restores the specified DynamoDB table backup to the specified target destination",
            "type": "post",
            "success": 202,
            "query_parms": {
                    "embed": [
                        "read-task"
                        ]
                    }
            },
    "014": {"name": "RDSBackupList", "api": "backups/aws/rds-resources",
            "header": "application/json", "version": "",
            "desc": "Retrieves a list of RDS database backups", "type": "get", "success": 200,
            "query_parms": {"limit": 100, "start": 1,
                            "filter": {
                                "start_timestamp": [
                                    "$lte", "$gt"],
                                "resource_id": [
                                    "$eq"]},
                            "sort": [
                                "-start_timestamp",
                                "start_timestamp"]}
            },
    "015": {"name": "RestoreRDS",
            "api": "restores/aws/rds-resources",
            "header": "application/api.clumio.restored-aws-rds-resources=v1+json",
            "version": "v1",
            "body_parms": {"source_conditions": {"xor": ["backup","snapshot"], "and": []},
                           "source": [
                                {"backup":{
                                    "backup_id": None },
                                    "condition": {"xor": [], "and": ["backup_id"]},
                                },
                                {"snapshot":{
                                    "resource_id": None,
                                    "timestamp": None,
                                    "condition": {"xor": [], "and": ["resource_id","timestamp"]},
                                    }
                                }
                            ],
                           "target_conditions": {"xor": [], "and": ["environment_id","name"]},
                           "target": {
                                "environment_id": None,
                                "instance_class": None,
                                "is_publicly_accessible": False,
                                "kms_key_native_id": None,
                                "name": None,
                                "option_group_name": None,
                                "security_group_native_ids": [None],
                                "subnet_group_name": None,
                                "tags": [
                                    {
                                        "key": None,
                                        "value": None
                                    }
                                ]
                           }
            },
            "desc": "Restores the specified RDS resource backup or snapshot to the specified target destination",
            "type": "post",
            "success": 202,
            "query_parms": {
                "embed": [
                    "read-task"
                ]
            }
    },
    "109": {"name": "ManageAWS",
            "api": "none",
            "header": "none",
            "version": "v1",
            "desc": "manage AWS",
            "type": "get",
            "success": 200,
            },
    "999": {"api": "test002", "version": "v1", "desc": 'List EC2 instance backups', "type": "get", "success": 200,
            "payload": {"a": 73, "b": "bye", "theRitz": "Gazila"}},
}


class API:
    def __init__(self, id):
        self.id = id
        self.good = True
        self.error_msg = []
        self.name = api_dict.get(id, {}).get('api', None)
        self.version = api_dict.get(id, {}).get('version', None)
        self.type = api_dict.get(id, {}).get('type', None)
        self.pagnation = False
        self.debug = 0
        if api_dict.get(id, {}).get('success', None):
            self.success = api_dict.get(id, {}).get('success', None)
        else:
            if self.debug > 7: print(
                f"API init debug new APIs {id},  success {api_dict.get(id, {}).get('success', None)}")
            self.good = False

        self.url_prefix = "https://us-west-2.api.clumio.com/"
        self.header = {}
        self.payload = {}
        self.payload_flag = False
        self.body_parms_flag = False
        self.body_parms = {}
        self.token_flag = False
        self.token = None
        self.good = True
        self.type_get = False
        self.type_post = False
        self.current_count = 0
        self.total_count = 0
        self.total_pages_count = 0
        self.exec_error_flag = False
        self.aws_account_id = "080005437757"
        self.aws_account_id_flag = False
        self.aws_region = "us-east-1"
        self.aws_region_flag = False
        self.region_option = ["us-east-1", "us-west-2", "us-east-2", "us-west-1"]
        self.aws_tag_key = ""
        self.aws_tag_value = ""
        self.aws_tag_flag = False
        self.usage_type = "S3"
        self.aws_credentials = None
        self.aws_connect_good = False
        self.dump_to_file_flag = False
        self.dump_file_name = None
        self.dump_bucket = None
        self.aws_bucket_region = None
        self.file_iam_role = None
        self.import_bucket = None
        self.aws_import_bucket_region = None
        self.import_file_name = None
        self.import_file_flag = False
        self.import_data = {}
        self.type_post = False
        self.task_id = None
        self.task_id_flag = False
        self.pagination = False
        # #print("Hello world its me Dave")
        # Function to Create an AWS Session using IAM role

        if api_dict.get(id, {}).get('header', None):
            self.accept_api = api_dict.get(id, {}).get('header', None)
        else:
            if self.debug > 7: print(
                f"API init debug new APIs {id},  header {api_dict.get(id, {}).get('header', None)}")
            self.good = False
        # #print(f"DICT {api_dict.get(id, {}).get('type', None)}")
        if api_dict.get(id, {}).get('type', None):
            type = api_dict.get(id, {}).get('type', None)
            if type == 'get':
                self.type_get = True
                # #print("found get")
            elif type == 'post':
                self.type_post = True
            else:
                if self.debug > 7: print(
                    f"API init debug new APIs {id},  type {api_dict.get(id, {}).get('type', None)}")
                self.good = False
        else:
            if self.debug > 7: print(
                f"API init debug new APIs {id},  type {api_dict.get(id, {}).get('type', None)}")
            self.good = False
        if api_dict.get(id, {}).get('api', None):
            self.url = self.url_prefix + api_dict.get(id, {}).get('api', None)
            self.url_full = self.url
        else:
            self.good = False
        if api_dict.get(id, {}).get('body_parms', False):
            self.body_parms_flag = True
            self.body_parms = api_dict.get(id, {}).get('body_parms', {})
        else:
            self.payload_flag = False
            self.payload = {}
        if api_dict.get(id, {}).get('query_parms', False):
            self.query_parms_flag = True
            self.query_parms = api_dict.get(id, {}).get('query_parms', {})
        else:
            self.query_parms_flag = False
            self.query_parms = {}
        if api_dict.get(id, {}).get('body_parms', False):
            self.body_parms_flag = True
            self.body_parms = api_dict.get(id, {}).get('body_parms', {})
        else:
            self.body_parms_flag = False
            self.body_parms = {}
        if api_dict.get(id, {}).get('pathParms', False):
            self.pathParms_flag = True
            self.pathParms = api_dict.get(id, {}).get('query_parms', {})
        else:
            self.pathParms_flag = False
            self.pathParms = {}

    # Set debug Level
    def set_debug(self, value):
        try:
            self.debug = int(value)
            return True
        except ValueError:
            return False

    def set_error_msg(self,msg):
        self.error_msg.append(msg)

    def get_error_msg(self):
        return self.error_msg

    def check_tag_overlap(self,current,new):
        newer_keys = []
        for new_key in new:
            new_key_name = new_key['key']
            old_key_index = 0
            for old_key in current:
                old_key_name = old_key['key']
                if new_key_name == old_key_name:
                    current[old_key_index]['value'] = new_key['value']
                else:
                    newer_keys.append(new_key)

        if len(newer_keys) > 0:
            current.extend(newer_keys)
        return current

    def replace_region_options(self,region_list):
        if not region_list:
            return False
        self.region_option = region_list
        return self.region_option
    def get_task_id(self):
        if self.task_id_flag:
            return self.task_id
        else:
            return False

    # Function for user to setup Dump file to S3
    def setup_dump_file_s3(self, filename, bucket, prefix, role, aws_session, region="us-east-2"):
        self.usage_type = "S3"
        self.aws_bucket_region = region

        if self.set_dump_bucket(bucket):
            self.set_iam_file_role(role)

            if self.connect_aws(aws_session):

                s3FilePath = f"{prefix}/{filename}"
                if self.set_dump_file(s3FilePath, True):
                    self.dump_to_file_flag = True
                    return True
        if self.debug > 3: print(
            f"set_dump_fileS3 failed {filename}, {bucket}, {prefix}, {role}, {aws_session}, {region}")
        return False

    # Function to set Dump to File option
    def set_dump_file(self, filename, timestamp_flag=False):

        check_re = re.compile('[a-zA-Z0-9_/-]+$')

        if check_re.match(filename):

            # If use timestamp flag is passed add timestamp to file name
            if timestamp_flag:

                today = datetime.now().astimezone(timezone.utc)

                now_date_str = today.strftime('%Y%m%d%H%M%S')
                self.dump_file_name = f"{filename}-{now_date_str}.json"

            else:

                self.dump_file_name = f"{filename}.json"
            return True
        if self.debug > 3: print(f"set_dump_file failed {filename}, {timestamp_flag}")
        return False

    # Function to set Dump Bucket
    def set_dump_bucket(self, bucket):
        # Only allow lower case characters, numbers, -
        check_re = re.compile('[a-z0-9-]+$')

        if check_re.match(bucket):

            self.dump_bucket = bucket
            return True
        else:
            status_msg = f"failed {bucket} is invalid name format"
            self.error_msg = f"status {status_msg}"
            if self.debug > 3: print(f"set_dump_bucket failed {bucket}")
            return False

    def set_iam_file_role(self, role):
        self.file_iam_role = role

    # Function to clear Dump to File option
    def clear_dump_to_file(self):
        self.dump_to_file_flag = False

    def data_dump(self, dict_data):

        if self.usage_type == "S3" and self.aws_connect_good:

            json_data = json.dumps(dict_data, indent=2)
            s3_key = self.dump_file_name
            access_key_id = self.aws_credentials.get("access_key_id", None)
            secret_access_key = self.aws_credentials.get('secret_access_key', None)
            session_token = self.aws_credentials.get('session_token', None)
            region = self.aws_bucket_region
            try:
                aws_session = boto3.Session(
                    aws_access_key_id=access_key_id,
                    aws_secret_access_key=secret_access_key,
                    aws_session_token=session_token,
                    region_name=region
                )
            except ClientError as e:
                ERROR = e.response['Error']['Code']
                self.error_msg = "failed to initiate session {ERROR}"
                if self.debug > 3: print(f"data_dump failed in AWS Session cmd {dict_data}")
                return False
            s3_client = aws_session.client("s3")
            s3_resource = aws_session.resource('s3')
            data_encoded = json_data.encode('ascii')
            object1 = s3_resource.Object(self.dump_bucket, s3_key)
            try:
                result = object1.put(Body=data_encoded)
            except ClientError as e:
                ERROR = e.response['Error']['Code']
                status_msg = f"failed to write ClumioResourceList file {s3_key}"
                self.error_msg = f"status: {status_msg}, msg: {ERROR}"
                if self.debug > 3: print(f"data_dump failed in AWS s3 put cmd {dict_data}")
                return False
            return result
        else:
            self.error_msg = "status: not supporting other dump methods yet"
            return False

    # Function to Create an AWS Session using IAM role
    def connect_aws(self, session):

        if self.usage_type == "S3":

            # BOILER PLATE AWS CONNECT STUFF
            client = session.client('sts')
            if self.file_iam_role:

                role_arn = self.file_iam_role
                external_id = "clumio"
                sessionName = "mysesino"
                try:
                    response = client.assume_role(
                        role_arn=role_arn,
                        role_session_name=sessionName,
                        external_id=external_id
                    )
                except ClientError as e:
                    ERROR = e.response['Error']['Code']
                    status_msg = f"failed to assume role {role_arn}"
                    self.error_msg = f"status {status_msg} Error {ERROR}"
                    if self.debug > 3: print(f"connect_aws failed in AWS s3 assume role cmd {session}")
                    return False
                credentials = response.get('credentials', None)
                if not credentials == None:
                    # #print("in connect_aws 05")
                    access_key_id = credentials.get("access_key_id", None)
                    secret_access_key = credentials.get('secret_access_key', None)
                    session_token = credentials.get('session_token', None)
                    if not access_key_id == None and not secret_access_key == None and not session_token == None:
                        # #print(f"status: passed, access_key_id: {access_key_id}, secret_access_key: {secret_access_key},session_token: {session_token}")
                        self.aws_credentials = credentials
                        self.aws_connect_good = True
                        return True
                    else:
                        # #print("in connect_aws 05")
                        status_msg = "failed AccessKey or SecretKey or session_token are blank"
                        self.error_msg = f"status {status_msg}"
                        # #print(status_msg)
                        # return {"status": status_msg, "msg": ""}
                else:
                    # #print("in connect_aws 06")
                    status_msg = "failed Credentilas are blank"
                    self.error_msg = f"status {status_msg}"
            else:
                # #print("in connect_aws 07")
                status_msg = "failed Role is blank"
                self.error_msg = f"status {status_msg}"
        else:
            # #print("in connect_aws 08")
            status_msg = "Usage Type is not S3"
            self.error_msg = f"status {status_msg}"
        # #print("in connect_aws 09")
        self.aws_connect_good = False
        if self.debug > 3: print(f"connect_aws failed error_msg {self.error_msg}, {session}")
        return False
        # END OF BOILER PLATE

    def get_error(self):
        return self.error_msg

    def get_version(self):
        # #print(self.version,type(self.version))
        ##print("hi")
        return self.version

    def set_token(self, token):
        self.token = token
        self.token_flag = True
        bear = f"Bearer {self.token}"

        if self.good:
            if self.type_get:
                self.header = {"accept": self.accept_api, "authorization": bear}
            elif self.type_post:
                self.header = {"accept": self.accept_api, "content-type": "application/json", "authorization": bear}
            return self.header

    def set_url_prefix(self, url_prefix):
        self.url_full = self.url_full.replace(self.url_prefix, url_prefix)
        self.url = self.url.replace(self.url_prefix, url_prefix)
        self.url_prefix = url_prefix

    def set_url(self, suffix):
        # #print(f"hi in set {prefix}")
        if self.good:
            self.url_full = self.url + suffix
            # #print(self.url_full)

    def get_url(self):
        if self.good:
            return self.url_full
        else:
            return False

    def set_pagination(self):
        self.pagination = True

    def get_header(self):
        if self.good:
            return self.header
        else:
            return False

    def set_bad(self):
        self.good = False

    def set_get(self):

        self.type_get = True

    def set_post(self):

        self.type_post = True

    def set_aws_tag_key(self, aws_tag_key):
        self.aws_tag_key = aws_tag_key
        self.aws_tag_flag = True
        if self.debug > 5: print(f"set_aws_tag_key: value {aws_tag_key}")
        return True

    def clear_aws_tag(self):
        self.aws_tag_key = ""
        self.aws_tag_value = ""
        self.aws_tag_flag = False

    def set_aws_tag_value(self, aws_tag_value=None):
        self.aws_tag_value = aws_tag_value
        return True

    def set_aws_account_id(self, aws_account_id):
        try:
            int(aws_account_id)
            self.aws_account_id = aws_account_id
            self.aws_account_id_flag_flag = False
            return True
        except ValueError:
            return False

    def set_aws_region(self, aws_region):

        if aws_region in self.region_option:

            self.aws_region = aws_region
            self.aws_region_flag = True
            return True
        else:
            return False

    def exec_api(self):
        if self.debug > 7: print(f"exec_api: In Post {self.id},  type post {self.type_post} type get {self.type_get}")
        if self.type_get:

            if self.good and self.token_flag:

                url = self.get_url()
                header = self.get_header()
                if self.debug > 0: print(f"exec_api - url {url}")
                #if self.debug > 0: print(f"exec_api - header {header}")

                try:
                    response = requests.get(url, headers=header)
                except ClientError as e:
                    ERROR = e.response['Error']['Code']
                    status_msg = "failed to initiate session"
                    if self.debug > 3: print("exec_api failed in request")
                    return {"status": status_msg, "msg": ERROR}
                status = response.status_code

                response_text = response.text
                if self.debug > 1: print(f"exec_api - get request response {response_text}")
                response_dict = json.loads(response_text)

                if not status == self.success:
                    status_msg = f"API status {status}"
                    ERROR = response_dict.get('errors')
                    self.exec_error_flag = True
                    self.error_msg = f"status: {status_msg}, msg: {ERROR}"
                    if self.debug > 3: print(f"exec_api get request error resonse - {self.error_msg}")
                    self.good = False
                if self.pagination:
                    self.current_count = response_dict.get("current_count")
                    self.total_count = response_dict.get("total_count")
                    self.total_pages_count = response_dict.get("total_pages_count")
                    if self.debug > 1: print(
                        f"exec_api - pagination info current, total, total pages {self.current_count} {self.total_count} {self.total_pages_count}")
                return response_dict
        elif self.type_post:
            if self.debug > 7: print(f"exec_api: post Post  {id},  header {api_dict.get(id, {}).get('header', None)}")
            if self.good and self.token_flag and self.payload_flag:
                if self.debug > 7: print(f"exec_api: In Post {id},  header {api_dict.get(id, {}).get('header', None)}")
                url = self.get_url()
                header = self.get_header()
                payload = self.get_payload()
                if self.debug > 0: print(f"exec_api - url {url}")
                if self.debug > 0: print(f"exec_api - header {header}")
                if self.debug > 0: print(f"exec_api - payload {payload}")
                try:
                    response = requests.post(url, json=payload, headers=header)
                    if self.debug > 1: print(f"exec_api - response {response}")
                except ClientError as e:
                    error = e.response['Error']['Code']
                    status_msg = "failed to initiate session"
                    if self.debug > 3: print(f"exec_api post request failed - {error}")
                    return {"status": status_msg, "msg": error}
                status = response.status_code

                response_text = response.text
                if self.debug > 1: print(f"exec_api - request response {response_text}")
                response_dict = json.loads(response_text)
                self.task_id = response_dict.get("task_id", None)
                print(f"resposne {response_dict} task id {self.task_id}")
                if self.task_id:
                    self.task_id_flag = True
                else:
                    self.task_id_flag = False

                if not status == self.success:
                    status_msg = f"API status {status}"
                    ERROR = response_dict.get('errors')
                    self.exec_error_flag = True
                    self.error_msg = f"status: {status_msg}, msg: {ERROR}"
                    self.good = False
                    if self.debug > 3: print(f"exec_api post request response - {self.error_msg}")
                return response_dict
        else:
            self.good = False
            return False

    # Function to set Import Bucket
    def set_import_bucket(self, bucket):
        # Only allow lower case characters, numbers, -
        check_re = re.compile('[a-z0-9-]+$')
        if check_re.match(bucket):
            self.import_bucket = bucket
            return True
        else:
            status_msg = f"failed {bucket} is invalid name format"
            self.error_msg = f"status {status_msg}"
            # #print("in SetUploadBucket 03")
            return False

    def setup_import_file_s3(self, filename, bucket, prefix, role, aws_session, region="us-east-2"):
        self.usage_type = "S3"
        self.aws_bucket_region = region
        # print("in SetupFileS3 01")
        if self.set_import_bucket(bucket):
            # print("in SetupFileS3 02")
            self.set_iam_file_role(role)
            # #print("in setup_dump_file_s3 02")
            if self.connect_aws(aws_session):
                # print("in setup_dump_file_s3 03")
                s3FilePath = f"{prefix}/{filename}"
                if self.set_import_file(s3FilePath):
                    # print("in SetupFileS3 04")
                    return True
        return False

    # Function to set Upload from File option
    def set_import_file(self, filename):
        # print(f"set_import_file 01 {filename}")
        self.import_file_name = filename
        self.import_file_flag = True
        return True

        # Function to clear import File option

    def clear_import_file(self, filename):
        self.import_file_name = None
        self.import_file_flag = False

    def data_import(self):
        if self.usage_type == "S3" and self.import_file_flag and self.aws_connect_good:
            # json_data = json.dumps(dict_data, indent=2)
            s3_key = self.import_file_name
            access_key_id = self.aws_credentials.get("access_key_id", None)
            secret_access_key = self.aws_credentials.get('secret_access_key', None)
            session_token = self.aws_credentials.get('session_token', None)
            region = self.aws_bucket_region
            try:
                aws_session = boto3.Session(
                    aws_access_key_id=access_key_id,
                    aws_secret_access_key=secret_access_key,
                    aws_session_token=session_token,
                    region_name=region
                )
            except ClientError as e:
                ERROR = e.response['Error']['Code']
                self.error_msg = "failed to initiate session {ERROR}"
                # #print("in data_dump 03")
                return False
            s3_client = aws_session.client("s3")
            s3_resource = aws_session.resource('s3')

            try:

                obj3 = s3_resource.Object(self.import_bucket, s3_key)
                jsonFile3 = obj3.get()['Body'].read().decode('utf-8')
                self.import_data = json.loads(jsonFile3)
                # print(type(self.import_data), self.import_data)
            except ClientError as e:
                ERROR = e.response['Error']['Code']
                status_msg = f"failed read input file{s3_key}"
                # print(f"status: {status_msg}, msg: {ERROR}")
                self.error_msg = f"status: {status_msg}, msg: {ERROR}"
                return False

            return True
        else:
            self.error_msg = "status: not supporting other import methods yet"
            return False

    def clear_payload(self):
        self.payload = None
        self.payload_flag = False

    def get_payload(self):
        if self.payload_flag:
            return self.payload


class ClumioConnectAccount(API):
    def __init__(self):
        super(ClumioConnectAccount, self).__init__("008")
        self.id = "008"
        self.aws_account_to_connect = None
        self.master_aws_region = None
        self.master_aws_account_id = None
        self.aws_region_list_to_connect = []
        self.asset_types_to_enable = []
        self.aws_account_list_to_connect_flag = False
        self.master_aws_region_flag = False
        self.master_aws_account_id_flag = False
        self.aws_region_list_to_connect_flag = False
        self.asset_types_to_enable_flag = False
        self.good = True

        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def confirm_payload(self):
        if self.aws_account_list_to_connect_flag and self.master_aws_account_id_flag and self.master_aws_region_flag and self.aws_region_list_to_connect_flag and self.asset_types_to_enable_flag:
            self.payload_flag = True
            self.payload = {
                "account_native_id": self.aws_account_to_connect,
                "master_region": self.master_aws_region,
                "master_aws_account_id": self.master_aws_account_id,
                "aws_regions": self.aws_region_list_to_connect,
                "asset_types_enabled": self.asset_types_to_enable,
                "template_permission_set": "all"
            }
        else:
            self.payload_flag = False
        return self.payload_flag

    def set_account(self, account_id):
        self.aws_account_to_connect = account_id
        self.master_aws_account_id = account_id
        self.aws_account_list_to_connect_flag = True
        self.master_aws_account_id_flag = True
        self.confirm_payload()

    def set_regions(self, region_list):
        self.master_aws_region = region_list[0]
        self.aws_region_list_to_connect = region_list
        self.master_aws_region_flag = True
        self.aws_region_list_to_connect_flag = True
        self.confirm_payload()

    def set_aws_services(self, service_list):
        self.asset_types_to_enable = service_list
        self.asset_types_to_enable_flag = True
        self.confirm_payload()

    def test(self):
        self.aws_account_to_connect = "323724565630"
        self.master_aws_region = "us-east-2"
        self.master_aws_account_id = "323724565630"
        self.aws_region_list_to_connect = ["us-east-2"]
        self.asset_types_to_enable = ["S3"]
        self.aws_account_list_to_connect_flag = True
        self.master_aws_region_flag = True
        self.master_aws_account_id_flag = True
        self.aws_region_list_to_connect_flag = True
        self.asset_types_to_enable_flag = True

        self.payload = {
            "account_native_id": self.aws_account_to_connect,
            "master_region": self.master_aws_region,
            "master_aws_account_id": self.master_aws_account_id,
            "aws_regions": self.aws_region_list_to_connect,
            "asset_types_enabled": self.asset_types_to_enable,
            "template_permission_set": "all"
        }
        self.payload_flag = True
        result = self.exec_api()
        print(result)
        return result

    def run(self):
        if self.payload_flag:
            result = self.exec_api()
            # print(result)
            return result
        else:
            raise Exception("Payload not set to run connect API")

    def set_import_bucket(self, bucket):
        # Only allow lower case characters, numbers, -
        check_re = re.compile('[a-z0-9-]+$')
        if check_re.match(bucket):
            self.import_bucket = bucket
            return True
        else:
            status_msg = f"failed {bucket} is invalid name format"
            self.error_msg = f"status {status_msg}"
            # #print("in SetUploadBucket 03")
            return False

    def setup_import_file_s3(self, filename, bucket, prefix, role, aws_session, region="us-east-2"):
        self.usage_type = "S3"
        self.aws_bucket_region = region
        # print("in SetupFileS3 01")
        if self.set_import_bucket(bucket):
            # print("in SetupFileS3 02")
            self.set_iam_file_role(role)
            # #print("in setup_dump_file_s3 02")
            if self.connect_aws(aws_session):
                # print("in setup_dump_file_s3 03")
                s3FilePath = f"{prefix}/{filename}"
                if self.set_import_file(s3FilePath):
                    # print("in SetupFileS3 04")
                    return True
        return False

    # Function to set Upload from File option
    def set_import_file(self, filename):
        # print(f"set_import_file 01 {filename}")
        self.import_file_name = filename
        self.import_file_flag = True
        return True

        # Function to clear import File option

    def clear_import_file(self, filename):
        self.import_file_name = None
        self.import_file_flag = False

    def data_import(self):
        if self.usage_type == "S3" and self.import_file_flag and self.aws_connect_good:
            # json_data = json.dumps(dict_data, indent=2)
            s3_key = self.import_file_name
            access_key_id = self.aws_credentials.get("access_key_id", None)
            secret_access_key = self.aws_credentials.get('secret_access_key', None)
            session_token = self.aws_credentials.get('session_token', None)
            region = self.aws_bucket_region
            try:
                aws_session = boto3.Session(
                    aws_access_key_id=access_key_id,
                    aws_secret_access_key=secret_access_key,
                    aws_session_token=session_token,
                    region_name=region
                )
            except ClientError as e:
                ERROR = e.response['Error']['Code']
                self.error_msg = "failed to initiate session {ERROR}"
                # #print("in data_dump 03")
                return False
            s3_client = aws_session.client("s3")
            s3_resource = aws_session.resource('s3')

            try:

                obj3 = s3_resource.Object(self.import_bucket, s3_key)
                jsonFile3 = obj3.get()['Body'].read().decode('utf-8')
                self.import_data = json.loads(jsonFile3)
                # print(type(self.import_data), self.import_data)
            except ClientError as e:
                ERROR = e.response['Error']['Code']
                status_msg = f"failed read input file{s3_key}"
                # print(f"status: {status_msg}, msg: {ERROR}")
                self.error_msg = f"status: {status_msg}, msg: {ERROR}"
                return False

            return True
        else:
            self.error_msg = "status: not supporting other import methods yet"
            return False

    def clear_payload(self):
        self.payload = None
        self.payload_flag = False

    def get_payload(self):
        if self.payload_flag:
            return self.payload


class AWSOrgAccount(API):
    def __init__(self):
        super(AWSOrgAccount, self).__init__("109")
        self.id = "109"
        self.token = ''.join(random.choices(string.ascii_letters, k=7))
        self.rnd_string = ''.join(random.choices(string.ascii_letters, k=5))
        self.ou_assume_policy = 'arn:aws:iam::757214333202:policy/ou-assume-policy'
        self.ou_role_child_name = 'OrganizationAccountAccessRole'
        self.required_policy_access = 'AWSAdministratorAccess'
        self.ou_role_arn = 'arn:aws:iam::757214333202:role/ou-admin-role'
        self.reserve_ou = 'ou-s6m3-q82210z1'
        self.log_bucket = '757214333202-clumio-se-training-useast1757214333202'
        self.log_prefix_parquet = '/prefix/my_file.parquet'
        self.log_prefix_csv = '/prefix/my_file.csv'
        self.log_mode = 'csv'  # could also be 'parquet'
        self.log_path = None

    def set_ou_assume_policy_arn(self, policy_arn):
        self.ou_assume_policy = policy_arn
        return True

    def set_ou_role_arn(self, role_arn):
        self.ou_role_arn = role_arn
        return True
    def set_log_mode(self, mode):
        if mode == 'csv':
            self.log_mode = mode
        elif mode == 'parquet':
            self.log_mode = mode
        else:
            return False
        return True

    def get_rnd_string(self):
        return self.rnd_string

    def set_ou_reserve(self, ou):
        self.reserve_ou = ou
        return

    def get_aws_org_token(self):
        # return ou admin role arn
        return self.token

    def get_ou_admin_role(self):
        return self.ou_role_arn

    def parse_arn(self, arn):
        # Parse an AWS Org Account Arn and return pieces in python dict
        elements = arn.split(':', 5)
        result = {
            'arn': elements[0],
            'partition': elements[1],
            'service': elements[2],
            'region': elements[3],
            'account': elements[4],
            'resource': elements[5],
            'ou_id': None,
            'resource_type': None
        }
        if '/' in result['resource']:
            result['resource_type'], result['resource'] = result['resource'].split('/', 1)
            if '/' in result['resource']:
                result['ou_id'], result['resource'] = result['resource'].split('/', 1)
            # print(f"in a {result['resource_type']} {result['resource']}")
        elif ':' in result['resource']:
            result['resource_type'], result['resource'] = result['resource'].split(':', 1)
            # print(f"in b {result['resource_type']}")
        return result

    def connect_assume_role(self, current_session, role, _id):
        # Manages an AWS Assume role operation from the current session to the role specified
        if current_session == "boto3":
            # If calling function is launched from a compute element that has a existing role relationship
            client = boto3.client('sts')
        else:
            client = current_session.client('sts')
        if role:
            # print("in ConnectAWS 03")
            external_id = f"clumio{_id}"
            session_name = f"mysession{_id}"
            # AWS Assume Role API
            try:
                response = client.assume_role(
                    RoleArn=role,
                    RoleSessionName=session_name,
                    ExternalId=external_id
                )
            except ClientError as e:
                ERROR = e.response['Error']['Code']
                status_msg = f"failed to assume role {role}"
                error_msg = f"status {status_msg} Error {ERROR}"
                # print(f"in ConnectAWS 04 {error_msg}")
                return False, error_msg, None
            credentials = response.get('Credentials', None)
            if not credentials == None:
                # print("in ConnectAWS 05")
                access_key_id = credentials.get("AccessKeyId", None)
                secret_access_key = credentials.get('SecretAccessKey', None)
                session_token = credentials.get('SessionToken', None)
                # If assume role was successfull return the credentials
                # for the new session  ALL other outcomes are failures
                if not access_key_id == None and not secret_access_key == None and not session_token == None:
                    # print(f"status: passed, AccessKeyId: {access_key_id}, SecretAccessKey: {secret_access_key},SessionToken: {SessionToken}")
                    aws_connect_good = True
                    return True, "", credentials
                else:
                    # print("in ConnectAWS 05")
                    status_msg = "failed AccessKey or SecretKey or SessionToken are blank"
                    error_msg = f"status {status_msg}"
                    return False, error_msg, None
                    # print(status_msg)
                    # return {"status": status_msg, "msg": ""}
            else:
                # print("in ConnectAWS 06")
                status_msg = f"failed Credentilas are blank"
                error_msg = f"status {status_msg}"
                return False, error_msg, None
        else:
            # print("in ConnectAWS 07")
            status_msg = f"failed Role is blank"
            error_msg = f"status {status_msg}"
            return False, error_msg, None

    def check_for_accounts(self, current_session, region):
        ou_client = current_session.client('organizations')
        try:
            response = ou_client.list_roots()
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"lookup root ou {error}"
            return False, error_msg, 0, None, None
        root_ou = response.get('Roots', [])[0].get('Id', None)
        rsp = ou_client.list_accounts()
        accounts = rsp.get('Accounts', [])
        # status_types = ['ACTIVE', 'SUSPENDED', 'PENDING_CLOSURE']
        usable_accounts = []
        for account in accounts:
            account_arn = account.get('Arn', None)
            result = self.parse_arn(account_arn)
            account_status = account.get('Status', None)
            child_account_id = result.get('resource', None)
            if child_account_id and account_status == 'ACTIVE':
                ou_result = ou_client.list_parents(ChildId=child_account_id)
                print(child_account_id, ou_result.get('Parents', [])[0].get('Id', None), self.reserve_ou)
                aws_session2 = {}
                if ou_result.get('Parents', [])[0].get('Id', None) == self.reserve_ou:
                    [status, msg, cred2] = self.connect_assume_role(current_session,
                                                     f'arn:aws:iam::{child_account_id}:role/{self.ou_role_child_name}',
                                                     '')
                    if not status:
                        error_msg = f'Unable to connect to arn:aws:iam::{child_account_id}:role/{self.ou_role_child_name} - {msg}'
                        return False, error_msg, 0, None, None
                    print(child_account_id, ou_result.get('Parents', [])[0].get('Id', None), self.reserve_ou)
                    access_key_id = cred2.get("AccessKeyId", None)
                    secret_access_key = cred2.get('SecretAccessKey', None)
                    session_token = cred2.get('SessionToken', None)
                    # region = AWSDumpBucketRegion

                    try:
                        aws_session2 = boto3.Session(
                            aws_access_key_id=access_key_id,
                            aws_secret_access_key=secret_access_key,
                            aws_session_token=session_token,
                            region_name=region
                        )
                    except ClientError as e:
                        error = e.response['Error']['Code']
                        error_msg = f"failed to initiate session {error}"
                        return False, error_msg, 0, None, None
                    sts_client2 = aws_session2.client('sts')
                    try:
                        response2 = sts_client2.get_caller_identity()
                    except ClientError as e:
                        error = e.response['Error']['Code']
                        error_msg = f"failed to lookup account id {error}"
                        return False, error_msg, 0, None, None
                    # print(response2)
                    if response2.get('Account', None) == child_account_id:
                        # print(f"match & connectable {ou_result} {child_account_id} {account_status}")
                        usable_accounts.append(child_account_id)
                    else:
                        no_action = True
                        # print(f"match but not-connectable {ou_result} {child_account_id} {account_status}")

                else:
                    no_action = True
                    # print(f"no match {ou_result} {child_account_id} {account_status}")
            else:
                no_action = True
                # print(f"non-viable {child_account_id} {account_status}")
        # print(usable_accounts)
        random.shuffle(usable_accounts)
        return True, "", len(usable_accounts), root_ou, usable_accounts

    def confirm_ou_role(self, current_aws_session, account_id):
        iam_client = current_aws_session.client('iam')
        policy_arn = self.ou_assume_policy
        try:
            response = iam_client.list_policy_versions(
                PolicyArn=policy_arn
            )
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"failed to list policy {error}"
            if self.debug > 5: print(f"error: {error_msg}")
            return False, error_msg
        version_list = response.get('Versions', [])
        for version in version_list:
            if not version.get('IsDefaultVersion', False):
                ver_id = version.get('VersionId', None)
                print(f"deleting {ver_id}")
                try:
                    response = iam_client.delete_policy_version(
                        PolicyArn=policy_arn,
                        VersionId=ver_id
                    )
                except ClientError as e:
                    error = e.response['Error']['Code']
                    error_msg = f"failed to delete policy version(s) {error}"
                    if self.debug > 5: print(f"error: {error_msg}")
                    return False, error_msg
        try:
            response = iam_client.get_policy(
                PolicyArn=policy_arn
            )
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"failed to get policy {error}"
            if self.debug > 5: print(f"error: {error_msg}")
            return False, error_msg
        default_version = response.get('Policy', {}).get('DefaultVersionId', 'v1')
        try:
            old_policy_info = iam_client.get_policy_version(
                PolicyArn=policy_arn,
                VersionId=default_version
            )
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"failed to get policy version {error}"
            if self.debug > 5: print(f"error: {error_msg}")
            return False, error_msg
        policy_doc = old_policy_info.get('PolicyVersion', {}).get('Document', {})
        print(policy_doc)
        new_principal = f'arn:aws:iam::{account_id}:role/{self.ou_role_child_name}'
        if not new_principal in policy_doc.get('Statement', {}).get('Resource', []):
            policy_doc['Statement']['Resource'].append(new_principal)
        print(policy_doc)
        policy_doc_json = json.dumps(policy_doc)
        try:
            response = iam_client.create_policy_version(
                PolicyArn=policy_arn,
                PolicyDocument=policy_doc_json,
                SetAsDefault=True
            )
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"failed to apply updated policy {error}"
            if self.debug > 5: print(f"error: {error_msg}")
            return False, error_msg
        time.sleep(5)
        return True, None

    def create_new_ou(self, current_aws_session, customer, root_ou):
        ou_client = current_aws_session.client('organizations')
        try:
            new_ou = f"lab-{customer}-{self.rnd_string}"
            response = ou_client.create_organizational_unit(
                ParentId=root_ou,
                Name=new_ou,
                Tags=[
                    {
                        'Key': 'Customer',
                        'Value': customer
                    },
                ]
            )
        except ClientError as e:
            ERROR = e.response['Error']['Code']
            error_msg = "failed to create ou"
            raise Exception(error_msg.format(ERROR=ERROR))
        # print(response)
        new_ou = response.get('OrganizationalUnit', {}).get('Id', None)
        return new_ou

    def create_account(self, current_aws_session, master_user):
        ou_client = current_aws_session.client('organizations')
        try:
            new_account_name = f'lab-{self.rnd_string}'
            role = self.ou_role_child_name
            response = ou_client.create_account(
                Email=master_user,
                AccountName=new_account_name,
                RoleName=role,
                IamUserAccessToBilling='DENY',
            )
        except ClientError as e:
            ERROR = e.response['Error']['Code']
            error_msg = "failed to create account"
            # print("in data_dump 03")
            raise Exception(error_msg.format(ERROR=ERROR))
        # print(response)
        create_id = response.get('CreateAccountStatus', {}).get('Id', None)
        final_status = ['SUCCEEDED', 'FAILED']
        try:
            response = ou_client.describe_create_account_status(
                CreateAccountRequestId=create_id
            )
        except ClientError as e:
            ERROR = e.response['Error']['Code']
            error_msg = "failed to get create account status"
            # print("in data_dump 03")
            raise Exception(error_msg.format(ERROR=ERROR))
        # print(response)
        create_status = response.get('CreateAccountStatus', {}).get('State', None)
        # running_status = ['IN_PROGRESS']
        iter_count = 0
        while not create_status in final_status:
            time.sleep(5)
            try:
                response = ou_client.describe_create_account_status(
                    CreateAccountRequestId=create_id
                )
            except ClientError as e:
                ERROR = e.response['Error']['Code']
                error_msg = "failed to get create account status"
                # print("in data_dump 03")
                raise Exception(error_msg.format(ERROR=ERROR))
            # print(response)
            create_status = response.get('CreateAccountStatus', {}).get('State', None)
            iter_count += 1
            if iter_count > 100:
                print("loser")
                break
            time.sleep(5)
        state = response.get("CreateAccountStatus", {}).get("State", None)
        if state == "SUCCEEDED":
            account_id = response.get("CreateAccountStatus", {}).get("AccountId", None)
            try:
                response = ou_client.list_parents(
                    ChildId=account_id,
                )
            except ClientError as e:
                ERROR = e.response['Error']['Code']
                error_msg = "failed to list parent OU"
                # print("in data_dump 03")
                raise Exception(error_msg.format(ERROR=ERROR))
            # print(response)
            parent_ou = response.get('Parents', [])[0].get('Id', None)
            return account_id, parent_ou
        else:
            try:
                int("eleven")
            except ValueError as e:
                errmsg = "Was not able to create an account."
                e.args += (errmsg,)
                raise e
        return False

    def account_prep(self, current_aws_session, account_id, new_ou, user):
        ou_client = current_aws_session.client('organizations')
        try:
            response = ou_client.move_account(
                AccountId=account_id,
                SourceParentId=self.reserve_ou,
                DestinationParentId=new_ou
            )
        except ClientError as e:
            ERROR = e.response['Error']['Code']
            error_msg = "failed to move account to new OU"
            # print("in data_dump 03")
            raise Exception(error_msg.format(ERROR=ERROR))
        idc_client = current_aws_session.client('sso-admin', region_name='us-east-2')
        ids_client = current_aws_session.client('identitystore', region_name='us-east-2')
        # response = idc_client.list_instances()
        idc_instance_arn = idc_client.list_instances().get('Instances', [])[0].get('InstanceArn', None)
        ids_id = idc_client.list_instances().get('Instances', [])[0].get('IdentityStoreId', None)
        print(idc_instance_arn, ids_id)

        permission_set_list = idc_client.list_permission_sets(InstanceArn=idc_instance_arn).get('PermissionSets', [])
        admin_permission_set_arn = ''
        for perm in permission_set_list:
            response = idc_client.describe_permission_set(
                InstanceArn=idc_instance_arn,
                PermissionSetArn=perm
            )
            if response.get('PermissionSet', {}).get('Name', None) == self.required_policy_access:
                admin_permission_set_arn = perm
                break
            print(response)
        print(admin_permission_set_arn)

        principal_id = ids_client.list_users(
            IdentityStoreId=ids_id,
            Filters=[
                {
                    'AttributePath': 'UserName',
                    'AttributeValue': user
                },
            ]
        ).get('Users', [])[0].get('UserId', None)
        print(principal_id)
        response = idc_client.create_account_assignment(
            InstanceArn=idc_instance_arn,
            PermissionSetArn=admin_permission_set_arn,
            PrincipalId=principal_id,
            PrincipalType='USER',
            TargetId=account_id,
            TargetType='AWS_ACCOUNT'
        )
        print(response)
        return True

    def run_clumio_deploy_stack(self, current_aws_session, child_account_id, region, url, token, id):
        print(f"in deploy clumio stack")
        deployment_template_url = url
        clumio_token = token
        external_id = id
        role = f'arn:aws:iam::{child_account_id}:role/{self.ou_role_child_name}'
        [status, msg, cred2] = self.connect_assume_role(current_aws_session,
                                         role, 'x')
        if not status:
            print(f"connect failed to {role}")
            return False, msg
        access_key_id = cred2.get("AccessKeyId", None)
        secret_access_key = cred2.get('SecretAccessKey', None)
        session_token = cred2.get('SessionToken', None)
        try:
            new_aws_session = boto3.Session(
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                aws_session_token=session_token,
                region_name=region
            )
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"failed to initiate session {error}"
            # print("in data_dump 03")
            return False, error_msg
        cft_client = new_aws_session.client('cloudformation')
        # response = cft_client.validate_template(
        #    TemplateURL=deployment_template_url
        # )
        # print(f"check clumio deploy url template {response}")
        try:
            deploy_rsp = cft_client.create_stack(
                StackName=f'clumiodeploy-{self.rnd_string}',
                TemplateURL=deployment_template_url,
                Parameters=[
                    {
                        'ParameterKey': 'ClumioToken',
                        'ParameterValue': clumio_token
                    },
                    {
                        'ParameterKey': 'RoleExternalId',
                        'ParameterValue': external_id
                    },
                    {
                        'ParameterKey': 'PermissionModel',
                        'ParameterValue': 'SELF_MANAGED'
                    },
                    {
                        'ParameterKey': 'PermissionsBoundaryARN',
                        'ParameterValue': ''
                    },
                ],
                Capabilities=[
                    'CAPABILITY_NAMED_IAM'
                ],
                DisableRollback=True,
                TimeoutInMinutes=60,
            )
            print(f"deploy_status: {deploy_rsp}")
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"failed to deploy stack {error}"
            if self.debug > 5: print(f"error: {error_msg}")
            return False, error_msg
        return True, ""

    def run_other_deploy_stack(self, current_aws_session, child_account_id, region, url, parameters):
        deployment_template_url = url
        [status, msg, cred2] = self.connect_assume_role(current_aws_session,
                                         f'arn:aws:iam::{child_account_id}:role/{self.ou_role_child_name}', '')
        if not status:
            return False, msg
            # S3Key = DumpFileName
        access_key_id = cred2.get("AccessKeyId", None)
        secret_access_key = cred2.get('SecretAccessKey', None)
        session_token = cred2.get('SessionToken', None)

        try:
            new_aws_session = boto3.Session(
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                aws_session_token=session_token,
                region_name=region
            )
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"failed to initiate session {error}"
            # print("in data_dump 03")
            return False, error_msg
        cft_client = new_aws_session.client('cloudformation')
        try:
            deploy_rsp = cft_client.create_stack(
                StackName=f'aws-resource-deploy-{self.rnd_string}',
                TemplateURL=deployment_template_url,
                Parameters=parameters,
                Capabilities=[
                    'CAPABILITY_NAMED_IAM'
                ],
                DisableRollback=True,
                TimeoutInMinutes=60,
            )
            print(f"deploy_status: {deploy_rsp}")
        except ClientError as e:
            error = e.response['Error']['Code']
            error_msg = f"failed to deploy stack {error}"
            if self.debug > 5: print(f"error: {error_msg}")
            return False, error_msg
        return True, ""

    def set_log_bucket(self, bucket_name):
        self.log_bucket = bucket_name
        return True

    def set_log_prefix(self, prefix):
        self.log_prefix = prefix
        return True


class ListEC2Instance(API):
    def __init__(self):
        super(ListEC2Instance, self).__init__("006")
        self.id = "006"
        self.filter_list = []
        self.filter_expression_string = ""
        self.filter_expression_string_encoded = ""
        self.limit_expression_string = ""
        self.start_expression_string = ""

        self.limit_flag = False
        self.start_flag = False
        self.limit = 100
        self.start = 1
        self.url_suffix_list = []

        self.url_suffix_string = ""
        self.clumio_org_id = None
        self.clumio_org_id_flag = False

        self.set_pagination()
        self.aws_vpc_id = None
        self.aws_vpc_id_flag = False
        self.aws_subnet_id = None
        self.aws_subnet_id_flag = False
        self.retention_units = None
        self.retention_value = 0
        self.retention_flag = False
        self.clumio_environment_id = None
        self.clumio_environment_id_flag = False
        self.search_filter_flag = False
        self.search_filters = [
            {"name": "environment_id", "value": None, "type": None, "flag": False},
            {"name": "name", "value": None, "type": None, "flag": False},
            {"name": "instance_native_id", "value": None, "type": None, "flag": False},
            {"name": "account_native_id", "value": None, "type": None, "flag": False},
            {"name": "compliance_status", "value": None, "type": None, "flag": False},
            {"name": "protection_status", "value": None, "type": None, "flag": False},
            {"name": "protection_info.policy_id", "value": None, "type": None, "flag": False},
            {"name": "tags.id", "value": None, "type": None, "flag": False},
            {"name": "is_deleted", "value": None, "type": None, "flag": False},
            {"name": "availability_zone", "value": None, "type": None, "flag": False}
        ]

        self.ec2_instance_dict = {}
        self.set_pagination()
        # print(f"RestoreEC2 why api_dict {api_dict.get(self.id, {}).get('type', None)}")
        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def run(self):
        error_msg = None
        if self.debug > 5: print("run_all: run api ")
        if self.search_filter_flag:

            if self.set_filters():
                pass
            else:
                if self.debug > 1: print("run_all: run_all failed to set filters ")
                error_msg = "run:  failed to set filters"
                return False, 0, error_msg
        self.results = []
        first_results = self.exec_api()
        if self.good:
            items = first_results.get("_embedded", {}).get("items", {})
            for i in items:
                check = self.pass_check(i)
                if check:
                    self.ec2_instance_dict[check] = i
        else:
            error_msg = "run: api in error state"
            return False, 0, error_msg
        if self.total_pages_count:
            if self.debug > 5: print(f"list_ec2_instance run: total pages found {self.total_pages_count}")
            if self.total_pages_count > 1:
                for i in range(2, self.total_pages_count):
                    if self.debug > 5: print(f"list_ec2_instance run: processing page {i}")
                    self.set_page_start(i)
                    aws_region = self.exec_api()
                    items = aws_region.get("_embedded", {}).get("items", {})
                    for i in items:
                        check = self.pass_check(i)
                        if check:
                            self.ec2_instance_dict[check] = i
        return True, len(self.ec2_instance_dict), None

    def pass_check(self, response):

        if self.aws_tag_flag:
            tags = response.get("tags", None)
            found_tag = False
            for tag in tags:
                if self.aws_tag_value:
                    if tag.get("key", None) == self.aws_tag_key and tag.get("value", None) == self.aws_tag_value:
                        found_tag = True
                else:  # Only tag key set not tag value
                    if tag.get("key", None) == self.aws_tag_key:
                        found_tag = True
            if not found_tag:
                if self.debug > 5: print(
                    f"pass_check: search tag set {self.aws_tag_key} {self.aws_tag_value} but no match in {tags}")
                return False
        if self.clumio_org_id_flag:
            if self.clumio_org_id == response.get("organizational_unit_id", None):
                pass
            else:
                if self.debug > 5: print(
                    f"pass_check: search org id {self.clumio_org_id} does not match {response.get("organizational_unit_id", None)} ")
                return False
        if self.aws_vpc_id_flag:
            if self.aws_vpc_id == response.get("vpc_id", None):
                pass
            else:
                if self.debug > 5: print(
                    f"pass_check: search vpc id {self.aws_vpc_id} does not match {response.get("vpc_id", None)} ")
                return False
        if self.aws_subnet_id_flag:
            if self.aws_subnet_id == response.get("subnet_id", None):
                pass
            else:
                if self.debug > 5: print(
                    f"pass_check: search vpc id {self.aws_subnet_id} does not match {response.get("subnet_id", None)} ")
                return False
        clumio_instance_id = response.get("id", None)
        return clumio_instance_id

    def list_ec2_info(self, list_type):

        records = []
        if list_type == "id":
            for inst in self.ec2_instance_dict.keys():
                rec = {"id_record": [self.ec2_instance_dict[inst].get("instance_native_id", None), inst]}
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:

                if not self.data_dump(clumio_resource_list_dict):
                    return False
            if self.debug > 5: print(f"list_ec2_info: record type {list_type} record {clumio_resource_list_dict} ")
            return clumio_resource_list_dict
        elif list_type == "all":
            for inst in self.ec2_instance_dict.keys():
                rec = {"item": self.ec2_instance_dict[inst]}
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:

                if not self.data_dump(clumio_resource_list_dict):
                    return False
            if self.debug > 5: print(f"list_ec2_info: record type {list_type} record {clumio_resource_list_dict} ")
            return clumio_resource_list_dict
        elif list_type == "BACKUP":

            for inst in self.ec2_instance_dict.keys():
                rec = {"instance_id": inst}
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:

                if not self.data_dump(clumio_resource_list_dict):
                    return False
            if self.debug > 5: print(f"list_ec2_info: record type {list_type} record {clumio_resource_list_dict} ")
            return clumio_resource_list_dict
        if self.debug > 5: print(f"list_ec2_info: unrecognized record type {list_type}")
        return {}

    def ec2_search_by_tag(self, Key, Value):
        self.set_aws_tag_key(Key)
        self.set_aws_tag_value(Value)

    def set_page_size(self, x):
        if self.query_parms_flag:
            if "limit" in self.query_parms.keys():
                self.limit = x
                self.limit_expression_string = "limit=" + str(self.limit)
                self.limit_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def set_page_start(self, x):
        if self.query_parms_flag:
            if "start" in self.query_parms.keys():
                self.start = x
                self.start_expression_string = "start=" + str(self.start)
                self.start_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def build_url_suffix(self):
        self.url_suffix_list = []
        if self.good:
            if self.search_filter_flag:
                self.url_suffix_list.append(self.filter_expression_string_encoded)

            if self.limit_flag:
                self.url_suffix_list.append(self.limit_expression_string)

            if self.start_flag:
                self.url_suffix_list.append(self.start_expression_string)

            self.url_suffix_string = "?" + "&".join(self.url_suffix_list)
            String = self.url_suffix_string
            # #print(String)
            self.set_url(String)
            return self.url_suffix_string

    def set_filters(self):
        filter_expression_dict = {}
        filter_changed = False
        if self.query_parms_flag:
            for filter_item in self.search_filters:
                if filter_item.get("flag", False):
                    if filter_item.get("name", None) in self.query_parms.get("filter", {}).keys():

                        if filter_item.get("type", None) in self.query_parms.get("filter", {}).get(
                                filter_item.get("name", None), []):
                            # the $in type conditional value is expecting a list not a single value
                            if filter_item.get("type", None) == "$in":
                                self.filter_list.append([filter_item.get("name", None), filter_item.get("type", None),
                                                         [filter_item.get("value", None)]])
                            else:
                                self.filter_list.append([filter_item.get("name", None), filter_item.get("type", None),
                                                         filter_item.get("value", None)])
                            if self.debug > 5: print(f"set_filter: found new filter {filter_item}")
                            filter_changed = True
                        else:
                            if self.debug > 2: print(f"set_filter: Failure non valid filter condition {filter_item}")
                            return False
                    else:
                        if self.debug > 2: print(f"set_filter: Failure non valid filter name {filter_item}")
                        return False
                else:
                    if self.debug > 5: print(f"set_filter: Info filter value not set {filter_item}")
            if filter_changed:
                for i in self.filter_list:
                    filter_expression_dict[i[0]] = {i[1]: i[2]}
                    self.filter_expression_string = "filter=" + json.dumps(filter_expression_dict,
                                                                           separators=(',', ':'))
                    self.filter_expression_string_encoded = "filter=" + urllib.parse.quote(
                        json.dumps(filter_expression_dict, separators=(',', ':')))
                    self.search_filter_flag = True
            if self.debug > 5: print(
                f"set_filter: filter dict unencoded {self.filter_expression_string} encoded {self.filter_expression_string_encoded}")
            return self.build_url_suffix()
        else:
            if self.debug > 2: print("set_filter: Failure API info incorrect")
            return False

    def find_environment_id(self):
        # if search for both an AWS account id and an AWS region is set find and use a Clumio environment id filter
        env_id_api = EnvironmentId()
        env_id_api.set_url_prefix(self.url_prefix)
        env_id_api.set_token(self.token)
        env_id_api.set_search_account_id(self.aws_account_id)
        env_id_api.set_search_region(self.aws_region)
        if env_id_api.run_api():
            rsp = env_id_api.environment_id_parse_results()
            del env_id_api
            if rsp:
                self.clumio_environment_id = rsp
                self.clumio_environment_id_flag = True
                count = 0
                found = False
                for search_item in self.search_filters:
                    if search_item.get("name") == "environment_id":
                        self.search_filters[count] = {"name": "environment_id", "value": self.clumio_environment_id,
                                                      "type": "$eq", "flag": True}
                        found = True
                    count += 1
                if not found:
                    if self.debug > 2: print(f"find_environment_id: Failure to match search filter {rsp}")
                    return False
                return True
        return False

    def set_search_aws_account_id(self, value, condition="$eq"):
        # Set search for aws account
        if self.set_aws_account_id(value):
            # if region flag is set then we can set, use environment id search
            if self.aws_region_flag:
                if self.find_environment_id():
                    pass
            self.search_filter_flag = True
            return True
        else:
            if self.debug > 2: print(f"set_search_aws_account_id: Failure to set account id {value}")
            return False

    def set_search_aws_region(self, value, condition="$eq"):
        # Set search for aws region
        if self.set_aws_region(value):
            # if aws account id flag is set then we can set, use environment id search
            if self.aws_account_id_flag:
                if self.find_environment_id():
                    pass
            self.search_filter_flag = True
            return True
        else:
            if self.debug > 2: print(f"set_search_aws_region: Failure to set aws region {value}")
            return False

    def set_search_compliance_status(self, value, condition="$eq"):
        # Set search filter for protection_status: allowed values are "compliant" and "non_compliant"
        valid_values = ["compliant", "non_compliant"]
        if not condition in valid_values:
            if self.debug > 2: print(f"set_search_compliance_status: value not valid {value}")
            return False
        count = 0
        found = False
        for search_item in self.search_filters:
            if search_item.get("name") == "compliance_status":
                self.search_filters[count] = {"name": "compliance_status", "value": value, "type": "$eq", "flag": True}
                found = True
            count += 1
        # if not match to filter set was found - error
        if not found:
            if self.debug > 2: print(f"set_search_compliance_status: Failure to match search filter {value}")
            return False
        self.search_filter_flag = True
        return True

    def set_search_protection_status(self, value, condition="$in"):
        # Set search filter for compliance status: allowed values are "protected", "unprotected", and "unsupported"
        valid_values = ["protected", "unprotected", "unsupported"]
        if not condition in valid_values:
            if self.debug > 2: print(f"set_search_protection_status: value not valid {value}")
            return False
        count = 0
        found = False
        for search_item in self.search_filters:
            if search_item.get("name") == "protection_status":
                self.search_filters[count] = {"name": "protection_status", "value": value, "type": "$in", "flag": True}
                found = True
            count += 1
        # if not match to filter set was found - error
        if not found:
            if self.debug > 2: print(f"set_search_protection_status: Failure to match search filter {value}")
            return False
        self.search_filter_flag = True
        return True

    def set_search_protection_info_policy_id(self, value, condition="$eq"):
        # Set search filter for protection_info_policy_id: allowed values are clumio policy id
        # NOT IMPLEMENTED
        return False

    def set_search_tags_id(self, value, condition="$eq"):
        # Set search filter for tags.id: allowed values are clumio tag id
        # NOT IMPLEMENTED
        return False

    def set_search_is_deleted(self, value, condition="$eq"):
        # Set search filter for deleted instances: allowed values are true and false
        valid_values = ["true", "false"]
        if not value in valid_values:
            if self.debug > 2: print(f"set_search_is_deleted: value not valid {value}")
            return False
        count = 0
        found = False
        for search_item in self.search_filters:
            if search_item.get("name") == "is_deleted":
                self.search_filters[count] = {"name": "is_deleted", "value": value, "type": "$eq", "flag": True}
                found = True
            count += 1
        # if not match to filter set was found - error
        if not found:
            if self.debug > 2: print(f"set_search_is_deleted: Failure to match search filter {value}")
            return False
        self.search_filter_flag = True
        return True

    def set_search_availability_zone(self, value, condition="$eq"):
        # Set search filter for AWS availability zone
        count = 0
        found = False
        for search_item in self.search_filters:
            if search_item.get("name") == "availability_zone":
                self.search_filters[count] = {"name": "availability_zone", "value": value, "type": "$eq", "flag": True}
                found = True
            count += 1
        # if not match to filter set was found - error
        if not found:
            if self.debug > 2: print(f"set_search_availability_zone: Failure to match search filter {value}")
            return False
        # if aws account id flag is set then we can set, use environment id search
        self.search_filter_flag = True
        region = value.rstrip("abcdefg")
        if self.set_search_aws_region(region):
            return True
        else:
            if self.debug > 2: print(f"set_search_availability_zone: Failure to set aws region {region}")
            return False

    def set_search_aws_id(self, value, condition="$eq"):
        # Set search filter for instance name
        valid_conditions = ["$eq"]
        if not condition in valid_conditions:
            if self.debug > 2: print(f"set_search_aws_id: type not valid {condition}")
            return False
        count = 0
        found = False
        for search_item in self.search_filters:
            print(f"found row {count} and value {search_item}")
            if search_item.get("name") == "instance_native_id":
                print(f"found row {count} and value {search_item}")
                self.search_filters[count] = {"name": "instance_native_id", "value": value, "type": condition,
                                              "flag": True}
                found = True
            count += 1
        # if not match to filter set was found - error
        if not found:
            if self.debug > 2: print(f"set_search_name: Failure to match search filter {value}")
            return False
        self.search_filter_flag = True
        return True

    def set_search_name(self, value, condition="$eq"):
        # Set search filter for instance name
        valid_conditions = ["$eq", "$contains"]
        if not condition in valid_conditions:
            if self.debug > 2: print(f"set_search_name: type not valid {condition}")
            return False
        count = 0
        found = False
        for search_item in self.search_filters:
            if search_item.get("name") == "name":
                self.search_filters[count] = {"name": "name", "value": value, "type": condition, "flag": True}
                found = True
            count += 1
        # if not match to filter set was found - error
        if not found:
            if self.debug > 2: print(f"set_search_name: Failure to match search filter {value}")
            return False
        self.search_filter_flag = True
        return True

    def set_search_aws_tag(self, key, value=None):
        if self.debug > 5: print(f"set_search_aws_tag: key {key}, and value {value}")
        if self.set_aws_tag_key(key) and self.set_aws_tag_value(value):
            return True
        else:
            return False

    def set_search_clumio_org_id(self, value):
        # NEED TO ADD CHECK FOR VALID ORG ID - Not implemented yet

        self.clumio_org_id = value
        self.clumio_org_id_flag = True
        return True

    def set_search_vpc_id(self, value):

        self.aws_vpc_id = value
        self.aws_vpc_id_flag = True
        return True

    def set_search_subnet_id(self, value):

        self.aws_subnet_id = value
        self.aws_subnet_id_flag = True
        return True

    def set_retention(self, units, value):
        valid_units = ["hours", "days", "weeks", "months", "years"]
        if units not in valid_units:
            try:
                value = int(value)
            except ValueError:
                if self.debug > 2: print(f"set_retention: value is not an integer {value}")
                return False
        self.retention_units = units
        self.retention_value = value
        self.retention_flag = True
        return True


class EnvironmentId(API):
    def __init__(self):
        super(EnvironmentId, self).__init__("002")
        self.id = "002"
        self.filter_list = []
        # self.filter_expression = {}
        self.filter_expression_string = ""
        self.filter_expression_string_enc = ""
        self.limit_expression_string = ""
        self.start_expression_string = ""
        self.embed_expression_string = ""
        self.filter_flag = False
        self.limit_flag = False
        self.start_flag = False
        # self.sort_flag = False
        self.embed_flag = False
        self.limit = 100
        self.start = 1
        self.url_suffix_list = []
        self.url_suffix_list_enc = []
        self.url_suffixString = ""
        self.url_suffixStringEnc = ""
        self.type_get = True
        self.type_post = False
        self.set_pagination()
        self.good_run_flag = True
        self.filter_expression = None
        self.search_service_flag = False
        self.search_service = None
        self.search_account_id_flag = False
        self.search_account_id = None
        self.search_status_flag = False
        self.search_region_flag = False
        self.search_region = None
        self.environment_id_dict = {}
        self.sort_expression_string = ""
        # self.set_pagination()

        # print(f"RestoreEC2 why api_dict {api_dict.get(self.id, {}).get('type', None)}")
        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()
        # print(f"type of REST get {self.type_get} post {self.type_post}")

    def run_api(self):
        if self.search_account_id_flag:
            # print(f"set account {self.search_account_id}")
            self.set_filter_env("account_native_id", "$eq", self.search_account_id)
            # print(f"set account {self.search_account_id}")
        if self.search_service_flag:
            self.set_filter_env("services_enabled", "$contains", self.search_service)
        if self.search_region_flag:
            # print(f"set account{self.search_region}")
            self.set_filter_env("aws_region", "$eq", self.search_region)
            # print(f"set account{self.search_region}")
        self.results = []
        first_results = self.exec_api()
        # print(f"run {first_results}")
        # print("Life of Brian",first_results)
        if self.good:
            items = first_results.get("_embedded", {}).get("items", {})
            for i in items:
                check = self.pass_check(i)
                if check:
                    self.environment_id_dict[check] = i
        else:
            return False
        if self.total_pages_count:
            if self.total_pages_count > 1:
                for i in range(2, self.total_pages_count):
                    self.set_page_start(i)
                    aws_region = self.exec_api()
                    if self.good:
                        items = aws_region.get("_embedded", {}).get("items", {})
                        for i in items:
                            check = self.pass_check(i)
                            if check:
                                self.environment_id_dict[check] = i
                    else:
                        return False
                # #print(f"read page {i}")
        return len(self.environment_id_dict)

    def pass_check(self, response):
        environment_id = response.get("id", None)
        if environment_id:
            return environment_id
        else:
            return False

    def environment_id_parse_results(self, parse_type="id"):
        example_parms_list = [
            "ID :backup_ids & InstanceIds & TimeStamp",
            "Restore :Parameters Needed for EC2 Restore",
            "Count : Number of Instances",
            "All : All Data"
        ]
        records = []
        # print(self.environment_id_dict)
        if parse_type == "id":
            if self.environment_id_dict:
                if len(self.environment_id_dict.keys()) > 1:
                    return False
                else:
                    for key in self.environment_id_dict.keys():
                        return key
            else:
                return False
        elif parse_type == "all":
            if self.environment_id_dict:
                return self.environment_id_dict
            else:
                return False
        return False

    def set_page_size(self, x):
        if self.query_parms_flag:
            if "limit" in self.query_parms.keys():
                self.limit = x
                self.limit_expression_string = "limit=" + str(self.limit)
                self.limit_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def set_page_start(self, x):
        if self.query_parms_flag:
            if "start" in self.query_parms.keys():
                self.start = x
                self.start_expression_string = "start=" + str(self.start)
                self.start_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def build_url_suffix(self):
        self.url_suffix_list = []
        if self.good:
            if self.filter_flag:
                self.url_suffix_list.append(self.filter_expression_string_enc)

            if self.embed_flag:
                self.url_suffix_list.append(self.sort_expression_string)

            if self.limit_flag:
                self.url_suffix_list.append(self.limit_expression_string)

            if self.start_flag:
                self.url_suffix_list.append(self.start_expression_string)

            self.url_suffix_string = "?" + "&".join(self.url_suffix_list)
            String = self.url_suffix_string
            # #print(String)
            self.set_url(String)
            return self.url_suffix_string

    def set_filter_env(self, filter_name_env, filter_expression_env, filter_value_env):
        filter_expression_dict = {}
        # print(f"in set_filter 01 {filter_name_env} {filter_expression_env} {filter_value_env}")
        if self.query_parms_flag:
            # print("in set_filter 02")
            if filter_name_env in self.query_parms.get("filter", {}).keys():
                # print("in set_filter 03")
                if filter_expression_env in self.query_parms.get("filter", {}).get(filter_name_env, []):
                    # print(f"in set_filter 04 {self.filter_list}")
                    self.filter_list.append([filter_name_env, filter_expression_env, filter_value_env])
                    # print(f"in set_filter 04 {self.filter_list}")

                    for i in self.filter_list:
                        # print(f"in set_filter 05 {i}")
                        filter_expression_dict[i[0]] = {i[1]: i[2]}
                        # print(f"in setFilter 07 {filter_expression_dict}")
                    self.filter_expression_string = "filter=" + json.dumps(filter_expression_dict,
                                                                           separators=(',', ':'))
                    self.filter_expression_string_enc = "filter=" + urllib.parse.quote(
                        json.dumps(filter_expression_dict, separators=(',', ':')))
                    self.filter_flag = True
                    return self.build_url_suffix()
        self.set_bad()
        return None

    def set_embed(self, aws_service_type):
        embed_dict_template = {
            "ebs": "read-aws-environment-ebs-volumes-compliance-stats",
            "rds": "read-aws-environment-rds-resources-compliance-stats",
            "DYNAMODB": "read-aws-environment-dynamodb-tables-compliance-stats",
            "S3": "read-aws-environment-protection-groups-compliance-stats",
            "MSSQL": "read-aws-environment-ec2-mssql-compliance-stats"
        }
        if aws_service_type not in embed_dict_template.keys():
            self.embed_flag = False
            return False

        if self.query_parms_flag:
            if embed_dict_template.get(aws_service_type, None) in self.query_parms.get("sort", []):
                self.embed_expression_string = "embed=" + embed_dict_template.get(aws_service_type, None)
                self.embed_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def set_search_account_id(self, account_id):
        self.search_account_id_flag = True
        self.search_account_id = account_id
        # print(f"AccountId {account_id}")

    def set_search_region(self, search_region):
        good_regions = ["us-east-1", "us-east-2", "us-west-1", "us-west-2"]
        if search_region in good_regions:
            self.search_region_flag = True
            self.search_region = search_region
        else:
            self.search_region_flag = False

    def set_search_status(self, status):
        # Not sure what good values are, so do not use.
        self.search_status_flag = False

    def set_search_service(self, aws_service):
        good_services = ["ebs", "rds", "DynamoDB"]
        if aws_service in good_services:
            self.search_service_flag = True
            self.search_service = aws_service
        else:
            self.search_region_flag = False


class RestoreEC2(API):
    def __init__(self):
        super(RestoreEC2, self).__init__("003")
        self.id = "003"
        self.filter_list = []
        # self.filter_expression = {}
        self.filter_expression_string = ""
        self.filter_expression_string_enc = ""
        self.limit_expression_string = ""
        self.start_expression_string = ""
        self.embed_expression_string = ""
        self.filter_flag = False
        self.limit_flag = False
        self.start_flag = False
        # self.sort_flag = False
        self.embed_flag = False
        self.limit = 100
        self.start = 1
        self.url_suffix_list = []
        self.url_suffix_list_enc = []
        self.url_suffix_string = ""
        self.url_suffix_string = ""
        self.url_suffix_string_enc = ""
        self.sort_expression_string = None
        self.type_get = False
        self.type_post = False
        self.good_run_flag = True
        self.filter_expression = None
        self.search_service_flag = False
        self.search_service = None
        self.search_account_id_flag = False
        self.search_account_id = None
        self.search_status_flag = False
        self.search_region_flag = False
        self.SearchRegion = None
        self.environment_id_dict = {}
        self.environment_id = None
        self.kms_key_name_target = ""
        self.kms_key_flag = False
        self.network_interface_mapping_flag = False
        self.network_interface_subnet_native_id_target = None
        self.network_interface_subnet_native_id_flag = False
        self.network_sg_list_target = None
        self.network_sg_list_flag = False
        self.ec2_vpc_native_id_target = None
        self.ec2_vpc_native_id_flag = False
        self.ec2_subnet_native_id_target = None
        self.ec2_subnet_native_id_flag = False
        self.iam_instance_profile_name_target = ""
        self.iam_instance_profile_name_flag = False
        self.ec2_key_pair_name_target = ""
        self.ec2_key_pair_name_flag = False
        self.aws_az_target = None
        self.aws_az_flag = False
        self.environment_id_target = None
        self.environment_id_flag = False
        self.target_flag = False
        self.log_activity_flag = False
        self.ebs_add_tag_flag = False
        self.backup_id = None
        self.backup_id_flag = False
        self.set_pagination()
        self.new_ec2_tags_flag = False
        self.new_ec2_tags = []
        self.backup_to_task_mapping = []
        self.restore_task_list_flag = False
        self.restore_task_list = []
        # print(f"RestoreEC2 why api_dict {api_dict.get(self.id, {}).get('type', None)}")
        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def set_restore_task_list(self, task_item):
        self.restore_task_list_flag = True
        self.restore_task_list.append(task_item)
        return True

    def get_restore_task_list(self):
        print(f"list {self.restore_task_list}")
        list_task = self.restore_task_list
        return list_task

    def save_restore_task(self):
        self.restore_task_list_flag = True
        return True

    def set_target_for_instance_restore(self, target, restore_type="simple"):
        EXAMPLE = {
            "account": "",
            "region": "",
            "aws_az": "",
            "iam_instance_profile_name": "",
            "key_pair_name": "",
            "security_group_native_ids": [],
            "subnet_native_id": "",
            "vpc_native_id": "",
            "kms_key_native_id": ""

        }
        if restore_type == "simple":
            # print(f" set target {target}")
            if self.set_target_environment_id(target.get("account", None), target.get("region", None)):
                # Required
                if target.get("aws_az", None):
                    self.set_target_aws_az(target.get("aws_az"))
                else:
                    self.set_error_msg(f"set_target_for_instance_restore: could not set target az {target.get("aws_az",None)}")
                    self.good = False
                    return False
                if target.get("subnet_native_id", None):
                    self.set_target_ec2_subnet_native_id(target.get("subnet_native_id", None))
                    self.set_target_network_interface_subnet_native_id(target.get("subnet_native_id", None))
                else:
                    self.set_error_msg(
                        f"set_target_for_instance_restore: could not set subnet_native_id {target.get("subnet_native_id", None)}")
                    self.good = False
                    return False
                if target.get("vpc_native_id", None):
                    self.set_target_ec2_vpc_native_id(target.get("vpc_native_id", None))
                else:
                    self.set_error_msg(
                        f"set_target_for_instance_restore: could not set target vpc_native_id {target.get("vpc_native_id", None)}")
                    self.good = False
                    return False
                self.target_flag = True
                # Optional
                iam = target.get("iam_instance_profile_name", None)
                # print(f"target iam {iam}")
                if iam:
                    self.set_target_iam_instance_profile_name(iam)
                sg = target.get("security_group_native_ids", None)
                # print(f"target sg {sg}")
                if sg:
                    # print("in set sg")
                    self.set_target_network_sg_list(sg)
                    self.network_sg_list_flag = True
                if target.get("key_pair_name", None):
                    self.set_target_ec2_key_pair_name(target.get("key_pair_name"))
                if target.get("kms_key_native_id", None):
                    self.set_target_kms_key_name(target.get("kms_key_native_id"))
                return True
            else:
                self.set_error_msg("set_target_for_instance_restore: could not set target account and region")
                self.good = False
                return False
        elif restore_type == "other":
            # Not implemented Yet
            self.good = False
            return False
        else:
            self.good = False
            return False

    def ec2_restore_from_record(self, List):
        if len(List) > 0:
            # print(f"Restore List {List}")
            for record in List:
                print(f"record to restore: {record}")
                # Check Expire Time
                if self.check_expire_time(record.get('backup_record', {}).get("source_expire_time", None)):
                    run_results = self.run_restore_record(record)
                    if run_results.get('errors', None):
                        return [False, f"restore failed for {record.get("instance_id", None)}: {run_results}"]
                    print(f"restore results for {record.get("instance_id", None)} {run_results}")
                    if self.log_activity_flag:
                        pass
                else:
                    print(f"Backup has expired for {record}")
                    pass
            return [True, ""]
        else:
            self.good = False
            return [False, "no records"]

    def check_expire_time(self, expire_time):
        try:
            expire_date = datetime.fromisoformat(expire_time[:-1]).astimezone(timezone.utc)
        except ValueError:
            if self.debug > 3: print(f"Expire date in invalid format: {expire_time}")
            return False
        if expire_date < datetime.now().astimezone(timezone.utc):
            return False
        else:
            # print(f"backup has not expired {expire_time}")
            return True


    def parse_ec2_tags(self, record, restore_type="simple"):
        if restore_type == "simple":
            ec2_tags = record.get("backup_record", {}).get("tags", [{"key": "", "value": ""}])
            if self.new_ec2_tags_flag:
                ec2_tags = self.check_tag_overlap(ec2_tags, self.new_ec2_tags)
            return ec2_tags
        elif restore_type == "add_tag":
            #Not implemented.

            return False
        else:
            self.good = False
            return False

    def parse_volumes_restore_target(self, record, restore_type="simple"):
        if restore_type == "simple":
            # simple restore means all default values and aws_az Not target_instance_native_id is used
            if self.aws_az_flag:
                aws_az = self.aws_az_target
                rsp = self.parse_ebs_from_record_list(record)
                if rsp:
                    ebs_block_device_mappings = rsp
                else:
                    self.good = False
                    return False
                if self.environment_id_flag:
                    environment_id = self.environment_id_target
                else:
                    self.good = False
                    return False
                volume_target_record = {
                    "aws_az": aws_az,
                    "ebs_block_device_mappings": ebs_block_device_mappings,
                    "environment_id": environment_id
                }
                return volume_target_record
            else:
                self.good = False
                return False
        elif restore_type == "other":
            # NOT IMPLEMENTED YET
            self.good = False
            return False
        else:
            self.good = False
            return False

    def parse_instance_restore_target(self, record, restore_type="simple"):
        # simple restore means all default values and should_power_on = True
        if restore_type == "simple":
            should_power_on = True
            if self.aws_az_flag:
                aws_az = self.aws_az_target
            else:
                self.good = False
                return False
            rsp = self.parse_ebs_from_record_list(record)
            if rsp:
                ebs_block_device_mappings = rsp
            else:
                self.good = False
                return False
            if self.environment_id_flag:
                environment_id = self.environment_id_target
            else:
                self.good = False
                return False
            ec2_tags = record.get("backup_record", {}).get("source_instance_tags", [])
            if self.new_ec2_tags_flag:
                ec2_tags.extend(self.new_ec2_tags)
            ami_native_id = ""
            if self.ec2_key_pair_name_flag:
                key_pair_name = self.ec2_key_pair_name_target
            else:
                key_pair_name = ""
            rsp = self.parse_network_interface_record_list(record)
            if rsp:
                network_interfaces = rsp
            else:
                self.good = False
                return False
            iam_instance_profile_name = None
            if record.get("backup_record", {}).get("source_iam_instance_profile_name", None):
                if self.iam_instance_profile_name_flag:
                    iam_instance_profile_name = self.iam_instance_profile_name_target
                else:
                    # record had an instance profile but target does not have one (dont fail)
                    iam_instance_profile_name = ""
                    self.error_msg = "Missing IAM Instance Profile for EC2 restore"
            if self.ec2_subnet_native_id_flag:
                subnet_native_id = self.ec2_subnet_native_id_target
            else:
                self.good = False
                return False
            if self.ec2_vpc_native_id_flag:
                vpc_native_id = self.ec2_vpc_native_id_target
            else:
                self.good = False
                return False
            instance_restore_target = {
                "should_power_on": should_power_on,
                "aws_az": aws_az,
                "ebs_block_device_mappings": ebs_block_device_mappings,
                "environment_id": environment_id,
                "iam_instance_profile_name": iam_instance_profile_name,
                "ami_native_id": ami_native_id,
                "tags": ec2_tags,
                "key_pair_name": key_pair_name,
                "network_interfaces": network_interfaces,
                "subnet_native_id": subnet_native_id,
                "vpc_native_id": vpc_native_id
            }
            return instance_restore_target

        elif restore_type == "other":
            # NOT IMPLEMENTED YET
            self.good = False
            return False
        else:
            self.good = False
            return False

    def parse_network_interface_record_list(self, record, restore_type="simple"):
        if restore_type == "simple":
            ni_record_list = []
            for ni_record in record.get("backup_record", {}).get("source_network_interface_list", []):
                rsp = self.parse_network_interface_record(ni_record)
                ni_record_list.append(rsp)
            return ni_record_list
        elif restore_type == "other":
            # No other methods supported yet
            return False
        else:
            self.good = False
            return False

    def parse_network_interface_record(self, record, restore_type="simple"):
        # simple restore means using default values, restore default = True and restore from backup = False
        if restore_type == "simple":
            # print(f"network interface record: {record}")
            if record.get("security_group_native_ids", []):
                # print("found sg")
                if self.network_sg_list_flag:
                    security_group_native_ids = self.network_sg_list_target
                else:
                    security_group_native_ids = []
            else:
                security_group_native_ids = []
            network_interface_native_id = ""
            if self.network_interface_subnet_native_id_flag:
                subnet_native_id = self.network_interface_subnet_native_id_target
            else:
                self.good = False
                return False
            device_index = record.get("device_index", None)
            network_interface_native_id = ""
            restore_default = True
            restore_from_backup = False

            network_interface_target_record = {
                "device_index": device_index,
                "network_interface_native_id": network_interface_native_id,
                "subnet_native_id": subnet_native_id,
                "restore_default": restore_default,
                "restore_from_backup": restore_from_backup,
                "security_group_native_ids": security_group_native_ids
            }
            return network_interface_target_record

        elif restore_type == "mapping":
            if self.network_interface_mapping_flag:
                # [] = self.network_interface_mapping(record)
                # NOT IMPLEMENTED YET
                return False
        else:
            return False

    def parse_ebs_from_record_list(self, record, restore_type="simple"):
        if restore_type == "simple":
            ebs_record_list = []
            for ebs_record in record.get("backup_record", {}).get("source_ebs_storage_list", []):
                rsp = self.parse_ebs_from_record(ebs_record)
                ebs_record_list.append(rsp)
            return ebs_record_list
        elif restore_type == "other":
            # No other methods supported yet
            return False
        else:
            self.good = False
            return False

    def parse_ebs_from_record(self, record, restore_type="simple"):
        # print(f"EBS record {record}")
        if restore_type == "simple":
            if record.get("kms_key_native_id", None):
                if self.kms_key_flag:
                    kms_key_native_id = self.kms_key_name_target
                else:
                    self.good = False
                    return False
            else:
                kms_key_native_id = ""
            volume_native_id = record.get("volume_native_id", None)
            mount_name = record.get("name", None)
            rec_ebs_tags = record.get("tags", [{"key": "", "value": ""}])
            # print(f"ebs tag {rec_ebs_tags}")
            if len(rec_ebs_tags) < 1:
                # print("ebs correct tags")
                rec_ebs_tags = [{"key": "Name", "value": ""}]
                ebs_tags = rec_ebs_tags
            else:
                ebs_tags = record.get("tags", [{"key": "", "value": ""}])
            ebs_target_record = {
                "volume_native_id": volume_native_id,
                "kms_key_native_id": kms_key_native_id,
                "name": mount_name,
                "tags": ebs_tags
            }
            return ebs_target_record
        elif restore_type == "add_tag":
            # No implemented
            return False
        elif restore_type == "ebs_mapping":
            # Not Implemented Yet
            return False
            # if self.ebs_mapping_flag:
            #    [NewMountName,NewKMSKey,new_tags] = self.ebs_mapping(record)
            #    if NewKMSKey:
            #        kms_key_native_id = NewKMSKey
            #    else:
            #        kms_key_native_id = None
            #    volume_native_id = record.get("backup_record",{}).get("source_ebs_storage_list",{}).get("volume_native_id",None)
            #    mount_name = NewMountName
            #    ebs_tags = new_tags
            #    ebs_target_record = {
            #        "volume_native_id":volume_native_id,
            #        "kms_key_native_id":kms_key_native_id,
            #        "name":mount_name,
            #        "tags": new_tags
            #    }
            #    return ebs_target_record
            # else:
            #    return False
        else:
            return False

    def set_ebs_block_mapping(self, input_block_mapping):
        # Not Implemented Yet
        return False

    def set_target_kms_key_name(self, value):
        self.kms_key_name_target = value
        self.kms_key_flag = True

    def clear_target_kms_key_name(self):
        self.kms_key_name_target = None
        self.kms_key_flag = False

    def set_target_network_interface_subnet_native_id(self, value):
        self.network_interface_subnet_native_id_target = value
        self.network_interface_subnet_native_id_flag = True

    def clear_target_network_interface_subnet_native_id(self):
        self.network_interface_subnet_native_id_target = None
        self.network_interface_subnet_native_id_flag = False

    def set_target_network_sg_list(self, value):
        self.network_sg_list_target = value
        self.network_sg_list_flag = True

    def clear_target_network_sg_list(self):
        self.network_sg_list_target = []
        self.network_sg_list_flag = False

    def set_target_ec2_vpc_native_id(self, value):
        self.ec2_vpc_native_id_target = value
        self.ec2_vpc_native_id_flag = True

    def clear_target_ec2_vpc_native_id(self):
        self.ec2_vpc_native_id_target = None
        self.ec2_vpc_native_id_flag = False

    def set_target_ec2_subnet_native_id(self, value):
        self.ec2_subnet_native_id_target = value
        self.ec2_subnet_native_id_flag = True

    def clear_target_ec2_subnet_native_id(self):
        self.ec2_subnet_native_id_target = None
        self.ec2_subnet_native_id_flag = False

    def set_target_iam_instance_profile_name(self, value):
        self.iam_instance_profile_name_target = value
        self.iam_instance_profile_name_flag = True

    def clear_target_iam_instance_profile_name(self):
        self.iam_instance_profile_name_target = None
        self.iam_instance_profile_name_flag = False

    def set_target_ec2_key_pair_name(self, value):
        self.ec2_key_pair_name_target = value
        self.ec2_key_pair_name_flag = True

    def clear_target_ec2_key_pair_name(self):
        self.ec2_key_pair_name_target = None
        self.ec2_key_pair_name_flag = False

    def set_target_aws_az(self, value):
        self.aws_az_target = value
        self.aws_az_flag = True

    def clear_target_aws_az(self):
        self.aws_az_target = None
        self.aws_az_flag = False

    def set_backup_id(self, record):
        if record.get("backup_record", {}).get("source_backup_id", None):
            self.backup_id = record.get("backup_record", {}).get("source_backup_id", None)
            self.backup_id_flag = True
            return self.backup_id

        else:
            self.good = False
        return False

    def clear_backup_id(self):
        self.aws_az_target = None
        self.aws_az_flag = False

    def set_target_environment_id(self, account, region):
        if self.set_aws_account_id(account) and self.set_aws_region(region):
            env_id_api = EnvironmentId()
            env_id_api.set_url_prefix(self.url_prefix)
            env_id_api.set_url_prefix(self.url_prefix)
            env_id_api.set_token(self.token)
            env_id_api.set_search_account_id(self.aws_account_id)
            env_id_api.set_search_region(self.aws_region)
            if env_id_api.run_api():
                rsp = env_id_api.environment_id_parse_results()
                del env_id_api
                if rsp:
                    self.environment_id_target = rsp
                    self.environment_id_flag = True
                    return True
        del env_id_api
        self.good = False
        return False

    def clear_target_environment_id(self):
        self.environment_id_target = None
        self.environment_id_flag = False

    def add_ec2_tag_to_instance(self, new_tags):
        if type(new_tags) == list:
            for tag in new_tags:
                if tag.get('key') and tag.get('value'):
                    pass
                else:
                    return False
        else:
            return False
        self.new_ec2_tags = new_tags
        self.new_ec2_tags_flag = True
        return True

    def set_payload(self, record, restore_type="ec2"):
        if restore_type == "ec2":
            if self.target_flag:
                backup_id = self.set_backup_id(record)
                ec2_restore_target = self.parse_instance_restore_target(record)

                if backup_id and ec2_restore_target:
                    payload = {
                        "source": {"backup_id": backup_id},
                        "target": {"instance_restore_target": ec2_restore_target}
                    }
                    self.payload = payload
                    self.payload_flag = True
                    return True
        elif restore_type == "ebs":
            # Not implemente yet
            return False
        elif restore_type == "ami":
            # Not implemente yet
            return False

        self.good = False
        return False

    def run_restore_record(self, record):
        if self.good:
            if self.set_payload(record):
                result = self.exec_api()
                if self.restore_task_list_flag:
                    task_item = {
                        "task": self.get_task_id(),
                        "instance_id": record.get("instance_id"),
                        "backup_id": record.get("backup_record", {}).get("source_backup_id", None)
                    }
                    self.set_restore_task_list(task_item)
                return result
        else:
            return False

    def environment_id_parse_results(self, parse_type="id"):
        ExampleParmsList = [
            "ID :backup_ids & InstanceIds & TimeStamp",
            "Restore :Parameters Needed for EC2 Restore",
            "Count : Number of Instances",
            "All : All Data"
        ]
        # print(self.environment_id_dict)
        if parse_type == "id":
            if self.environment_id_dict:
                if len(self.environment_id_dict.keys()) > 1:
                    return False
                else:
                    for key in self.environment_id_dict.keys():
                        return key
            else:
                return False
        elif parse_type == "all":
            if self.environment_id_dict:
                return self.environment_id_dict
            else:
                return False
        return False

    def set_page_size(self, x):
        if self.query_parms_flag:
            if "limit" in self.query_parms.keys():
                self.limit = x
                self.limit_expression_string = "limit=" + str(self.limit)
                self.limit_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def build_url_suffix(self):
        self.url_suffix_list = []
        if self.good:
            if self.filter_flag:
                self.url_suffix_list.append(self.filter_expression_string_enc)

            if self.embed_flag:
                self.url_suffix_list.append(self.sort_expression_string)

            if self.limit_flag:
                self.url_suffix_list.append(self.limit_expression_string)

            if self.start_flag:
                self.url_suffix_list.append(self.start_expression_string)

            self.url_suffix_string = "?" + "&".join(self.url_suffix_list)
            String = self.url_suffix_string
            # #print(String)
            self.set_url(String)
            return self.url_suffix_string


class EC2BackupList(API):
    def __init__(self):
        super(EC2BackupList, self).__init__("001")
        self.id = "001"
        self.filter_list = []
        self.filter_expression = {}
        self.filter_expression_string = ""
        self.filter_expression_string_enc = ""
        self.limit_expression_string = ""
        self.start_expression_string = ""
        self.sort_expression_string = ""
        self.filter_flag = False
        self.limit_flag = False
        self.start_flag = False
        self.sort_flag = False
        self.limit = 100
        self.start = 1
        self.url_suffix_list = []
        self.url_suffix_list_enc = []
        self.url_suffix_string = ""
        self.url_suffix_string_enc = ""
        self.type_get = False
        self.type_post = False
        self.good_run_flag = True
        self.filter_expression = None
        self.start_day_time_stamp = None
        self.search_instance_id = None
        self.search_type = None
        self.search_start_day = 0
        self.search_end_day = 10
        self.tag_name_match = ""
        self.tag_value_match = ""
        self.search_instance_id_flag = False
        # self.filter_expression = "$lte"
        self.start_hour = "22"
        self.start_min = "58"
        self.start_sec = "58"
        self.end_date = None
        self.set_sort("-start_timestamp")
        self.current_ec2_instance_id_time_stamp = {}
        self.current_ec2_instance_info = {}
        self.set_pagination()
        # print(f"RestoreEC2 why api_dict {api_dict.get(self.id, {}).get('type', None)}")
        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def run_all(self):
        if self.search_type:
            day_start_delta = self.search_start_day
            day_end_delta = self.search_end_day
            # print(f"the delta start end {day_start_delta} {day_end_delta}")
            if self.search_type == "backwards":
                if day_start_delta > day_end_delta:
                    self.error_msg = "Search Start day is older then search end day"
                    return False
                today = datetime.now().astimezone(timezone.utc)
                start_delta = timedelta(days=day_start_delta)
                end_delta = timedelta(days=day_end_delta)
                start_date = today - start_delta
                self.end_date = today - end_delta
                # #print(start_date)
                if start_date.year < 10:
                    start_date_year = "0" + str(start_date.year)
                else:
                    start_date_year = str(start_date.year)
                if start_date.month < 10:
                    start_date_month = "0" + str(start_date.month)
                else:
                    start_date_month = str(start_date.month)
                if start_date.day < 10:
                    start_date_day = "0" + str(start_date.day)
                else:
                    start_date_day = str(start_date.day)
                self.start_day_time_stamp = f"{start_date_year}-{start_date_month}-{start_date_day}T{self.start_hour}:{self.start_min}:{self.start_sec}Z"
            elif self.search_type == "forwards":
                today = datetime.now().astimezone(timezone.utc)
                end_delta = timedelta(days=day_end_delta)
                end_date = today - end_delta
                # #print(start_date)
                if end_date.year < 10:
                    end_date_year = "0" + str(end_date.year)
                else:
                    end_date_year = str(end_date.year)
                if end_date.month < 10:
                    end_date_month = "0" + str(end_date.month)
                else:
                    end_date_month = str(end_date.month)
                if end_date.day < 10:
                    end_date_day = "0" + str(end_date.day)
                else:
                    end_date_day = str(end_date.day)
                # start_date_month = str(start_date.month)
                # start_date_day = str(start_date.day)
                self.start_day_time_stamp = f"{end_date_year}-{end_date_month}-{end_date_day}T{self.start_hour}:{self.start_min}:{self.start_sec}Z"
            self.set_filter("start_timestamp", self.filter_expression, self.start_day_time_stamp)
        if self.search_instance_id_flag:
            self.set_filter("instance_id", "$eq", self.search_instance_id)
        self.results = []
        first_results = self.exec_api()
        if self.good:
            items = first_results.get("_embedded", {}).get("items", {})
            for i in items:
                check = self.pass_check(i)
                if check:
                    self.current_ec2_instance_info[check] = i
        else:
            return False
        if self.total_pages_count:
            if self.total_pages_count > 1:
                for i in range(2, self.total_pages_count):
                    self.set_page_start(i)
                    aws_region = self.exec_api()
                    if self.good:
                        items = aws_region.get("_embedded", {}).get("items", {})
                        for i in items:
                            check = self.pass_check(i)
                            if check:
                                self.current_ec2_instance_info[check] = i
                    else:
                        return False
                    # #print(f"read page {i}")
        return len(self.current_ec2_instance_info)

    def pass_check(self, response):
        aws_region = response.get("aws_region", None)
        aws_account_id = response.get("account_native_id", None)
        if not (aws_region == self.aws_region and aws_account_id == self.aws_account_id):
            return False
        if self.aws_tag_flag:
            tags = response.get("tags", None)
            found_tag = False
            for tag in tags:
                if tag.get("key", None) == self.aws_tag_key and tag.get("value", None) == self.aws_tag_value:
                    found_tag = True
            if not found_tag:
                return False
        time_stamp = response.get("start_timestamp", None)
        clumio_instance_id = response.get("instance_id", None)
        new_date = datetime.fromisoformat(time_stamp[:-1]).astimezone(timezone.utc)
        if self.search_type == "backwards":
            # new_date = datetime.fromisoformat(time_stamp[:-1]).astimezone(timezone.utc)
            if self.current_ec2_instance_id_time_stamp.get(clumio_instance_id, None):
                if new_date > self.current_ec2_instance_id_time_stamp.get(clumio_instance_id, None):
                    self.current_ec2_instance_id_time_stamp[clumio_instance_id] = new_date
                    return clumio_instance_id
                else:
                    return False
            else:
                if new_date > self.end_date:
                    self.current_ec2_instance_id_time_stamp[clumio_instance_id] = new_date
                    return clumio_instance_id
                else:
                    return False
        else:
            if self.current_ec2_instance_id_time_stamp.get(clumio_instance_id, None):
                if new_date > self.current_ec2_instance_id_time_stamp.get(clumio_instance_id, None):
                    self.current_ec2_instance_id_time_stamp[clumio_instance_id] = new_date
                    return clumio_instance_id
                else:
                    return False
            else:
                self.current_ec2_instance_id_time_stamp[clumio_instance_id] = new_date
                return clumio_instance_id

    def ec2_parse_results(self, parse_type):
        ExampleParmsList = [
            "ID :backup_ids & InstanceIds & TimeStamp",
            "Restore :Parameters Needed for EC2 Restore",
            "Count : Number of Instances",
            "All : All Data"
        ]
        records = []
        if parse_type == "id":
            for inst in self.current_ec2_instance_info.keys():
                rec = {"id_record": [self.current_ec2_instance_info[inst].get("instance_native_id", None),
                                     self.current_ec2_instance_info[inst].get("id", None),
                                     self.current_ec2_instance_info[inst].get("start_timestamp", None)]}
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        elif parse_type == "all":
            for inst in self.current_ec2_instance_info.keys():
                rec = {"item": self.current_ec2_instance_info[inst]}
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        elif parse_type == "restore":
            for inst in self.current_ec2_instance_info.keys():
                rec = {"instance_id": self.current_ec2_instance_info[inst].get("instance_native_id", None),
                       "backup_record": {
                           "source_backup_id": self.current_ec2_instance_info[inst].get("id", None),
                           "SourceAmiId": self.current_ec2_instance_info[inst].get("ami", {}).get("ami_native_id",
                                                                                                  None),
                           "source_iam_instance_profile_name": self.current_ec2_instance_info[inst].get(
                               "iam_instance_profile",
                               None),
                           "SourceKeyPairName": self.current_ec2_instance_info[inst].get("key_pair_name", None),
                           "source_network_interface_list": self.current_ec2_instance_info[inst].get(
                               "network_interfaces",
                               []),
                           "source_ebs_storage_list": self.current_ec2_instance_info[inst].get(
                               "attached_backup_ebs_volumes",
                               []),
                           "source_instance_tags": self.current_ec2_instance_info[inst].get("tags", []),
                           "SourceVPCID": self.current_ec2_instance_info[inst].get("vpc_native_id", []),
                           "source_az": self.current_ec2_instance_info[inst].get("aws_az", []),
                           "source_expire_time": self.current_ec2_instance_info[inst].get("expiration_timestamp", []),
                       }
                       }
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        return {}

    def ec2_search_by_tag(self, Key, Value):
        self.set_aws_tag_key(Key)
        self.set_aws_tag_value(Value)

    def set_page_size(self, x):
        if self.query_parms_flag:
            if "limit" in self.query_parms.keys():
                self.limit = x
                self.limit_expression_string = "limit=" + str(self.limit)
                self.limit_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def set_page_start(self, x):
        if self.query_parms_flag:
            if "start" in self.query_parms.keys():
                self.start = x
                self.start_expression_string = "start=" + str(self.start)
                self.start_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def build_url_suffix(self):
        self.url_suffix_list = []
        if self.good:
            if self.filter_flag:
                self.url_suffix_list.append(self.filter_expression_string_enc)

            if self.sort_flag:
                self.url_suffix_list.append(self.sort_expression_string)

            if self.limit_flag:
                self.url_suffix_list.append(self.limit_expression_string)

            if self.start_flag:
                self.url_suffix_list.append(self.start_expression_string)

            self.url_suffix_string = "?" + "&".join(self.url_suffix_list)
            String = self.url_suffix_string
            # #print(String)
            self.set_url(String)
            return self.url_suffix_string

    def set_filter(self, filter_name, filter_expression, filter_value):
        filter_expression_dict = {}
        # print(f"setFilter 01 {filter_name}")
        if self.query_parms_flag:
            # print("setFilter 02")
            if filter_name in self.query_parms.get("filter", {}).keys():
                # print("setFilter 03")
                if filter_expression in self.query_parms.get("filter", {}).get(filter_name, []):
                    # print("setFilter 04")
                    self.filter_list.append([filter_name, filter_expression, filter_value])
                    for i in self.filter_list:
                        ##print(f"in set_filter 05 {i}")
                        filter_expression_dict[i[0]] = {i[1]: i[2]}
                    self.filter_expression_string = "filter=" + json.dumps(filter_expression_dict,
                                                                           separators=(',', ':'))
                    self.filter_expression_string_enc = "filter=" + urllib.parse.quote(
                        json.dumps(filter_expression_dict, separators=(',', ':')))
                    self.filter_flag = True
                    return self.build_url_suffix()
        self.set_bad()
        return None

    def set_sort(self, sortName):

        if self.query_parms_flag:
            if sortName in self.query_parms.get("sort", []):
                self.sort_expression_string = "sort=" + sortName
                self.sort_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def set_search_instance_id(self, instance_id):
        self.search_instance_id_flag = True
        self.search_instance_id = instance_id

    def set_search_forwards_from_offset(self, end_day_offset, search_type="forwards"):
        self.search_type = search_type
        self.set_search_end_day(end_day_offset)
        self.filter_expression = "$gt"
        self.start_hour = "00"
        self.start_min = "00"
        self.start_sec = "00"

    def set_search_backwards_from_offset(self, start_day_offset, end_day_offset, search_type="backwards"):
        self.search_type = search_type
        self.set_search_end_day(end_day_offset)
        self.set_search_start_day(start_day_offset)
        self.filter_expression = "$lte"
        self.start_hour = "23"
        self.start_min = "59"
        self.start_sec = "59"

    def set_search_start_day(self, start_day):
        try:
            int(start_day)
            self.search_start_day = start_day
        except ValueError:
            return False
        return True

    def set_search_end_day(self, end_day):
        try:
            int(end_day)
            self.search_end_day = end_day
        except ValueError:
            return False
        return True


class EBSBackupList(API):
    def __init__(self):
        super(EBSBackupList, self).__init__("004")
        self.id = "001"
        self.filter_list = []
        self.filter_expression = {}
        self.filter_expression_string = ""
        self.filter_expression_string_enc = ""
        self.limit_expression_string = ""
        self.start_expression_string = ""
        self.sort_expression_string = ""
        self.filter_flag = False
        self.limit_flag = False
        self.start_flag = False
        self.sort_flag = False
        self.limit = 100
        self.start = 1
        self.url_suffix_list = []
        self.url_suffix_list_enc = []
        self.url_suffix_string = ""
        self.url_suffix_string_enc = ""
        self.type_get = False
        self.type_post = False
        self.set_pagination()
        self.good_run_flag = True
        self.filter_expression = None
        self.start_day_time_stamp = None
        self.search_volume_id = None
        self.search_type = None
        self.search_start_day = 0
        self.search_end_day = 10
        self.tag_name_match = ""
        self.tag_value_match = ""
        self.search_instance_id = ""
        self.search_volume_id_flag = False
        # self.filter_expression = "$lte"
        self.start_hour = "22"
        self.start_min = "58"
        self.start_sec = "58"
        self.end_date = None
        # self.set_sort("-start_timestamp")
        self.current_ebs_volume_id_time_stamp = {}
        self.current_ebs_volume_info = {}
        self.set_pagination()
        self.backup_type = "clumio_backup"
        # print(f"RestoreEC2 why api_dict {api_dict.get(self.id, {}).get('type', None)}")
        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def run_all(self):
        if self.search_type:
            day_start_delta = self.search_start_day
            day_end_delta = self.search_end_day
            # print(f"the delta start end {day_start_delta} {day_end_delta}")
            if self.search_type == "backwards":
                if day_start_delta > day_end_delta:
                    self.error_msg = "Search Start day is older then search end day"
                    return False
                today = datetime.now().astimezone(timezone.utc)
                start_delta = timedelta(days=day_start_delta)
                end_delta = timedelta(days=day_end_delta)
                start_date = today - start_delta
                self.end_date = today - end_delta
                # #print(start_date)
                if start_date.year < 10:
                    start_date_year = "0" + str(start_date.year)
                else:
                    start_date_year = str(start_date.year)
                if start_date.month < 10:
                    start_date_month = "0" + str(start_date.month)
                else:
                    start_date_month = str(start_date.month)
                if start_date.day < 10:
                    start_date_day = "0" + str(start_date.day)
                else:
                    start_date_day = str(start_date.day)
                self.start_day_time_stamp = f"{start_date_year}-{start_date_month}-{start_date_day}T{self.start_hour}:{self.start_min}:{self.start_sec}Z"
            elif self.search_type == "forwards":
                today = datetime.now().astimezone(timezone.utc)
                end_delta = timedelta(days=day_end_delta)
                end_date = today - end_delta
                # #print(start_date)
                if end_date.year < 10:
                    end_date_year = "0" + str(end_date.year)
                else:
                    end_date_year = str(end_date.year)
                if end_date.month < 10:
                    end_date_month = "0" + str(end_date.month)
                else:
                    end_date_month = str(end_date.month)
                if end_date.day < 10:
                    end_date_day = "0" + str(end_date.day)
                else:
                    end_date_day = str(end_date.day)
                # start_date_month = str(start_date.month)
                # start_date_day = str(start_date.day)
                self.start_day_time_stamp = f"{end_date_year}-{end_date_month}-{end_date_day}T{self.start_hour}:{self.start_min}:{self.start_sec}Z"
            self.set_filter("start_timestamp", self.filter_expression, self.start_day_time_stamp)
        if self.search_volume_id_flag:
            self.set_filter("volume_id", "$eq", self.search_volume_id)
        self.results = []
        first_results = self.exec_api()
        if self.good:
            items = first_results.get("_embedded", {}).get("items", {})
            for i in items:
                check = self.pass_check(i)
                if check:
                    self.current_ebs_volume_info[check] = i
        else:
            return False
        if self.total_pages_count:
            if self.total_pages_count > 1:
                for i in range(2, self.total_pages_count):
                    self.set_page_start(i)
                    aws_region = self.exec_api()
                    if self.good:
                        items = aws_region.get("_embedded", {}).get("items", {})
                        for i in items:
                            check = self.pass_check(i)
                            if check:
                                self.current_ebs_volume_info[check] = i
                    else:
                        return False
                    # #print(f"read page {i}")
        return len(self.current_ebs_volume_info)

    def pass_check(self, response):
        aws_region = response.get("aws_region", None)
        aws_account_id = response.get("account_native_id", None)
        if not (aws_region == self.aws_region and aws_account_id == self.aws_account_id):
            return False
        if self.aws_tag_flag:
            tags = response.get("tags", None)
            found_tag = False
            for tag in tags:
                if tag.get("key", None) == self.aws_tag_key and tag.get("value", None) == self.aws_tag_value:
                    found_tag = True
            if not found_tag:
                return False
        time_stamp = response.get("start_timestamp", None)
        clumio_volume_id = response.get("volume_id", None)
        new_date = datetime.fromisoformat(time_stamp[:-1]).astimezone(timezone.utc)
        if self.search_type == "backwards":
            # new_date = datetime.fromisoformat(time_stamp[:-1]).astimezone(timezone.utc)
            if self.current_ebs_volume_id_time_stamp.get(clumio_volume_id, None):
                if new_date > self.current_ebs_volume_id_time_stamp.get(clumio_volume_id, None):
                    self.current_ebs_volume_id_time_stamp[clumio_volume_id] = new_date
                    return clumio_volume_id
                else:
                    return False
            else:
                if new_date > self.end_date:
                    self.current_ebs_volume_id_time_stamp[clumio_volume_id] = new_date
                    return clumio_volume_id
                else:
                    return False
        else:
            if self.current_ebs_volume_id_time_stamp.get(clumio_volume_id, None):
                if new_date > self.current_ebs_volume_id_time_stamp.get(clumio_volume_id, None):
                    self.current_ebs_volume_id_time_stamp[clumio_volume_id] = new_date
                    return clumio_volume_id
                else:
                    return False
            else:
                self.current_ebs_volume_id_time_stamp[clumio_volume_id] = new_date
                return clumio_volume_id

    def ebs_parse_results(self, parse_type):
        ExampleParmsList = [
            "ID :backup_ids & VolumeIds & TimeStamp",
            "Restore :Parameters Needed for EC2 Restore",
            "Count : Number of Volumes",
            "All : All Data"
        ]
        records = []
        print("start parse")
        if parse_type == "id":
            for vol in self.current_ebs_volume_info.keys():
                rec = {"id_record": [self.current_ebs_volume_info[vol].get("volume_native_id", None),
                                     self.current_ebs_volume_info[vol].get("id", None),
                                     self.current_ebs_volume_info[vol].get("start_timestamp", None),
                                     self.current_ebs_volume_info[vol].get("type", None)]}
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        elif parse_type == "all":
            for vol in self.current_ebs_volume_info.keys():
                rec = {"item": self.current_ebs_volume_info[vol]}
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        elif parse_type == "restore":
            print(f"in parse restore{self.debug} {self.current_ebs_volume_info}")
            for vol in self.current_ebs_volume_info.keys():
                if self.debug > 2: print(f"volume record {self.current_ebs_volume_info[vol]}")
                # Skip volumes not backed up to Clumio (i.e. in account snapshots)
                if self.current_ebs_volume_info[vol].get("type", None) == self.backup_type:
                    if self.current_ebs_volume_info[vol].get("is_encrypted"):
                        is_encrypted = True
                    else:
                        is_encrypted = False

                    rec = {"volume_id": self.current_ebs_volume_info[vol].get("volume_native_id", None),
                           "backup_record": {
                               "source_backup_id": self.current_ebs_volume_info[vol].get("id", None),
                               "source_volume_id": self.current_ebs_volume_info[vol].get("volume_native_id", None),
                               "source_volume_tags": self.current_ebs_volume_info[vol].get("tags", []),
                               "source_encrypted_flag": is_encrypted,
                               "source_az": self.current_ebs_volume_info[vol].get("aws_az", None),
                               "source_kms": self.current_ebs_volume_info[vol].get("kms_key_native_id", None),
                               "source_expire_time": self.current_ebs_volume_info[vol].get("expiration_timestamp",
                                                                                           None),
                           }
                           }
                    records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        return {}

    def ebs_search_by_tag(self, Key, Value):
        self.set_aws_tag_key(Key)
        self.set_aws_tag_value(Value)

    def set_page_size(self, x):
        if self.query_parms_flag:
            if "limit" in self.query_parms.keys():
                self.limit = x
                self.limit_expression_string = "limit=" + str(self.limit)
                self.limit_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def set_page_start(self, x):
        if self.query_parms_flag:
            if "start" in self.query_parms.keys():
                self.start = x
                self.start_expression_string = "start=" + str(self.start)
                self.start_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def build_url_suffix(self):
        self.url_suffix_list = []
        if self.good:
            if self.filter_flag:
                self.url_suffix_list.append(self.filter_expression_string_enc)

            if self.sort_flag:
                self.url_suffix_list.append(self.sort_expression_string)

            if self.limit_flag:
                self.url_suffix_list.append(self.limit_expression_string)

            if self.start_flag:
                self.url_suffix_list.append(self.start_expression_string)

            self.url_suffix_string = "?" + "&".join(self.url_suffix_list)
            String = self.url_suffix_string
            # #print(String)
            self.set_url(String)
            return self.url_suffix_string

    def set_filter(self, filter_name, filter_expression, filter_value):
        filter_expression_dict = {}
        # print(f"setFilter 01 {filter_name}")
        if self.query_parms_flag:
            # print("setFilter 02")
            if filter_name in self.query_parms.get("filter", {}).keys():
                # print("setFilter 03")
                if filter_expression in self.query_parms.get("filter", {}).get(filter_name, []):
                    # print("setFilter 04")
                    self.filter_list.append([filter_name, filter_expression, filter_value])
                    for i in self.filter_list:
                        ##print(f"in set_filter 05 {i}")
                        filter_expression_dict[i[0]] = {i[1]: i[2]}
                    self.filter_expression_string = "filter=" + json.dumps(filter_expression_dict,
                                                                           separators=(',', ':'))
                    self.filter_expression_string_enc = "filter=" + urllib.parse.quote(
                        json.dumps(filter_expression_dict, separators=(',', ':')))
                    self.filter_flag = True
                    return self.build_url_suffix()
        self.set_bad()
        return None

    def set_sort(self, sortName):

        if self.query_parms_flag:
            if sortName in self.query_parms.get("sort", []):
                self.sort_expression_string = "sort=" + sortName
                self.sort_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def set_search_volume_id(self, volume_id):
        self.search_volume_id_flag = True
        self.search_volume_id = volume_id

    def set_search_forwards_from_offset(self, end_day_offset, search_type="forwards"):
        self.search_type = search_type
        self.set_search_end_day(end_day_offset)
        self.filter_expression = "$gt"
        self.start_hour = "00"
        self.start_min = "00"
        self.start_sec = "00"

    def set_search_backwards_from_offset(self, start_day_offset, end_day_offset, search_type="backwards"):
        self.search_type = search_type
        self.set_search_end_day(end_day_offset)
        self.set_search_start_day(start_day_offset)
        self.filter_expression = "$lte"
        self.start_hour = "23"
        self.start_min = "59"
        self.start_sec = "59"

    def set_search_start_day(self, start_day):
        try:
            int(start_day)
            self.search_start_day = start_day
        except ValueError:
            return False
        return True

    def set_search_end_day(self, end_day):
        try:
            int(end_day)
            self.search_end_day = end_day
        except ValueError:
            return False
        return True


# NEW API

class RestoreEBS(API):
    def __init__(self):
        super(RestoreEBS, self).__init__("005")
        self.id = "003"
        self.filter_list = []
        # self.filter_expression = {}
        self.filter_expression_string = ""
        self.filter_expression_string_enc = ""
        self.limit_expression_string = ""
        self.start_expression_string = ""
        self.embed_expression_string = ""
        self.filter_flag = False
        self.limit_flag = False
        self.start_flag = False
        # self.sort_flag = False
        self.embed_flag = False
        self.limit = 100
        self.start = 1
        self.url_suffix_list = []
        self.url_suffix_list_enc = []
        self.url_suffix_string = ""
        self.url_suffix_string_enc = ""
        self.HTMLURLSuffSting = ""
        self.type_get = False
        self.type_post = False
        self.set_pagination()
        self.good_run_flag = True
        self.filter_expression = None
        self.search_service_flag = False
        self.search_service = None
        self.search_account_id_flag = False
        self.search_account_id = None
        self.search_status_flag = False
        self.search_region_flag = False
        self.SearchRegion = None
        self.environment_id_dict = {}
        self.set_pagination()
        self.kms_key_name_target = ""
        self.kms_key_flag = False
        self.allowed_volume_types = ["gp2", "gp3"]
        self.aws_az_target = None
        self.aws_az_flag = False
        self.environment_id_flag = False
        self.environment_id = None
        self.target_flag = False
        self.log_activity_flag = False
        self.iops_target = None
        self.iops_flag = False
        self.volume_type_target = None
        self.volume_type_flag = False
        self.set_post()
        self.source_volume_tag_flag = True
        self.source_volume_tags = []
        self.restore_task_list_flag = False
        self.restore_task_list = []
        # print(f"RestoreEC2 why api_dict {api_dict.get(self.id, {}).get('type', None)}")
        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def set_restore_task_list(self, task_item):
        print(f"new task {task_item}")
        self.restore_task_list.append(task_item)
        return True

    def get_restore_task_list(self):
        print(f"list {self.restore_task_list}")
        list_task = self.restore_task_list
        return list_task

    def save_restore_task(self):
        self.restore_task_list_flag = True
        return True

    def set_target_for_ebs_restore(self, target, restore_type="simple"):
        EXAMPLE = {
            "account": "",
            "region": "",
            "aws_az": "",
            "iops": None,
            "kms_key_native_id": "",
            "volume_type": None

        }
        if self.debug > 1: print(f"set_target_for_ebs_restore - Good value {self.good}")
        if restore_type == "simple":
            # print(f" set target {target}")
            if self.debug > 3: print(f"set_target_for_ebs_restore target: {target}, restore_type: {restore_type} ")
            if self.set_target_environment_id(target.get("account", None), target.get("region", None)):
                # Required
                if target.get("aws_az", None):
                    self.set_target_aws_az(target.get("aws_az"))
                else:
                    if self.debug > 3: print("set_target_for_ebs_restore could not set az ")
                    self.set_error_msg(f"set_target_for_ebs_restore: could not set az {target.get("aws_az")}")
                    self.good = False
                    return False
                self.target_flag = True

                # Optional

                iops = target.get("iops", None)
                if iops:
                    self.set_target_iops(iops)

                volume_type = target.get("volume_type", None)
                if volume_type:
                    self.set_target_volume_type(volume_type)

                kms_key_native_id = target.get("kms_key_native_id", None)
                if kms_key_native_id:
                    self.set_target_kms_key_name(kms_key_native_id)
                    self.kms_key_flag = True
                if self.debug > 4: print("set_target_for_ebs_restore TargetValuesSet ")
                return True

            else:
                if self.debug > 3: print("set_target_for_ebs_restore could not set target account and region ")
                self.set_error_msg("set_target_for_ebs_restore: could not set target account and region")
                self.good = False
                return False
        elif restore_type == "verlero":
            # print(f" set target {target}")
            if self.debug > 3: print(f"set_target_for_ebs_restore target: {target}, restore_type: {restore_type} ")
            if self.set_target_environment_id(target.get("account", None), target.get("region", None)):
                # Required
                if target.get("aws_az", None):
                    self.set_target_aws_az(target.get("aws_az"))
                else:
                    if self.debug > 3: print("set_target_for_ebs_restore could not set az ")
                    self.set_error_msg(f"set_target_for_ebs_restore: could not set az {target.get("aws_az")}")
                    self.good = False
                    return False
                self.target_flag = True

                # Optional

                iops = target.get("iops", None)
                if iops:
                    self.set_target_iops(iops)

                volume_type = target.get("volume_type", None)
                if volume_type:
                    self.set_target_volume_type(volume_type)

                kms_key_native_id = target.get("kms_key_native_id", None)
                if kms_key_native_id:
                    self.set_target_kms_key_name(kms_key_native_id)
                    self.kms_key_flag = True
                if self.debug > 4: print("set_target_for_ebs_restore TargetValuesSet ")
                return True

            else:
                if self.debug > 3: print("set_target_for_ebs_restore could not set target account and region ")
                self.set_error_msg("set_target_for_ebs_restore: could not set target account and region")
                self.good = False
                return False
        elif restore_type == "other":

            self.good = False
            return False
        else:
            self.good = False
            return False

    def ebs_restore_from_record(self, r_list, restore_type="simple"):
        if self.debug > 1: print(f"ebs_restore_from_record - Good value {self.good}")
        if len(r_list) > 0:
            # print(f"Restore r_list {r_list}")
            for record in r_list:
                if self.debug > 1: print(f"record to restore: {record}")
                # Check Expire Time
                if self.check_expire_time(record.get('backup_record', {}).get("source_expire_time", None)):
                    run_results = self.run_restore_record(record, restore_type)
                    if run_results.get('errors', None):
                        return [False, f"restore failed for {record.get("volumeId", None)}: {run_results}"]
                    if self.debug > 1: print(f"restore results for {record.get("volumeId", None)} {run_results}")
                    if self.log_activity_flag:
                        pass
                else:
                    if self.debug > 3:
                        print(f"Backup has expired for {record}")
                    pass
            return [True, ""]
        else:
            self.good = False
            if self.debug > 3:
                print("EBS List has no records to restore")
            return [False, "no records"]

    def check_expire_time(self, expire_time):
        try:
            expire_date = datetime.fromisoformat(expire_time[:-1]).astimezone(timezone.utc)
        except ValueError:
            if self.debug > 3: print(f"Expire date in invalid format: {expire_time}")
            return False
        if expire_date < datetime.now().astimezone(timezone.utc):
            if self.debug > 3: print(f"Expired: {expire_time}")
            return False
        else:
            if self.debug > 3: print(f"backup has not expired {expire_time}")
            return True

    def parse_ebs_from_record(self, record, restore_type="simple"):
        # print(f"EBS record {record}")
        if restore_type == "simple":
            if record.get("kms_key_native_id", None):
                if self.kms_key_flag:
                    kms_key_native_id = self.kms_key_name_target
                else:
                    self.good = False
                    return False
            else:
                kms_key_native_id = ""
            volume_native_id = record.get("volume_native_id", None)
            mount_name = record.get("name", None)
            rec_ebs_tags = record.get("tags", [{"key": "", "value": ""}])
            # print(f"ebs tag {rec_ebs_tags}")
            if len(rec_ebs_tags) < 1:
                # print("ebs correct tags")
                rec_ebs_tags = [{"key": "Name", "value": ""}]
                ebs_tags = rec_ebs_tags
            else:
                ebs_tags = record.get("tags", [{"key": "", "value": ""}])
            ebs_target_record = {
                "volume_native_id": volume_native_id,
                "kms_key_native_id": kms_key_native_id,
                "name": mount_name,
                "tags": ebs_tags
            }
            return ebs_target_record
        elif restore_type == "add_tag":
            # Not implemented.

            return False
        elif restore_type == "ebs_mapping":
            # Not Implemented Yet
            return False
            # if self.ebs_mapping_flag:
            #    [NewMountName,NewKMSKey,new_tags] = self.ebs_mapping(record)
            #    if NewKMSKey:
            #        kms_key_native_id = NewKMSKey
            #    else:
            #        kms_key_native_id = None
            #    volume_native_id = record.get("backup_record",{}).get("source_ebs_storage_list",{}).get("volume_native_id",None)
            #    mount_name = NewMountName
            #    ebs_tags = new_tags
            #    ebs_target_record = {
            #        "volume_native_id":volume_native_id,
            #        "kms_key_native_id":kms_key_native_id,
            #        "name":mount_name,
            #        "tags": new_tags
            #    }
            #    return ebs_target_record
            # else:
            #    return False
        else:
            return False

    def parse_ebs_from_record_list(self, record, restore_type="simple"):
        if restore_type == "simple":
            ebs_record_list = []
            for ebs_record in record.get("backup_record", {}).get("source_ebs_storage_list", []):
                rsp = self.parse_ebs_from_record(ebs_record)
                ebs_record_list.append(rsp)
            return ebs_record_list
        elif restore_type == "other":
            # No other methods supported yet
            return False
        else:
            self.good = False
            return False

    def parse_volumes_restore_target(self, record, restore_type="simple"):
        if restore_type == "simple":
            # simple restore means all default values and aws_az Not target_instance_native_id is used
            if self.aws_az_flag:
                aws_az = self.aws_az_target
                rsp = self.parse_ebs_from_record_list(record)
                if rsp:
                    ebs_block_device_mappings = rsp
                else:
                    self.good = False
                    return False
                if self.environment_id_flag:
                    environment_id = self.environment_id_target
                else:
                    self.good = False
                    return False
                volume_target_record = {
                    "aws_az": aws_az,
                    "ebs_block_device_mappings": ebs_block_device_mappings,
                    "environment_id": environment_id
                }
                return volume_target_record
            else:
                self.good = False
                return False
        elif restore_type == "other":
            # NOT IMPLEMENTED YET
            self.good = False
            return False
        else:
            self.good = False
            return False

    def parse_ebs_restore_target(self, record, restore_type="simple"):
        # simple restore means all default values and should_power_on = True
        if restore_type == "simple":
            if self.aws_az_flag:
                aws_az = self.aws_az_target
            else:
                self.good = False
                return False
            if self.environment_id_flag:
                environment_id = self.environment_id_target
            else:
                self.good = False
                return False
            ebs_tags = record.get("backup_record", {}).get("source_volume_tags", [])
            volume_restore_target = {
                "aws_az": aws_az,
                "environment_id": environment_id,
                "tags": ebs_tags
            }
            if record.get("backup_record", {}).get("source_encrypted_flag"):
                if self.kms_key_flag:
                    kms_key_native_id = self.kms_key_name_target
                    volume_restore_target["kms_key_native_id"] = kms_key_native_id
                else:
                    if self.debug > 3: print(f"EBS Restore requires KMS, but none provided in target {record} ")
                    self.good = False
                    return False
            if self.iops_flag:
                iops = self.iops_target
                volume_restore_target["iops"] = iops
            if self.volume_type_flag:
                type = self.volume_type_target
                volume_restore_target["type"] = type
            if self.debug > 7: print(f"parse_ebs_restore_target: volume restore target {volume_restore_target}")
            return volume_restore_target
        elif restore_type == "add_source_volume_tag":
            vol_id = record.get("backup_record", {}).get("source_volume_id", None)
            backup_id = record.get("backup_record", {}).get('source_backup_id', None)
            if vol_id and backup_id:
                new_tags = [
                    {"key": "org_volume_id", "value": vol_id},
                    {"key": "source_backup_id", "value": backup_id}
                ]
                self.source_volume_tag_flag = True
                self.source_volume_tags = new_tags
            else:
                if self.debug > 3: print(f"No Source Volume Id found {record} ")
                self.good = False
                return False
            if self.aws_az_flag:
                aws_az = self.aws_az_target
            else:
                self.good = False
                return False
            if self.environment_id_flag:
                environment_id = self.environment_id_target
            else:
                self.good = False
                return False
            ebs_tags = record.get("backup_record", {}).get("source_volume_tags", [])
            if new_tags:
                ebs_tags.extend(new_tags)
            volume_restore_target = {
                "aws_az": aws_az,
                "environment_id": environment_id,
                "tags": ebs_tags
            }
            if record.get("backup_record", {}).get("source_encrypted_flag"):
                if self.kms_key_flag:
                    kms_key_native_id = self.kms_key_name_target
                    volume_restore_target["kms_key_native_id"] = kms_key_native_id
                else:
                    if self.debug > 3: print(f"EBS Restore requires KMS, but none provided in target {record} ")
                    self.good = False
                    return False
            if self.iops_flag:
                iops = self.iops_target
                volume_restore_target["iops"] = iops
            if self.volume_type_flag:
                type = self.volume_type_target
                volume_restore_target["type"] = type

            return volume_restore_target

        elif restore_type == "other":
            # NOT IMPLEMENTED YET
            self.good = False
            return False
        else:
            self.good = False
            return False

    def get_source_volume_tags(self):
        if self.source_volume_tag_flag:
            return self.source_volume_tags
        else:
            return False

    def set_target_iops(self, value):
        self.iops_target = value
        self.iops_flag = True

    def clear_target_iops(self):
        self.iops_target = None
        self.iops_flag = False

    def set_target_volume_type(self, value):
        if value in self.allowed_volume_types:
            self.volume_type_target = value
            self.volume_type_flag = True
        else:
            self.volume_type_target = None
            self.volume_type_flag = False

    def clear_target_volume_type(self):
        self.volume_type_target = None
        self.volume_type_flag = False

    def set_target_kms_key_name(self, value):
        self.kms_key_name_target = value
        self.kms_key_flag = True

    def clear_target_kms_key_name(self):
        self.kms_key_name_target = None
        self.kms_key_flag = False

    def set_target_aws_az(self, value):
        self.aws_az_target = value
        self.aws_az_flag = True

    def clear_target_aws_az(self):
        self.aws_az_target = None
        self.aws_az_flag = False

    def set_backup_id(self, record):
        if record.get("backup_record", {}).get("source_backup_id", None):
            self.backup_id = record.get("backup_record", {}).get("source_backup_id", None)
            self.backup_id_flag = True
            return self.backup_id

        else:
            self.good = False
        return False

    def clear_backup_id(self):
        self.aws_az_target = None
        self.aws_az_flag = False

    def set_target_environment_id(self, account, region):
        if self.set_aws_account_id(account) and self.set_aws_region(region):
            env_id_api = EnvironmentId()
            env_id_api.set_url_prefix(self.url_prefix)
            env_id_api.set_token(self.token)
            env_id_api.set_debug(99)
            env_id_api.set_search_account_id(self.aws_account_id)
            env_id_api.set_search_region(self.aws_region)
            if env_id_api.run_api():
                rsp = env_id_api.environment_id_parse_results()
                del env_id_api
                if rsp:
                    self.environment_id_target = rsp
                    print(f"clumio env id {self.environment_id_target}")
                    self.environment_id_flag = True
                    return True
        del env_id_api
        self.good = False
        return False

    def clear_target_environment_id(self):
        self.environment_id_target = None
        self.environment_id_flag = False

    def set_payload(self, record, restore_type="simple"):
        if restore_type == "simple":
            if self.target_flag:
                backup_id = self.set_backup_id(record)
                ebs_restore_target = self.parse_ebs_restore_target(record)

                if backup_id and ebs_restore_target:
                    payload = {
                        "source": {"backup_id": backup_id},
                        "target": ebs_restore_target
                    }
                    self.payload = payload
                    self.payload_flag = True
                    return True
        elif restore_type == "add_source_volume_tag":
            # Add a Tag to the volume with the source volume id
            if self.target_flag:
                backup_id = self.set_backup_id(record)
                ebs_restore_target = self.parse_ebs_restore_target(record, "add_source_volume_tag")

                if backup_id and ebs_restore_target:
                    payload = {
                        "source": {"backup_id": backup_id},
                        "target": ebs_restore_target
                    }
                    self.payload = payload
                    self.payload_flag = True
                    return True
            return False

        self.good = False
        return False

    def run_restore_record(self, record, restore_type="simple"):
        if restore_type == "simple":
            if self.good:
                if self.debug > 3: print(f"run_restore_record -process payload {record} ")
                if self.set_payload(record):
                    result = self.exec_api()
                    if self.restore_task_list_flag:
                        task_item = {
                            "task": self.get_task_id(),
                            "volume_id": record.get("volume_id"),
                            "backup_id": record.get("backup_record", {}).get("source_backup_id", None)
                        }
                        self.set_restore_task_list(task_item)
                        print(f"task added {task_item}")

                    return result
            else:
                if self.debug > 3: print("run_restore_record good not set ")
                return False
        elif restore_type == "add_source_volume_tag":
            if self.good:
                if self.set_payload(record, "add_source_volume_tag"):
                    result = self.exec_api()
                    if self.restore_task_list_flag:
                        task_item = {
                            "task": self.get_task_id(),
                            "volume_id": record.get("volume_id"),
                            "backup_id": record.get("backup_record", {}).get("source_backup_id", None)
                        }
                        self.set_restore_task_list(task_item)
                        print(f"task added {task_item}")

                    return result
            else:
                return False

    def environment_id_parse_results(self, parse_type="id"):
        # ExampleParmsList = [
        #    "ID :backup_ids & InstanceIds & TimeStamp",
        #    "Restore :Parameters Needed for EC2 Restore",
        #    "Count : Number of Instances",
        #    "All : All Data"
        # ]

        if parse_type == "id":
            if self.environment_id_dict:
                if len(self.environment_id_dict.keys()) > 1:
                    return False
                else:
                    for key in self.environment_id_dict.keys():
                        return key
            else:
                return False
        elif parse_type == "all":
            if self.environment_id_dict:
                return self.environment_id_dict
            else:
                return False
        return False


class OnDemandBackupEC2(API):
    def __init__(self):
        super(OnDemandBackupEC2, self).__init__("007")
        self.backup_target_flag = False
        self.id = "007"
        self.log_activity_flag = False
        self.set_post()
        self.target_advanced_tier = None
        self.target_advanced_flag = False
        self.target_advanced_type = None
        self.target_retention_value = None
        self.target_retention_flag = False
        self.target_retention_units = None
        self.target_backup_type = None
        self.target_backup_type_flag = False
        self.target_region = None
        self.target_region_flag = False

        if api_dict.get(self.id, {}).get('type', None):
            if self.good:
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def set_target_retention(self, units, value):
        valid_units = ['hours', 'day', 'week', 'month', 'year']
        if units not in valid_units:
            self.target_retention_units = units
            try:
                value = int(value)
            except ValueError:
                if self.debug > 1: print(f"set_target_retention: invalid value {value}")
                return False
            self.target_retention_value = value
            self.target_retention_flag = True
            if self.debug > 5: print(f"set_target_retention: set units {units} and value {value}")
            return True
        else:
            if self.debug > 1: print(f"set_target_retention: invalid units {units}")
            return False

    def set_target_type(self, backup_type="clumio_backup"):
        valid_types = ["clumio_backup", "aws_snapshot"]
        if backup_type.lower() in valid_types:
            self.target_backup_type = backup_type.lower()
            self.target_backup_type_flag = True
        else:
            if self.debug > 1: print(f"set_target_type: invalid type {backup_type}")
            return False

    def set_target_ec2_advanced_tier(self, tier):
        self.target_advanced_type = "aws_ec2_instance_backup"
        valid_tiers = ["standard", "lite"]
        if tier.lower() in valid_tiers:
            self.target_advanced_tier = tier.lower()
            self.target_advanced_flag = True
        else:
            if self.debug > 1: print(f"set_target_ec2_advanced_tier: invalid tier {tier.lower()}")
            return False

    def set_target_region(self, region):
        if region in self.region_option:
            self.target_region = region
            self.target_region_flag = True
            if self.debug > 5: print(f"set_target_region: set region {region}")
            return True
        else:
            if self.debug > 1: print(f"set_target_region: invalid region {region}")
            return False

    def ec2_backup_from_record(self, r_list):
        if self.debug > 5: print(f"ec2_backup_from_record: number of records {len(r_list)}")
        if len(r_list) > 0:
            for record in r_list:
                if self.debug > 6: print(f"ec2_backup_from_record: backup record: {record}")
                if self.run_backup_record(record):
                    pass
                else:
                    if self.debug > 2: print(f"ec2_backup_from_record: failed api: {record}")
                    self.good = False
            return True
        else:
            if self.debug > 2: print("ec2_backup_from_record: no records found")
            self.good = False
            return False

    def set_payload(self, record):
        instance_id = record.get("instance_id", None)
        if instance_id:
            setting_dict = {}
            if self.target_retention_flag:
                setting_dict["retention_duration"] = {"unit": self.target_retention_units,
                                                      "value": self.target_retention_value}
            else:
                self.good = False
                if self.debug > 1: print("set_payload: retention not set")
                return False
            if self.target_advanced_flag:
                setting_dict["advanced_settings"] = {
                    self.target_advanced_type: {"backup_tier": self.target_advanced_tier}}
            if self.target_region_flag:
                setting_dict["backup_aws_region"] = self.target_region

            payload = {
                "settings": setting_dict,
                "instance_id": instance_id,
            }
            if self.target_backup_type_flag:
                payload["type"] = self.target_backup_type
            self.payload = payload
            self.payload_flag = True
            if self.debug > 6: print(f"set_payload: payload: {self.payload}")
            return True

        else:
            self.good = False
            if self.debug > 2: print(f"set_payload: no instance id {record}")
            return False

    def run_backup_record(self, record):
        if self.good:
            if self.set_payload(record):
                result = self.exec_api()
                return result
        else:
            return False


class RetrieveTask(API):
    def __init__(self):
        super(RetrieveTask, self).__init__("011")
        self.task_id = None
        self.task_id_flag = False
        self.set_get()
        self.url_suffix_string = None
        self.running_task_types = ["queued", "in_progress"]

        if api_dict.get(self.id, {}).get('type', None):
            if self.good:
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def build_url_suffix(self):
        if self.good:
            self.url_suffix_string = f"/{self.task_id}"
            self.set_url(self.url_suffix_string)
            return self.url_suffix_string

    def retrieve_task_id(self, task_id, run_type="wait"):
        # External - call to get the status of a task
        # default ("wait") run type will not return just the status when the task is finished
        # "one" run type will do a single check and return: 1) True or False if job is finished, 2) status, and 3) api response
        if task_id:
            self.task_id = task_id
            self.task_id_flag = True
            self.build_url_suffix()
            result = self.exec_api()
            print(f"task lookup {result}")
            status = result.get('status', "none")
            if run_type == "wait":
                if status in self.running_task_types:
                    not_found = True
                    iter_count = 0
                    while not_found:
                        time.sleep(10)
                        result = self.exec_api()
                        status = result.get('status', None)
                        if status in self.running_task_types:
                            iter_count += 1
                            if self.debug > 2: print(f"waiting on task {task_id} with current status of {status}")
                            if self.debug == 0: print(f"waiting on task {task_id} with current status of {status}")
                            if iter_count > 60:
                                raise Exception("task timed out")
                        else:
                            not_found = False
                            return status
                else:
                    return status
            elif run_type == "one":
                if status in self.running_task_types:
                    return False, status, result
                elif status:
                    return True, status, result
            else:
                return False, status, result
        else:
            self.task_id = None
            self.task_id_flag = False
        return False


class DynamoDBBackupList(API):
    def __init__(self):
        super(DynamoDBBackupList, self).__init__("012")
        self.id = "012"
        self.filter_list = []
        self.filter_expression = {}
        self.filter_expression_string = ""
        self.filter_expression_string_enc = ""
        self.limit_expression_string = ""
        self.start_expression_string = ""
        self.sort_expression_string = ""
        self.filter_flag = False
        self.limit_flag = False
        self.start_flag = False
        self.sort_flag = False
        self.limit = 100
        self.start = 1
        self.url_suffix_list = []
        self.url_suffix_list_enc = []
        self.url_suffix_string = ""
        self.url_suffix_string_enc = ""
        self.type_get = False
        self.type_post = False
        self.set_pagination()
        self.good_run_flag = True
        self.filter_expression = None
        self.start_day_time_stamp = None
        self.search_table_id = None
        self.search_type = None
        self.search_start_day = 0
        self.search_end_day = 10
        self.tag_name_match = ""
        self.tag_value_match = ""
        self.search_table_id = ""
        self.search_table_id_flag = False
        self.start_hour = "22"
        self.start_min = "58"
        self.start_sec = "58"
        self.end_date = None
        self.current_ddn_table_id_time_stamp = {}
        self.current_ddn_table_info = {}
        self.set_pagination()
        self.backup_type = "clumio_backup"
        self.ddn_any_region = False
        self.ddn_any_account = False
        # print(f"RestoreEC2 why api_dict {api_dict.get(self.id, {}).get('type', None)}")
        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def run_all(self):
        if self.search_type:
            day_start_delta = self.search_start_day
            day_end_delta = self.search_end_day
            # print(f"the delta start end {day_start_delta} {day_end_delta}")
            if self.search_type == "backwards":
                if day_start_delta > day_end_delta:
                    self.error_msg = "Search Start day is older then search end day"
                    return False
                today = datetime.now().astimezone(timezone.utc)
                start_delta = timedelta(days=day_start_delta)
                end_delta = timedelta(days=day_end_delta)
                start_date = today - start_delta
                self.end_date = today - end_delta
                # #print(start_date)
                if start_date.year < 10:
                    start_date_year = "0" + str(start_date.year)
                else:
                    start_date_year = str(start_date.year)
                if start_date.month < 10:
                    start_date_month = "0" + str(start_date.month)
                else:
                    start_date_month = str(start_date.month)
                if start_date.day < 10:
                    start_date_day = "0" + str(start_date.day)
                else:
                    start_date_day = str(start_date.day)
                self.start_day_time_stamp = f"{start_date_year}-{start_date_month}-{start_date_day}T{self.start_hour}:{self.start_min}:{self.start_sec}Z"
            elif self.search_type == "forwards":
                today = datetime.now().astimezone(timezone.utc)
                end_delta = timedelta(days=day_end_delta)
                end_date = today - end_delta
                # #print(start_date)
                if end_date.year < 10:
                    end_date_year = "0" + str(end_date.year)
                else:
                    end_date_year = str(end_date.year)
                if end_date.month < 10:
                    end_date_month = "0" + str(end_date.month)
                else:
                    end_date_month = str(end_date.month)
                if end_date.day < 10:
                    end_date_day = "0" + str(end_date.day)
                else:
                    end_date_day = str(end_date.day)
                # start_date_month = str(start_date.month)
                # start_date_day = str(start_date.day)
                self.start_day_time_stamp = f"{end_date_year}-{end_date_month}-{end_date_day}T{self.start_hour}:{self.start_min}:{self.start_sec}Z"
            self.set_filter("start_timestamp", self.filter_expression, self.start_day_time_stamp)
        if self.search_table_id_flag:
            self.set_filter("table_id", "$eq", self.search_table_id)
        self.results = []
        first_results = self.exec_api()
        if self.good:
            items = first_results.get("_embedded", {}).get("items", {})
            for i in items:
                check = self.pass_check(i)
                if check:
                    self.current_ddn_table_info[check] = i
        else:
            return False
        if self.total_pages_count:
            if self.total_pages_count > 1:
                for i in range(2, self.total_pages_count):
                    self.set_page_start(i)
                    next_result = self.exec_api()
                    if self.good:
                        items = next_result.get("_embedded", {}).get("items", {})
                        for i in items:
                            check = self.pass_check(i)
                            if check:
                                self.current_ddn_table_info[check] = i
                    else:
                        return False
                    # #print(f"read page {i}")
        return len(self.current_ddn_table_info)

    def pass_check(self, response):
        if not self.ddn_any_region:
            aws_region = response.get("aws_region", None)
            if not self.ddn_any_account:
                aws_account_id = response.get("account_native_id", None)
                if not (aws_region == self.aws_region and aws_account_id == self.aws_account_id):
                    return False
            else:
                if not aws_region == self.aws_region:
                    return False
        elif not self.ddn_any_account:
            aws_account_id = response.get("account_native_id", None)
            if not aws_account_id == self.aws_account_id:
                return False

        if self.aws_tag_flag:
            tags = response.get("tags", None)
            found_tag = False
            for tag in tags:
                if tag.get("key", None) == self.aws_tag_key and tag.get("value", None) == self.aws_tag_value:
                    found_tag = True
            if not found_tag:
                return False
        time_stamp = response.get("start_timestamp", None)
        clumio_table_id = response.get("table_id", None)
        new_date = datetime.fromisoformat(time_stamp[:-1]).astimezone(timezone.utc)
        if self.search_type == "backwards":
            # new_date = datetime.fromisoformat(time_stamp[:-1]).astimezone(timezone.utc)
            if self.current_ddn_table_id_time_stamp.get(clumio_table_id, None):
                if new_date > self.current_ddn_table_id_time_stamp.get(clumio_table_id, None):
                    self.current_ddn_table_id_time_stamp[clumio_table_id] = new_date
                    return clumio_table_id
                else:
                    return False
            else:
                if new_date > self.end_date:
                    self.current_ddn_table_id_time_stamp[clumio_table_id] = new_date
                    return clumio_table_id
                else:
                    return False
        else:
            if self.current_ddn_table_id_time_stamp.get(clumio_table_id, None):
                if new_date > self.current_ddn_table_id_time_stamp.get(clumio_table_id, None):
                    self.current_ddn_table_id_time_stamp[clumio_table_id] = new_date
                    return clumio_table_id
                else:
                    return False
            else:
                self.current_ddn_table_id_time_stamp[clumio_table_id] = new_date
                return clumio_table_id

    def ddn_parse_results(self, parse_type="basic"):
        # External function - must be call to return results of API after applying any parse values.
        # Returns data in format specified by parse_type

        ExampleParmsList = [
            "id :backup_ids & TableIds & TimeStamp",
            "basic :Parameters needed to do a basic restore"
            "restore :Parameters Needed for DynamoDB Restore with all options",
            "count : Number of Tables",
            "all : returns all data for the items"
        ]
        records = []
        # print("start parse")
        if self.debug > 2: print(f"data results returned in {parse_type} format")
        if parse_type == "id":
            for clumio_table_id in self.current_ddn_table_info.keys():
                rec = {self.current_ddn_table_info[clumio_table_id].get("table_name", None): [clumio_table_id,
                                     self.current_ddn_table_info[clumio_table_id].get("start_timestamp", None),
                                     self.current_ddn_table_info[clumio_table_id].get("type", None)]}
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        elif parse_type == "all":
            for clumio_table_id in self.current_ddn_table_info.keys():
                rec = self.current_ddn_table_info[clumio_table_id]
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        elif parse_type == "basic":
            # print(f"in parse restore{self.debug} {self.current_ddn_table_info}")
            for clumio_table_id in self.current_ddn_table_info.keys():
                if self.debug > 2: print(f"ddn record {self.current_ddn_table_info[clumio_table_id]}")
                # Skip volumes not backed up to Clumio (i.e. in account snapshots)
                if self.current_ddn_table_info[clumio_table_id].get("type", None) == self.backup_type:
                    rec = {"table_name": self.current_ddn_table_info[clumio_table_id].get("table_name", None),
                               "backup_record": {
                                   "source_backup_id": self.current_ddn_table_info[clumio_table_id].get("id", None),
                                   "source_table_name": self.current_ddn_table_info[clumio_table_id].get("table_name",
                                                                                                         None),
                                   "source_ddn_tags": self.current_ddn_table_info[clumio_table_id].get("tags", []),
                                   "source_sse_specification": self.current_ddn_table_info[clumio_table_id].get("sse_specification", {}),
                                   "source_provisioned_throughput": self.current_ddn_table_info[clumio_table_id].get(
                                       "provisioned_throughput", {}),
                                   "source_billing_mode": self.current_ddn_table_info[clumio_table_id].get("billing_mode",
                                                                                                           None),
                                   "source_table_class": self.current_ddn_table_info[clumio_table_id].get("table_class", None),
                                   "source_expire_time": self.current_ddn_table_info[clumio_table_id].get(
                                       "expiration_timestamp",
                                       None),
                                   "source_global_table_version": self.current_ddn_table_info[clumio_table_id].get("global_table_version",None),
                                   "source_global_secondary_indexes": self.current_ddn_table_info[clumio_table_id].get(
                                       "global_secondary_indexes", None),
                                   "source_local_secondary_indexes": self.current_ddn_table_info[clumio_table_id].get(
                                       "local_secondary_indexes", None),
                                   "source_replicas": self.current_ddn_table_info[clumio_table_id].get("replicas", None),
                               }
                           }
                    records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        elif parse_type == "restore":
            # Not implemented yet
            return {}
        elif parse_type == "count":
            return len(self.current_ddn_table_info.keys())
        else:
            return {}

    def ddn_search_by_tag(self, Key, Value):
        # External function - must be set to parse results by AWS resource tag
        # Call functions from the parent class

        self.set_aws_tag_key(Key)
        self.set_aws_tag_value(Value)

    def set_page_size(self, x):
        # External function - must be called to set number of items returned per page/run

        if self.query_parms_flag:
            if "limit" in self.query_parms.keys():
                self.limit = x
                self.limit_expression_string = "limit=" + str(self.limit)
                self.limit_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def set_page_start(self, x):
        # External function - must be called to set pagnation page to return first = default is first page (why would you want to chagne that)

        if self.query_parms_flag:
            if "start" in self.query_parms.keys():
                self.start = x
                self.start_expression_string = "start=" + str(self.start)
                self.start_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def build_url_suffix(self):
        # Internal Function - do not use

        self.url_suffix_list = []
        if self.good:
            if self.filter_flag:
                self.url_suffix_list.append(self.filter_expression_string_enc)

            if self.sort_flag:
                self.url_suffix_list.append(self.sort_expression_string)

            if self.limit_flag:
                self.url_suffix_list.append(self.limit_expression_string)

            if self.start_flag:
                self.url_suffix_list.append(self.start_expression_string)

            self.url_suffix_string = "?" + "&".join(self.url_suffix_list)
            String = self.url_suffix_string
            # #print(String)
            self.set_url(String)
            return self.url_suffix_string

    def set_filter(self, filter_name, filter_expression, filter_value):
        # Internal Function - do not use

        filter_expression_dict = {}
        # print(f"setFilter 01 {filter_name}")
        if self.query_parms_flag:
            # print("setFilter 02")
            if filter_name in self.query_parms.get("filter", {}).keys():
                # print("setFilter 03")
                if filter_expression in self.query_parms.get("filter", {}).get(filter_name, []):
                    # print("setFilter 04")
                    self.filter_list.append([filter_name, filter_expression, filter_value])
                    for i in self.filter_list:
                        ##print(f"in set_filter 05 {i}")
                        filter_expression_dict[i[0]] = {i[1]: i[2]}
                    self.filter_expression_string = "filter=" + json.dumps(filter_expression_dict,
                                                                           separators=(',', ':'))
                    self.filter_expression_string_enc = "filter=" + urllib.parse.quote(
                        json.dumps(filter_expression_dict, separators=(',', ':')))
                    self.filter_flag = True
                    return self.build_url_suffix()
        self.set_bad()
        return None

    def set_sort(self, sort):
        # External function - must be called to set search direction

        # Sort options "-start_timestamp" and "start_timestamp"
        good_sort_name = ["forward", "backward"]
        if sort == "forward":
            sort_name = "start_timestamp"
        elif sort == "backward":
            sort_name = "-start_timestamp"
        else:
            return False

        if self.query_parms_flag:
            if sort_name in self.query_parms.get("sort", []):
                self.sort_expression_string = "sort=" + sort_name
                self.sort_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return False

    def set_search_table_id(self, table_id):
        # External function - must be called to set search table name

        # Search by table id not implemented yet
        return False
        self.search_table_id_flag = True
        self.search_table_id = table_id

    def set_search_backup_type(self, backup_type):
        # External function - must be called to set backup type

        # Backup type of "aws_snashot" not implemented
        return False
        good_backup_types = ["clumio_backup", "aws_snapshot"]
        if backup_type in good_backup_types:
            self.backup_type = backup_type
            return True
        else:
            return False

    def set_search_forwards_from_offset(self, end_day_offset, search_type="forwards"):
        self.search_type = search_type
        self.set_search_end_day(end_day_offset)
        self.filter_expression = "$gt"
        self.start_hour = "00"
        self.start_min = "00"
        self.start_sec = "00"

    def set_search_backwards_from_offset(self, start_day_offset, end_day_offset, search_type="backwards"):
        self.search_type = search_type
        self.set_search_end_day(end_day_offset)
        self.set_search_start_day(start_day_offset)
        self.filter_expression = "$lte"
        self.start_hour = "23"
        self.start_min = "59"
        self.start_sec = "59"

    def set_search_start_day(self, start_day):
        # External function - must be called to set search start day

        try:
            int(start_day)
            self.search_start_day = start_day
        except ValueError:
            return False
        return True

    def set_search_all_regions(self):
        # External function - set search for all AWS regions

        self.ddn_any_region = True

    def set_search_all_accounts(self):
        # External function - set search for all AWS accounts

        self.ddn_any_account = True

    def set_search_end_day(self, end_day):
        # External function - must be called to set search end day

        try:
            int(end_day)
            self.search_end_day = end_day
        except ValueError:
            return False
        return True


class RestoreDDN(API):
    def __init__(self):
        super(RestoreDDN, self).__init__("013")
        self.id = "013"
        self.filter_list = []
        self.filter_expression_string = ""
        self.filter_expression_string_enc = ""
        self.limit_expression_string = ""
        self.start_expression_string = ""
        self.embed_expression_string = ""
        self.filter_flag = False
        self.limit_flag = False
        self.start_flag = False
        # self.sort_flag = False
        self.embed_flag = False
        self.limit = 100
        self.start = 1
        self.type_get = False
        self.type_post = True
        self.good_run_flag = True
        self.filter_expression = None
        # Values associated with restore target
        self.provisioned_throughput_flag = False
        self.provisioned_throughput_read = 1
        self.provisioned_throughput_write = 1
        self.billing_mode_flag = False
        self.billing_mode = "PROVISIONED"
        self.sse_flag = False
        self.sse = {}
        self.kms_key_flag = False
        self.kms_key_name_target = None
        self.global_secondary_indexes_flag = False
        self.global_secondary_indexes = {}
        self.local_secondary_indexes_flag = False
        self.local_secondary_indexes = {}
        self.table_class_flag = False
        self.table_class = "STANDARD"
        self.target_table_name_flag = False
        self.target_table_name = None

        self.allowed_volume_types = ["gp2", "gp3"]
        self.aws_az_target = None
        self.aws_az_flag = False
        self.environment_id_flag = False
        self.environment_id = None
        self.target_flag = False
        self.log_activity_flag = False
        self.set_post()
        self.source_ddn_tag_flag = True
        self.source_ddn_tags = []
        self.restore_task_list_flag = False
        self.restore_task_list = []
        self.import_to_cft_flag = False
        self.import_to_cft_key = "clumio-import-to-cft"
        # print(f"RestoreEC2 why api_dict {api_dict.get(self.id, {}).get('type', None)}")
        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def set_restore_task_list(self, task_item):
        # Internal do not use

        self.restore_task_list.append(task_item)
        return True

    def get_restore_task_list(self):
        # External function - must be called to retrun task list

        list_task = self.restore_task_list
        return list_task

    def save_restore_task(self):
        # External function - must be called to save restore task ids

        self.restore_task_list_flag = True
        return True

    def set_target_for_ddn_restore(self, target, restore_type="simple"):
        # External function - must be called to set restore target values

        EXAMPLE = {
            "account": "",
            "region": "",
            "table_name": "",
        }
        if self.debug > 1: print(f"set_target_for_ddn_restore - Good value {self.good}")
        if restore_type == "simple":
            if self.debug > 3: print(f"set_target_for_ddn_restore target: {target}, restore_type: {restore_type} ")
            if self.set_target_environment_id(target.get("account", None), target.get("region", None)):
                # Required
                if target.get("table_name", None):
                    self.set_target_table_name(target.get("table_name"))
                else:
                    if self.debug > 3: print("set_target_for_ddn_restore could not set table name ")
                    self.set_error_msg(f"set_target_for_ddn_restore: could not set target table_name {target.get("table_name", None)}")
                    self.good = False
                    return False
                # Optional

                #Not implemented yet

                self.target_flag = True
                return True
            else:
                if self.debug > 3: print("set_target_for_ddn_restore could not set target account and region ")
                self.set_error_msg("set_target_for_ddn_restore: could not set target account or region")
                self.good = False
                return False

        elif restore_type == "other":

            self.good = False
            return False
        else:
            self.good = False
            return False

    def ddn_restore_from_record(self, r_list, restore_type="simple"):
        # External - initiates restore using restore record (created by DDN Backup List API)

        if self.debug > 1: print(f"ddn_restore_from_record - Good value {self.good}")
        if len(r_list) > 0:
            # print(f"Restore r_list {r_list}")
            for record in r_list:
                if self.debug > 1: print(f"record to restore: {record}")
                # Check Expire Time
                if self.check_expire_time(record.get('backup_record', {}).get("source_expire_time", None)):
                    run_results = self.run_restore_record(record, restore_type)
                    if run_results.get('errors',None):
                        return [False, f"restore failed for {record.get("table_name", None)}: {run_results}"]
                    if self.debug > 1: print(f"restore results for {record.get("table_name", None)} {run_results}")
                    if self.log_activity_flag:
                        pass
                else:
                    if self.debug > 3:
                        print(f"Backup has expired for {record}")
                    pass
            return [True, ""]
        else:
            self.good = False
            if self.debug > 3:
                print("DDN List has no records to restore")
            return [False, "No tables to restore"]

    def check_expire_time(self, expire_time):
        try:
            expire_date = datetime.fromisoformat(expire_time[:-1]).astimezone(timezone.utc)
        except ValueError:
            if self.debug > 3: print(f"Expire date in invalid format: {expire_time}")
            return False
        if expire_date < datetime.now().astimezone(timezone.utc):
            if self.debug > 3: print(f"Expired: {expire_time}")
            return False
        else:
            if self.debug > 3: print(f"backup has not expired {expire_time}")
            return True

    def parse_ddn_restore_target(self, record, restore_type="simple"):
        # simple restore means all default values and should_power_on = True
        if self.target_table_name_flag:
            old_name = record.get("table_name",None)
            new_name = old_name + self.target_table_name
        else:
            self.good = False
            return False
        if restore_type == "simple":
            if self.environment_id_flag:
                environment_id = self.environment_id_target
            else:
                self.good = False
                return False
            ddn_tags = record.get("backup_record", {}).get("source_ddn_tags", [])
            if self.import_to_cft_flag:
                new_tags = [
                    {"key": self.import_to_cft_key, "value": new_name},
                    {"key": "source_backup_id", "value": record.get("backup_record", {}).get("source_backup_id", None)}
                ]
                ddn_tags.extend(new_tags)
            ddn_restore_target = {
                "table_name": new_name,
                "environment_id": environment_id,
                "tags": ddn_tags
            }
            if record.get("backup_record", {}).get("source_sse_specification", {}):
                ddn_restore_target["sse_specification"] = record.get("backup_record", {}).get("sse_specification", {})
            if record.get("backup_record", {}).get("source_provisioned_throughput", {}):
                ddn_restore_target["provisioned_throughput"] = record.get("backup_record", {}).get("source_provisioned_throughput",{})
            if record.get("backup_record", {}).get("source_billing_mode", {}):
                ddn_restore_target["billing_mode"] = record.get("backup_record", {}).get("source_billing_mode",{})
            if record.get("backup_record", {}).get("source_table_class", {}):
                ddn_restore_target["table_class"] = record.get("backup_record", {}).get("source_table_class",{})
            if record.get("backup_record", {}).get("source_global_table_version", {}):
                ddn_restore_target["global_table_version"] = record.get("backup_record", {}).get("source_global_table_version",{})
            if record.get("backup_record", {}).get("source_global_secondary_indexes", {}):
                ddn_restore_target["global_secondary_indexes"] = record.get("backup_record", {}).get("source_global_secondary_indexes",{})
            if record.get("backup_record", {}).get("source_local_secondary_indexes", {}):
                ddn_restore_target["local_secondary_indexes"] = record.get("backup_record", {}).get("source_local_secondary_indexes",{})
            if record.get("backup_record", {}).get("source_replicas", {}):
                ddn_restore_target["replicas"] = record.get("backup_record", {}).get("source_replicas",{})
            if self.debug > 5: print(f"parse_ddn_restore_target: target {ddn_restore_target}")

            return ddn_restore_target
        elif restore_type == "basic":
            if self.environment_id_flag:
                environment_id = self.environment_id_target
            else:
                self.good = False
                return False
            ddn_tags = record.get("backup_record", {}).get("source_ddn_tags", [])
            if self.import_to_cft_flag:
                new_tags = [
                    {"key": self.import_to_cft_key, "value": new_name},
                    {"key": "source_backup_id", "value": record.get("backup_record", {}).get("source_backup_id", None)}
                ]
                ddn_tags.extend(new_tags)
            ddn_restore_target = {
                "table_name": new_name,
                "environment_id": environment_id,
                "tags": ddn_tags
            }

            return ddn_restore_target
        elif restore_type == "other":
            # NOT IMPLEMENTED YET
            self.good = False
            return False
        else:
            self.good = False
            return False

    def get_source_ddn_tags(self):
        if self.source_ddn_tag_flag:
            return self.source_ddn_tags
        else:
            return False

    def set_target_table_name(self, value):
        self.target_table_name = value
        self.target_table_name_flag = True

    def set_target_kms_key_name(self, value):
        self.kms_key_name_target = value
        self.kms_key_flag = True

    def set_backup_id(self, record):
        if record.get("backup_record", {}).get("source_backup_id", None):
            self.backup_id = record.get("backup_record", {}).get("source_backup_id", None)
            self.backup_id_flag = True
            return self.backup_id

        else:
            self.good = False
        return False

    def set_target_environment_id(self, account, region):
        if self.set_aws_account_id(account) and self.set_aws_region(region):
            env_id_api = EnvironmentId()
            env_id_api.set_url_prefix(self.url_prefix)
            env_id_api.set_token(self.token)
            env_id_api.set_debug(99)
            env_id_api.set_search_account_id(self.aws_account_id)
            env_id_api.set_search_region(self.aws_region)
            if env_id_api.run_api():
                rsp = env_id_api.environment_id_parse_results()
                del env_id_api
                if rsp:
                    self.environment_id_target = rsp
                    if self.debug > 3: print(f"clumio env id {self.environment_id_target}")
                    self.environment_id_flag = True
                    return True
        del env_id_api
        self.good = False
        return False

    def set_payload(self, record, restore_type="simple"):
        if restore_type == "simple":
            if self.target_flag:
                backup_id = self.set_backup_id(record)
                ddn_restore_target = self.parse_ddn_restore_target(record)

                if backup_id and ddn_restore_target:
                    payload = {
                        "source": {
                                "securevault_backup":
                                    {
                                        "backup_id": backup_id
                                    }
                                },
                        "target": ddn_restore_target
                    }
                    self.payload = payload
                    self.payload_flag = True
                    return True
        else:
            self.good = False
            return False

    def run_restore_record(self, record, restore_type="simple"):
        if restore_type == "simple":
            if self.good:
                if self.debug > 3: print(f"run_restore_record -process payload {record} ")
                if self.set_payload(record):
                    result = self.exec_api()
                    if self.restore_task_list_flag:
                        task_item = {
                            "task": self.get_task_id(),
                            "table_name": record.get("table_name"),
                            "backup_id": record.get("backup_record", {}).get("source_backup_id", None)
                        }
                        self.set_restore_task_list(task_item)
                        if self.debug > 3: print(f"task added {task_item}")

                    return result
            else:
                if self.debug > 3: print("run_restore_record good not set ")
                return False
        else:
            return False

    def set_clumio_import_to_cft(self,key=None):
        # External function - must be called to set import to cft
        self.import_to_cft_flag = True
        if key:
            self.import_to_cft_key = key


class RDSBackupList(API):
    def __init__(self):
        super(RDSBackupList, self).__init__("014")
        self.id = "014"
        self.filter_list = []
        self.filter_expression = {}
        self.filter_expression_string = ""
        self.filter_expression_string_enc = ""
        self.limit_expression_string = ""
        self.start_expression_string = ""
        self.sort_expression_string = ""
        self.filter_flag = False
        self.limit_flag = False
        self.start_flag = False
        self.sort_flag = False
        self.limit = 100
        self.start = 1
        self.url_suffix_list = []
        self.url_suffix_list_enc = []
        self.url_suffix_string = ""
        self.url_suffix_string_enc = ""
        self.type_get = False
        self.type_post = False
        self.set_pagination()
        self.good_run_flag = True
        self.filter_expression = None
        self.start_day_time_stamp = None
        self.search_resource_id = None
        self.search_type = None
        self.search_start_day = 0
        self.search_end_day = 10
        self.tag_name_match = ""
        self.tag_value_match = ""
        self.search_resource_id_flag = False
        # self.filter_expression = "$lte"
        self.start_hour = "22"
        self.start_min = "58"
        self.start_sec = "58"
        self.end_date = None
        # self.set_sort("-start_timestamp")
        self.current_rds_resource_id_time_stamp = {}
        self.current_rds_resource_info = {}
        self.set_pagination()
        self.backup_type = "aws_rds_resource_rolling_backup"
        # print(f"RestoreEC2 why api_dict {api_dict.get(self.id, {}).get('type', None)}")
        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def run_all(self):
        if self.search_type:
            day_start_delta = self.search_start_day
            day_end_delta = self.search_end_day
            # print(f"the delta start end {day_start_delta} {day_end_delta}")
            if self.search_type == "backwards":
                if day_start_delta > day_end_delta:
                    self.error_msg = "Search Start day is older then search end day"
                    return False
                today = datetime.now().astimezone(timezone.utc)
                start_delta = timedelta(days=day_start_delta)
                end_delta = timedelta(days=day_end_delta)
                start_date = today - start_delta
                self.end_date = today - end_delta
                # #print(start_date)
                if start_date.year < 10:
                    start_date_year = "0" + str(start_date.year)
                else:
                    start_date_year = str(start_date.year)
                if start_date.month < 10:
                    start_date_month = "0" + str(start_date.month)
                else:
                    start_date_month = str(start_date.month)
                if start_date.day < 10:
                    start_date_day = "0" + str(start_date.day)
                else:
                    start_date_day = str(start_date.day)
                self.start_day_time_stamp = f"{start_date_year}-{start_date_month}-{start_date_day}T{self.start_hour}:{self.start_min}:{self.start_sec}Z"
            elif self.search_type == "forwards":
                today = datetime.now().astimezone(timezone.utc)
                end_delta = timedelta(days=day_end_delta)
                end_date = today - end_delta
                # #print(start_date)
                if end_date.year < 10:
                    end_date_year = "0" + str(end_date.year)
                else:
                    end_date_year = str(end_date.year)
                if end_date.month < 10:
                    end_date_month = "0" + str(end_date.month)
                else:
                    end_date_month = str(end_date.month)
                if end_date.day < 10:
                    end_date_day = "0" + str(end_date.day)
                else:
                    end_date_day = str(end_date.day)
                # start_date_month = str(start_date.month)
                # start_date_day = str(start_date.day)
                self.start_day_time_stamp = f"{end_date_year}-{end_date_month}-{end_date_day}T{self.start_hour}:{self.start_min}:{self.start_sec}Z"
            self.set_filter("start_timestamp", self.filter_expression, self.start_day_time_stamp)
        if self.search_resource_id_flag:
            self.set_filter("resource_id", "$eq", self.search_resource_id)
        self.results = []
        first_results = self.exec_api()
        if self.good:
            items = first_results.get("_embedded", {}).get("items", {})
            for i in items:

                check = self.pass_check(i)
                if check:

                    self.current_rds_resource_info[check] = i
        else:
            return False

        if self.total_pages_count:
            if self.total_pages_count > 1:
                for i in range(2, self.total_pages_count):
                    self.set_page_start(i)
                    aws_region = self.exec_api()
                    if self.good:
                        items = aws_region.get("_embedded", {}).get("items", {})
                        for i in items:
                            check = self.pass_check(i)
                            if check:
                                self.current_rds_resource_info[check] = i
                    else:
                        return False
                    # #print(f"read page {i}")
        return len(self.current_rds_resource_info)

    def pass_check(self, response):
        aws_region = response.get("aws_region", None)
        aws_account_id = response.get("account_native_id", None)
        if not (aws_region == self.aws_region and aws_account_id == self.aws_account_id):
            if self.debug > 7: print(f"failed pass check issue with account and/or region found {aws_region} wanted {self.aws_region} found {aws_account_id} wanted {self.aws_account_id}")
            return False
        if self.aws_tag_flag:
            tags = response.get("tags", None)
            found_tag = False
            for tag in tags:
                if tag.get("key", None) == self.aws_tag_key and tag.get("value", None) == self.aws_tag_value:
                    found_tag = True
            if not found_tag:
                if self.debug > 7: print(
                    f"failed pass check issue with tags {self.aws_tag_key} {self.aws_tag_value}")
                return False
        time_stamp = response.get("start_timestamp", None)
        clumio_resource_id = response.get("resource_id", None)
        new_date = datetime.fromisoformat(time_stamp[:-1]).astimezone(timezone.utc)
        if self.search_type == "backwards":
            # new_date = datetime.fromisoformat(time_stamp[:-1]).astimezone(timezone.utc)
            if self.current_rds_resource_id_time_stamp.get(clumio_resource_id, None):
                if new_date > self.current_rds_resource_id_time_stamp.get(clumio_resource_id, None):
                    self.current_rds_resource_id_time_stamp[clumio_resource_id] = new_date
                    return clumio_resource_id
                else:
                    if self.debug > 7: print(
                        f"failed pass check issue with offset time {new_date}")
                    return False
            else:
                if new_date > self.end_date:
                    self.current_rds_resource_id_time_stamp[clumio_resource_id] = new_date
                    return clumio_resource_id
                else:
                    if self.debug > 7: print(
                        f"failed pass check issue with before time {new_date}")
                    return False
        else:  # search_type = before
            if self.current_rds_resource_id_time_stamp.get(clumio_resource_id, None):
                if new_date > self.current_rds_resource_id_time_stamp.get(clumio_resource_id, None):
                    self.current_rds_resource_id_time_stamp[clumio_resource_id] = new_date
                    return clumio_resource_id
                else:
                    if self.debug > 7: print(
                        f"failed pass check issue with end date {new_date}")
                    return False
            else:
                self.current_rds_resource_id_time_stamp[clumio_resource_id] = new_date
                return clumio_resource_id

    def rds_parse_results(self, parse_type):
        ExampleParmsList = [
            "ID :backup_ids & resource_ids & time_stamp",
            "Restore :Parameters Needed for RDS Restore",
            "Count : Number of resrouce_ids",
            "All : All Data"
        ]
        records = []
        print("start parse")
        if parse_type == "id":
            for resource in self.current_rds_resource_info.keys():
                rec = {"id_record": [self.current_rds_resource_info[resource].get("database_native_id", None),
                                     self.current_rds_resource_info[resource].get("id", None),
                                     self.current_rds_resource_info[resource].get("start_timestamp", None),
                                     self.current_rds_resource_info[resource].get("type", None)]}
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        elif parse_type == "all":
            for resource in self.current_rds_resource_info.keys():
                rec = {"item": self.current_rds_resource_info[resource]}
                records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        elif parse_type == "restore":
            print(f"in parse restore {self.debug} {self.current_rds_resource_info}")
            for resource in self.current_rds_resource_info.keys():
                if self.debug > 2: print(f"rds record {self.current_rds_resource_info[resource]}")
                # Skip volumes not backed up to Clumio (i.e. in account snapshots)
                if self.current_rds_resource_info[resource].get("type", None) == self.backup_type:
                    if self.current_rds_resource_info[resource].get("kms_key_native_id"):
                        is_encrypted = True
                    else:
                        is_encrypted = False

                    rec = {"resource_id": self.current_rds_resource_info[resource].get("database_native_id", None),
                           "backup_record": {
                               "source_backup_id": self.current_rds_resource_info[resource].get("id", None),
                               "source_resource_id": self.current_rds_resource_info[resource].get("database_native_id",
                                                                                                  None),
                               "source_resource_tags": self.current_rds_resource_info[resource].get("tags", []),
                               "source_encrypted_flag": is_encrypted,
                               "source_instances": self.current_rds_resource_info[resource].get("instances", []),
                               "source_instance_class": self.current_rds_resource_info[resource].get("instances", [])[
                                   0].get("class", None),
                               "source_is_publicly_accessible":
                                   self.current_rds_resource_info[resource].get("instances", [])[0].get(
                                       "is_publicly_accessible", None),
                               "source_subnet_group_name": self.current_rds_resource_info[resource].get(
                                   "subnet_group_name", None),
                               "source_kms": self.current_rds_resource_info[resource].get("kms_key_native_id", None),
                               "source_expire_time": self.current_rds_resource_info[resource].get(
                                   "expiration_timestamp", None),
                           }
                           }
                    records.append(rec)
            clumio_resource_list_dict = {"records": records}
            if self.dump_to_file_flag:
                # #print("in the dumpining 01")
                if not self.data_dump(clumio_resource_list_dict):
                    return False
            return clumio_resource_list_dict
        return {}

    def rds_search_by_tag(self, Key, Value):
        self.set_aws_tag_key(Key)
        self.set_aws_tag_value(Value)

    def set_page_size(self, x):
        if self.query_parms_flag:
            if "limit" in self.query_parms.keys():
                self.limit = x
                self.limit_expression_string = "limit=" + str(self.limit)
                self.limit_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def set_page_start(self, x):
        if self.query_parms_flag:
            if "start" in self.query_parms.keys():
                self.start = x
                self.start_expression_string = "start=" + str(self.start)
                self.start_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def build_url_suffix(self):
        self.url_suffix_list = []
        if self.good:
            if self.filter_flag:
                self.url_suffix_list.append(self.filter_expression_string_enc)

            if self.sort_flag:
                self.url_suffix_list.append(self.sort_expression_string)

            if self.limit_flag:
                self.url_suffix_list.append(self.limit_expression_string)

            if self.start_flag:
                self.url_suffix_list.append(self.start_expression_string)

            self.url_suffix_string = "?" + "&".join(self.url_suffix_list)
            String = self.url_suffix_string
            # #print(String)
            self.set_url(String)
            return self.url_suffix_string

    def set_filter(self, filter_name, filter_expression, filter_value):
        filter_expression_dict = {}
        # print(f"setFilter 01 {filter_name}")
        if self.query_parms_flag:
            # print("setFilter 02")
            if filter_name in self.query_parms.get("filter", {}).keys():
                # print("setFilter 03")
                if filter_expression in self.query_parms.get("filter", {}).get(filter_name, []):
                    # print("setFilter 04")
                    self.filter_list.append([filter_name, filter_expression, filter_value])
                    for i in self.filter_list:
                        ##print(f"in set_filter 05 {i}")
                        filter_expression_dict[i[0]] = {i[1]: i[2]}
                    self.filter_expression_string = "filter=" + json.dumps(filter_expression_dict,
                                                                           separators=(',', ':'))
                    self.filter_expression_string_enc = "filter=" + urllib.parse.quote(
                        json.dumps(filter_expression_dict, separators=(',', ':')))
                    self.filter_flag = True
                    return self.build_url_suffix()
        self.set_bad()
        return None

    def set_sort(self, sortName):

        if self.query_parms_flag:
            if sortName in self.query_parms.get("sort", []):
                self.sort_expression_string = "sort=" + sortName
                self.sort_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def set_search_resource_id(self, resource_id):
        self.search_resource_id_flag = True
        self.search_resource_id = resource_id


    def set_search_forwards_from_offset(self, end_day_offset, search_type="forwards"):
        self.search_type = search_type
        self.set_search_end_day(end_day_offset)
        self.filter_expression = "$gt"
        self.start_hour = "00"
        self.start_min = "00"
        self.start_sec = "00"

    def set_search_backwards_from_offset(self, start_day_offset, end_day_offset, search_type="backwards"):
        self.search_type = search_type
        self.set_search_end_day(end_day_offset)
        self.set_search_start_day(start_day_offset)
        self.filter_expression = "$lte"
        self.start_hour = "23"
        self.start_min = "59"
        self.start_sec = "59"

    def set_search_start_day(self, start_day):
        try:
            int(start_day)
            self.search_start_day = start_day
        except ValueError:
            return False
        return True

    def set_search_end_day(self, end_day):
        try:
            int(end_day)
            self.search_end_day = end_day
        except ValueError:
            return False
        return True


class RestoreRDS(API):
    def __init__(self):
        super(RestoreRDS, self).__init__("015")
        self.id = "015"
        self.filter_list = []
        # self.filter_expression = {}
        self.filter_expression_string = ""
        self.filter_expression_string_enc = ""
        self.limit_expression_string = ""
        self.start_expression_string = ""
        self.embed_expression_string = ""
        self.filter_flag = False
        self.limit_flag = False
        self.start_flag = False
        # self.sort_flag = False
        self.embed_flag = False
        self.limit = 100
        self.start = 1
        self.url_suffix_list = []
        self.url_suffix_list_enc = []
        self.url_suffix_string = ""
        self.url_suffix_string = ""
        self.url_suffix_string_enc = ""
        self.sort_expression_string = None
        self.type_get = False
        self.type_post = True
        self.good_run_flag = True
        self.environment_id_dict = {}
        self.environment_id = None
        self.kms_key_name_target = ""
        self.kms_key_flag = False
        self.rds_subnet_group_target = None
        self.rds_subnet_group_flag = False
        self.option_group_name_target = ""
        self.option_group_name_flag = False
        self.instance_class_target = ""
        self.instance_class_flag = False
        self.network_sg_list_target = None
        self.network_sg_list_flag = False
        self.rds_name_target = None
        self.rds_name_flag = False
        self.is_public_target = None
        self.is_public_flag = False
        self.environment_id_target = None
        self.environment_id_flag = False
        self.target_flag = False
        self.log_activity_flag = False
        self.backup_id = None
        self.backup_id_flag = False
        self.new_ec2_tags_flag = False
        self.new_ec2_tags = []
        self.restore_task_list_flag = False
        self.restore_task_list = []
        # print(f"RestoreEC2 why api_dict {api_dict.get(self.id, {}).get('type', None)}")
        if api_dict.get(self.id, {}).get('type', None):
            # print(f"api_dict 01")
            if self.good:
                # print(f"api_dict 02")
                if api_dict.get(self.id, {}).get('type', None) == "get":
                    # print(f"api_dict 03")
                    self.set_get()
                elif api_dict.get(self.id, {}).get('type', None) == "post":
                    # print(f"api_dict 04")
                    self.set_post()
                else:
                    # print(f"api_dict 05")
                    self.set_bad()

    def set_restore_task_list(self, task_item):
        self.restore_task_list_flag = True
        self.restore_task_list.append(task_item)
        return True

    def get_restore_task_list(self):
        print(f"list {self.restore_task_list}")
        list_task = self.restore_task_list
        return list_task

    def save_restore_task(self):
        self.restore_task_list_flag = True
        return True

    def set_target_for_rds_restore(self, target, restore_type="simple"):
        if self.debug > 20: print(f"set_target_for_rds_restore: target{target}")
        if restore_type == "simple":
            # print(f" set target {target}")
            if self.set_target_environment_id(target.get("account", None), target.get("region", None)):
                # Required
                if self.debug > 40: print(f"set_target_for_rds_restore: mode {restore_type}")
                if target.get('name', None):
                    if self.debug > 40: print(f"set_target_for_rds_restore: name {target.get('name', None)}")
                    self.set_target_name(target.get('name'))
                else:
                    if self.debug > 40: print(f"set_target_for_rds_restore: no resource_id")
                    self.good = False
                    return False
                if target.get("subnet_group_name", None):
                    if self.debug > 40: print(f"set_target_for_rds_restore: subnet_group_name {target.get("subnet_group_name", None)}")
                    self.set_target_subnet_group_name(target.get("subnet_group_name", None))
                else:
                    if self.debug > 40: print(f"set_target_for_rds_restore: no subnet_group_name")
                    self.set_error_msg(
                        f"set_target_for_rds_restore: could not set target subnet_group_name {target.get("subnet_group_name", None)}")
                    self.good = False
                    return False
                # else:
                #    self.good = False
                #    return False
                if target.get("instance_class", None):
                    self.set_target_instance_class(target.get("instance_class", None))
                # else:
                #    self.good = False
                #    return False
                self.target_flag = True
                # Optional
                sg = target.get("security_group_native_ids", None)
                # print(f"target sg {sg}")
                if sg:
                    # print("in set sg")
                    self.set_target_network_sg_list(sg)
                    # self.network_sg_list_flag = True
                if target.get("kms_key_native_id", None):
                    self.set_target_kms_key_name(target.get("kms_key_native_id"))
                if target.get("option_group_name", None):
                    self.set_target_option_group_name(target.get("option_group_name", None))
                if target.get("is_publicly_accessible", None):
                    self.set_target_is_publicly_accessible(target.get("is_publicly_accessible", None))
                return True
            else:
                self.good = False
                self.set_error_msg("set_target_for_rds_restore: could not set target account or region")
                return False
        elif restore_type == "other":
            # Not implemented Yet
            self.good = False
            return False
        else:
            self.good = False
            return False

    def rds_restore_from_record(self, List):
        run_results = None
        if len(List) > 0:
            # print(f"Restore List {List}")
            for record in List:
                if self.debug > 5: print(f"record to restore: {record}")
                # Check Expire Time
                if self.check_expire_time(record.get('backup_record', {}).get("source_expire_time", None)):
                    run_results = self.run_restore_record(record)
                    if run_results.get('errors', None):
                        return [False, f"restore failed for {record.get("resource_id", None)}: {run_results}"]
                    if self.debug > 5: print(f"restore results for {record.get("resource_id", None)} {run_results}")
                    if self.log_activity_flag:
                        pass
                else:
                    print(f"Backup has expired for {record}")
                    pass
            return [True, ""]
        else:
            self.good = False
            return [False, "no records"]

    def check_expire_time(self, expire_time):
        try:
            expire_date = datetime.fromisoformat(expire_time[:-1]).astimezone(timezone.utc)
        except ValueError:
            if self.debug > 3: print(f"Expire date in invalid format: {expire_time}")
            return False
        if expire_date < datetime.now().astimezone(timezone.utc):
            return False
        else:
            # print(f"backup has not expired {expire_time}")
            return True

    # def parse_ec2_tags(self, record, restore_type="simple"):
    #    if restore_type == "simple":
    #        ec2_tags = record.get("backup_record", {}).get("tags", [{"key": "", "value": ""}])
    #        if self.new_ec2_tags_flag:
    #            ec2_tags = self.check_tag_overlap(ec2_tags, self.new_ec2_tags)
    #        return ec2_tags
    #    elif restore_type == "add_tag":
    #        #Not implemented.
    #
    #        return False
    #    else:
    #        self.good = False
    #        return False

    def parse_rds_restore_target(self, record, restore_type="simple"):
        # simple restore means all default values and should_power_on = True
        if restore_type == "simple":
            should_power_on = True
            #source_name = record.get("backup_record", {}).get("source_resource_id", None)
            target_name = self.rds_name_target
            if self.environment_id_flag:
                environment_id = self.environment_id_target
            else:
                self.good = False
                return False
            rds_tags = record.get("backup_record", {}).get("source_instance_tags", [])
            # if self.new_ec2_tags_flag:
            #    ec2_tags.extend(self.new_ec2_tags)
            if self.rds_subnet_group_flag:
                subnet_group = self.rds_subnet_group_target
            else:
                self.good = False
                return False
            rds_restore_target = {
                "environment_id": environment_id,
                "name": target_name,
                "tags": rds_tags,
                "subnet_group_name": subnet_group
            }
            if self.instance_class_flag:
                rds_restore_target["instance_class"] = self.instance_class_target
            if self.is_public_flag:
                rds_restore_target["is_publicly_accessible"] = self.is_public_target
            if self.kms_key_flag:
                rds_restore_target["kms_key_native_id"] = self.kms_key_name_target
            if self.option_group_name_flag:
                rds_restore_target["option_group_name"] = self.option_group_name_target
            if self.network_sg_list_flag:
                rds_restore_target["security_group_native_ids"] = self.network_sg_list_target

            return rds_restore_target

        elif restore_type == "other":
            # NOT IMPLEMENTED YET
            self.good = False
            return False
        else:
            self.good = False
            return False

    def set_target_kms_key_name(self, value):
        self.kms_key_name_target = value
        self.kms_key_flag = True

    def set_target_network_sg_list(self, value):
        self.network_sg_list_target = value
        self.network_sg_list_flag = True

    def set_target_name(self, value):
        self.rds_name_target = value
        self.rds_name_flag = True

    def set_target_subnet_group_name(self, value):
        self.rds_subnet_group_target = value
        self.rds_subnet_group_flag = True

    def set_target_instance_class(self, value):
        self.instance_class_target = value
        self.instance_class_flag = True

    def set_target_option_group_name(self, value):
        self.option_group_name_target = value
        self.option_group_name_flag = True

    def set_target_is_publicly_accessible(self, value):
        if value == True:
            self.is_public_target = True
        else:
            self.is_public_target = False
        self.is_public_flag = True

    def set_backup_id(self, record):
        if record.get("backup_record", {}).get("source_backup_id", None):
            self.backup_id = record.get("backup_record", {}).get("source_backup_id", None)
            self.backup_id_flag = True
            return self.backup_id
        else:
            self.good = False
        return False

    def set_target_environment_id(self, account, region):
        if self.set_aws_account_id(account) and self.set_aws_region(region):
            env_id_api = EnvironmentId()
            env_id_api.set_url_prefix(self.url_prefix)
            env_id_api.set_token(self.token)
            env_id_api.set_search_account_id(self.aws_account_id)
            env_id_api.set_search_region(self.aws_region)
            if env_id_api.run_api():
                rsp = env_id_api.environment_id_parse_results()
                del env_id_api
                if rsp:
                    self.environment_id_target = rsp
                    self.environment_id_flag = True
                    return True
        del env_id_api
        self.good = False
        return False

    # def add_ec2_tag_to_instance(self, new_tags):
    #    if type(new_tags) == list:
    #        for tag in new_tags:
    #            if tag.get('key') and tag.get('value'):
    #                pass
    #            else:
    #                return False
    #    else:
    #        return False
    #    self.new_ec2_tags = new_tags
    #    self.new_ec2_tags_flag = True
    #    return True

    def set_payload(self, record):
        if self.target_flag:
            backup_id = self.set_backup_id(record)
            rds_restore_target = self.parse_rds_restore_target(record)

            if backup_id and rds_restore_target:
                payload = {
                    "source": {"backup": {"backup_id": backup_id}},
                    "target": rds_restore_target
                }
                if self.debug > 5: print(f"rds restore payload: {payload}")
                self.payload = payload
                self.payload_flag = True
                return True
        else:
            self.good = False
            return False

    def run_restore_record(self, record):
        if self.good:
            if self.set_payload(record):
                result = self.exec_api()
                print(f"rds restore results {result}")
                if self.restore_task_list_flag:
                    task_item = {
                        "task": self.get_task_id(),
                        "name": record.get('resource_id'),
                        "backup_id": record.get("backup_record", {}).get("source_backup_id", None)
                    }
                    self.set_restore_task_list(task_item)
                return result
        else:
            return False

    def set_page_size(self, x):
        if self.query_parms_flag:
            if "limit" in self.query_parms.keys():
                self.limit = x
                self.limit_expression_string = "limit=" + str(self.limit)
                self.limit_flag = True
                return self.build_url_suffix()
        self.set_bad()
        return None

    def build_url_suffix(self):
        self.url_suffix_list = []
        if self.good:
            if self.filter_flag:
                self.url_suffix_list.append(self.filter_expression_string_enc)

            if self.embed_flag:
                self.url_suffix_list.append(self.sort_expression_string)

            if self.limit_flag:
                self.url_suffix_list.append(self.limit_expression_string)

            if self.start_flag:
                self.url_suffix_list.append(self.start_expression_string)

            self.url_suffix_string = "?" + "&".join(self.url_suffix_list)
            String = self.url_suffix_string
            # #print(String)
            self.set_url(String)
            return self.url_suffix_string