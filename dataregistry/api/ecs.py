import json
import time

import boto3

from dataregistry.api import query, bioidx
from dataregistry.api.db import DataRegistryReadWriteDB
from dataregistry.api.model import BioIndexCreationStatus

CLUSTER = 'TsvConverterCluster'

engine = DataRegistryReadWriteDB().get_engine()


def get_eni_id(task_response):
    attachments = task_response['tasks'][0]['attachments'][0]
    for detail in attachments['details']:
        if detail['name'] == 'networkInterfaceId':
            return detail['value']


def wait_for_task_running(ecs_client, cluster, task_arn):
    while True:
        response = ecs_client.describe_tasks(cluster=cluster, tasks=[task_arn])
        task_status = response['tasks'][0]['lastStatus']
        if task_status == 'RUNNING':
            return response
        time.sleep(10)


def get_public_ip(ec2_client, eni_id):
    response = ec2_client.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    return response['NetworkInterfaces'][0]['Association']['PublicIp']


def run_ecs_sort_and_convert_job(s3_path, sort_columns, schema_info, already_sorted, process_id):
    ecs_client = boto3.client('ecs', region_name='us-east-1')
    # ec2_client = boto3.client('ec2', region_name='us-east-1')

    response = ecs_client.run_task(
        cluster=CLUSTER,
        launchType='FARGATE',
        taskDefinition='MiniBioindex',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': ['subnet-041ed74e61806c6f0'],
                'securityGroups': ['sg-2b58c961'],
                'assignPublicIp': 'ENABLED'
            }
        },
        overrides={
            'containerOverrides': [
                {
                    'name': 'ConverterContainer',
                    'command': [
                        'python3', '-u', 'sort_file.py', '-s', s3_path, '-c', sort_columns,
                        '-a', json.dumps(schema_info), '-o', str(already_sorted), '-p', str(process_id)
                    ],
                }
            ]
        }
    )

    task_arn = response['tasks'][0]['taskArn']
    while True:
        response = ecs_client.describe_tasks(
            cluster=CLUSTER,
            tasks=[task_arn]
        )
        time.sleep(30)
        if response['tasks'][0]['lastStatus'] == 'STOPPED':
            container_exit_code = response['tasks'][0]['containers'][0].get('exitCode', 1)
            if container_exit_code != 0:
                query.update_bioindex_tracking(engine, process_id, BioIndexCreationStatus.FAILED)
            else:
                query.update_bioindex_tracking(engine, process_id, BioIndexCreationStatus.INDEXING)
                try:
                    prefix = 'bioindex/' + str(process_id) + '/'
                    bioidx.create_new_bioindex(engine, process_id, prefix, sort_columns)
                    query.update_bioindex_tracking(engine, process_id, BioIndexCreationStatus.SUCCEEDED)
                except Exception as e:
                    print(f"Error creating bioindex: {e}")
                    query.update_bioindex_tracking(engine, process_id, BioIndexCreationStatus.FAILED)
            return
