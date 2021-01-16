#!/usr/bin/env python

import argparse
from datetime import datetime, timedelta
import sys
import time

import boto3
from snowplow_analytics_sdk.run_manifests import *

# -----------------------------------------------------------------------------
#  CONSTANTS
# -----------------------------------------------------------------------------

TIME_FORMAT = '%Y-%m-%d-%H-%M-%S'
SCRIPT_VERSION = 'reload-script-0.1.0'


# -----------------------------------------------------------------------------
#  COMMANDS
# -----------------------------------------------------------------------------

def reload(dynamodb, args):
    """Resets manifests between specified start and end dates to a PROCESSED state"""
    paginator = dynamodb.get_paginator('scan')
    operation_parameters = {
      'TableName': args.manifest_table_name
    }
    page_iterator = paginator.paginate(**operation_parameters)

    reloaded = 0
    skipped = 0

    for page in page_iterator:
        for item in page['Items']:
            if item['ToSkip']['BOOL'] is False and should_reload(item['RunId']['S'], args.startdate, args.enddate):
                reloaded = reloaded + 1
                reload_item(dynamodb, args.manifest_table_name, item)
            else:
                skipped = skipped + 1

    print("Reloaded {} run ids to PROCESSED state".format(reloaded))
    print("Skipped {} run ids".format(skipped))


def should_reload(run_id, startdate, enddate):
    """Predicate checking that a run ID is after a specified start date"""
    RUN_ID_TIME_LENGTH = 'YYYY-mm-dd-HH-MM-SS/'   # 20
    startdate = datetime.strptime(startdate, TIME_FORMAT)
    enddate = datetime.strptime(enddate, TIME_FORMAT)
    date = run_id[-len(RUN_ID_TIME_LENGTH):]
    d = datetime.strptime(date, '%Y-%m-%d-%H-%M-%S/')
    if d > startdate and d < enddate:
        return True
    else:
        return False


def reload_item(dynamodb, table_name, item):
    """Restores item in manifest to PROCESSED state"""
    dynamodb.put_item(
        TableName=table_name,
        Item={
            'RunId': item['RunId'],
            'AddedBy': {
                'S': SCRIPT_VERSION
            },
            'AddedAt': {
                'N': str(int(time.time()))
            },
            'ToSkip': item['ToSkip'],
            'ProcessedAt': item['ProcessedAt'],
            'SavedTo': item['SavedTo'],
            'ShredTypes': item['ShredTypes']
        }
    )


if __name__ == "__main__":
    # Initialize

    parser = argparse.ArgumentParser(description='Reload events from a snowflake-loader run manifest')

    parser.add_argument('--aws-access-key-id', type=str, required=True,
                        help="AWS access key id DynamoDB and S3")
    parser.add_argument('--aws-secret-access-key', type=str, required=True,
                        help="AWS secret access key id DynamoDB and S3")
    parser.add_argument('--region', type=str, required=True,
                        help="AWS Region to access DynamoDB and S3")
    parser.add_argument('--manifest-table-name', required=True,
                        help="DynamoDB Process manifest table name")
    parser.add_argument('--startdate', type=str, required=True,
                        help="Date marking the start of runs to be reloaded")
    parser.add_argument('--enddate', type=str, required=True,
                        help="Date marking the end of runs to be reloaded")

    args = parser.parse_args()

    session = boto3.Session(aws_access_key_id=args.aws_access_key_id, aws_secret_access_key=args.aws_secret_access_key)
    dynamodb = session.client('dynamodb', region_name=args.region)

    try:
        startdate = datetime.strptime(args.startdate, TIME_FORMAT)
    except ValueError:
        print("--startdate must match {} format".format(TIME_FORMAT))
        sys.exit(1)

    try:
        enddate = datetime.strptime(args.enddate, TIME_FORMAT)
    except ValueError:
        print("--enddate must match {} format".format(TIME_FORMAT))
        sys.exit(1)

    if startdate > enddate:
        print("start date must be less than or equal to the end date")
        sys.exit(1)        

    reload(dynamodb, args)
