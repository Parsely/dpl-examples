import base64
import boto3
import json
import logging

from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo_client = boto3.resource('dynamodb')

def _make_dynamo_table():
    table = dynamo_client.create_table(
        TableName='consumers',
        KeySchema=[
            {
                'AttributeName': 'consumer',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'shard',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'consumer',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'shard',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1,
            'WriteCapacityUnits': 1
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName='consumers')
    return table

def _ensure_dynamo_table():
    table = dynamo_client.Table('consumers')
    try:
        table.table_status
    except ClientError:
        _make_dynamo_table()
    return table

def _clear_consumer_lease(table, consumer, shard):
    table.delete_item(Key={'consumer': consumer, 'shard': shard})

def _get_consumer_lease(table, consumer, shard):
    row = table.get_item(Key={'consumer': consumer, 'shard': shard})
    if 'Item' not in row:
        return None
    else:
        return row['Item']['lease']

def _put_consumer_lease(table, consumer, shard, lease):
    table.put_item(
        Item={
            'consumer': consumer,
            'shard': shard,
            'lease': lease}
    )

def process(json_event):
    pass

def handler(event, context):
    """
    'If you create a Lambda function that processes events from
    stream-based services (Amazon Kinesis Streams), the number of
    shards per stream is the unit of concurrency. If your stream
    has 100 active shards, there will be 100 Lambda functions
    running concurrently. Then, each Lambda function processes
    events on a shard in the order that they arrive.'

    Therefore, for checkpointing logic, we should make the primary
    key a combination of:

    - consumer_id (hash)
    - shard_id (range)

    This will let us get a single Dynamo item, describing the
    current "consumer lease", for a consumer_id + shard_id
    combination. It will also mean that for a given consumer_id,
    we can do a range query to get all the shards owned by that
    consumer.
    """
    debug = False
    rewind = False
    dry_run = False

    table = _ensure_dynamo_table()
    consumer_id = 'test-consumer'

    if debug:
        state = table.scan()
        print "Active leases in Dynamo:", state["Count"]
        for item in state["Items"]:
            print json.dumps(item, indent=4, sort_keys=True)

    lease = None
    shard = None

    try:
        visitors = set()
        last_timestamp = None
        for i, record in enumerate(event.get('Records', [])):
            event_id, data = (record['eventID'], record['kinesis']['data'])
            shard, checkpoint = event_id.split(u':')
            if rewind:
                print "Rewinding to checkpoint 0"
                _clear_consumer_lease(table, consumer_id, shard)
                rewind = False
            if lease is None:
                lease = _get_consumer_lease(table, consumer_id, shard) \
                or {"checkpoint": "0"}

            if checkpoint <= lease["checkpoint"]:
                # replayed event, we should skip it
                print "Replayed event; skipping"
                continue
            # => decode from b64
            raw_event = base64.b64decode(data)
            # => parse from JSON
            json_event = json.loads(raw_event)
            # => extract out visitor id and timestamp if present
            visitor = json_event.get("visitor_site_id", "N/A")
            visitors.add(visitor)
            last_timestamp = json_event.get("ts_action", "N/A")
            # => do something with the data
            result = process(json_event)
            if result:
                pass
            # => checkpoint the shard
            lease["checkpoint"] = checkpoint
        logger.info("Saw {} unique visitors in batch ending with {}".format(
            len(visitors), last_timestamp))
        if not dry_run:
            _put_consumer_lease(table, consumer_id, shard, lease)
    except Exception as ex:
        # do not save consumer checkpoints because error happened
        # instead, we should probably log something about the error
        # in the consumer lease, to allow the Lambda to retry a fixed
        # number of times, before finally "giving up" and skipping
        # the records
        raise
        "^ some form of error handling required"
        if ex:
            pass
