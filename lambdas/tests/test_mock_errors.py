import unittest

from mock import MagicMock

import json

from copy import deepcopy

class InMemoryDynamoTable(object):
    def __init__(self):
        self.meta = MagicMock()
        self.key_ids = "consumer", "shard"

    def create_table(self):
        self.state = {}

    def get_item(self, Key=None):
        key_str = json.dumps(Key, sort_keys=True)
        value = self.state.get(key_str)
        if value is not None:
            value.update(Key)
            return {"Item": value}
        else:
            return {}

    def put_item(self, Item=None):
        item = deepcopy(Item)
        key_dict = {}
        for key_id in self.key_ids:
            key_dict[key_id] = item.pop(key_id)
        key_str = json.dumps(key_dict, sort_keys=True)
        self.state[key_str] = item
        return None

    def delete_item(self, Key=None):
        key_str = json.dumps(Key, sort_keys=True)
        del self.state[key_str]
        return None

    def scan(self):
        items = []
        for key_str, item_value in self.state.iteritems():
            item_dict = json.loads(key_str)
            item_dict.update(item_value)
            items.append(item_dict)
        if len(items) > 0:
            return {"Items": items}
        else:
            return {}

class NormalRun(Exception): pass
class DynamoDown(Exception): pass
class SkipNecessary(Exception): pass
class CrashOnProcess(Exception): pass
class CrashOnRecord(Exception): pass


def handler(event, context, state=NormalRun, table=None):
    # err #1: can't even connect to DynamoDB
    consumer = {"consumer": "lambda-test", "shard": "shard-001"}
    if state is DynamoDown:
        raise DynamoDown()
    row = table.get_item(Key=consumer)
    lease = row.get("Item", None)
    if lease is None:
        default_lease = {"lease": {"checkpoint": 0}}
        default_lease.update(consumer)
        lease = default_lease

    new_checkpoint = None

    num_done = 0

    try:
        for i, record in enumerate(event.get('Records', [])):
            event_id, data = (record['eventID'], record['kinesis']['data'])
            if state is CrashOnRecord:
                raise CrashOnRecord()
            shard, checkpoint = event_id.split(u':')

            # err #2: checkpoint is "ahead" of this message; need to skip
            # we should use standard logging at INFO level to say we're skipping
            if state is SkipNecessary and i == 0:
                if not (checkpoint <= lease["lease"]["checkpoint"]):
                    raise SkipNecessary()

            if checkpoint <= lease["lease"]["checkpoint"]:
                print("skipping")
                continue

            if state is NormalRun:
                process = lambda x: x
                process(record)
                num_done += 1

            if state is CrashOnProcess:
                raise CrashOnProcess()

            new_checkpoint = checkpoint
    except:
        # err #3: something went wrong when processing the batch
        # log the exception using standard logging (and thus, CloudWatch Logs)
        # we'll checkpoint in finally block, but we should capture all details here
        raise
    finally:
        # whether we had an error or not, we should update how far along we got
        if new_checkpoint is not None:
            lease.update({"lease": {"checkpoint": new_checkpoint}})
            table.put_item(Item=lease)
        else:
            # err #4: not only did something get wrong with the batch, but
            # we couldn't even get a checkpoint value; we didn't get far
            # enough to even process a record
            pass
    return num_done


class TestInMemoryDynamoTable(unittest.TestCase):
    def test_basic_table(self):
        # all state in table.state
        table = InMemoryDynamoTable()
        table.create_table()
        key = {"consumer": "dpl", "shard": "shard-001"}
        item = dict(key)
        item.update({"lease": {"checkpoint": 0}})
        table.put_item(Item=item)
        row = table.get_item(Key=key)
        self.assertIn("Item", row)
        self.assertEqual(row["Item"]["lease"], {"checkpoint": 0})
        table.delete_item(Key=key)
        row = table.get_item(Key=key)
        self.assertNotIn("Item", row)
        # table.meta uses an uneventful MagicMock
        table.meta.client.get_waiter('table_exists').wait(TableName='consumers')

    def test_mock(self):
        table = MagicMock()


class TestExceptionHandling(unittest.TestCase):
    def setUp(self):
        event = {"Records": [
            {"eventID": "shard-001:%s" % i, "kinesis": {"data": "xxxxxxxxxxxxx"}}
            for i in range(1, 51)
        ]}
        self.event = event

    def test_basic_lambda_calls(self):
        event = self.event
        context = {}

        table = InMemoryDynamoTable()
        table.create_table()

        total = 0

        num = handler(event, context, table=table, state=NormalRun)
        total += num
        self.assertEqual(len(table.state), 1)

        num = handler(event, context, table=table, state=SkipNecessary)
        total += num
        self.assertEqual(len(table.state), 1)

        with self.assertRaises(DynamoDown):
            num = handler(event, context, table=table, state=DynamoDown)
            total += num

        with self.assertRaises(CrashOnRecord):
            num = handler(event, context, table=table, state=CrashOnRecord)
            total += num

        # this won't raise because all records are skipped
        num = handler(event, context, table=table, state=CrashOnProcess)
        total += num

        self.assertEqual(total, 50)

    def test_partial_lambda_calls(self):
        first_half = {"Records": self.event["Records"][0:25]}
        second_half = {"Records": self.event["Records"][25:]}
        context = {}

        table = InMemoryDynamoTable()
        table.create_table()

        total = 0

        num = handler(first_half, context, table=table, state=NormalRun)
        total += num
        self.assertEqual(num, 25)

        with self.assertRaises(CrashOnProcess):
            num = handler(second_half, context, table=table, state=CrashOnProcess)
            total += num

        num = handler(second_half, context, table=table, state=NormalRun)
        total += num
        self.assertEqual(num, 25)
        self.assertEqual(total, 50)


if __name__ == "__main__":
    unittest.main()
