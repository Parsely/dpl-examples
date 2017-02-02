# Parse.ly Data Pipeline Examples

The [Parse.ly Data Pipeline](http://parse.ly/data-pipeline) provides a stream
of analytics data to customers, as described in the product's [technical
documentation](http://www.parsely.com/help/rawdata/pipeline/).

This repository does not provide an installable library of any kind. Instead,
it is meant to be cloned and borrowed from to implement common integration
patterns with Parse.ly's streaming data.

See the related project
[parsely_raw_data](https://github.com/Parsely/parsely_raw_data) standard
schemas for the DPL data formats, as well as command-line tools for working
with the data.

## Examples - in progress

- `lambda_stream`: An AWS Lambda function that consumes a Parse.ly Kinesis stream and forwards data to another Kinesis Firehose stream for data warehousing to S3, Redshift, or Amazon ES. Provides basic support for checkpointing, retry, and error handling by leveraging a small DynamoDB table. Leverages Lambda's [Event Source](http://docs.aws.amazon.com/lambda/latest/dg/invoking-lambda-function.html#supported-event-source-kinesis-streams), which requires that Parse.ly deliver to a Kinesis Stream inside your own AWS account.

## TODO

- `lambda_batch`: An AWS Lambda function that consumes events in a Parse.ly S3 bucket, regardless of which AWS account owns that S3 bucket (Parse.ly's or your own). Uses a small DynamoDB table in your own account to track which events have already been copied.

- `bigquery_batch`: A command-line tool that will synchronize a Parse.ly S3 bucket (`s3://`) to a Google Cloud Storage (`gs://`), and then invoke a bulk import from a `gs://` bucket to a BigQuery dataset and table. Can be run on a cron on a compute node inside Google's cloud.
