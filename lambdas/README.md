# Parse.ly DPL lambda function example

What it does:

- gordon for lambda management
- kinesis event source and trigger to read records from kinesis stream within AWS account
- dynamodb table to store checkpoint information

Known Issues:

- gordon's CloudFormation seems to fail with a Kinesis Event Source, so, for now, this is disabled in the code and you need to manually add the event source as a "trigger" in the AWS web UI
- dynamodb was not accessible to my lambda, so I needed to add a policy so that the specific table had permissions
