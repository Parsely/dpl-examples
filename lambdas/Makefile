SAMPLE_DATA := "import base64; import json; print(json.dumps({\"Records\": [{\"eventID\": \"shard-01:1001\", \"kinesis\": {\"data\": base64.b64encode('{\"visitor_site_id\": \"ABC\"}')}}]}))"

deploy:
	gordon build && gordon apply dev

test:
	python -c ${SAMPLE_DATA} | gordon run app.lambda_stream
	python -c ${SAMPLE_DATA}
