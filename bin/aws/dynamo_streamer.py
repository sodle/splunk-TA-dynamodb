import boto3


class DynamoStreamer:
    """
    Follows a DynamoDB table's stream.

    :param session: boto3 session for connecting to DynamoDB
    :type session: boto3.Session
    :param region: AWS Region containing DynamoDB table
    :type region: str
    :param table_name: Name or ARN of DynamoDB table
    :type table_name: str
    :ivar dynamo: AWS DynamoDB client
    :type dynamo: boto3.DynamoDB.Client
    :ivar dynamo_streams: AWS DynamoDB Streams client
    :type dynamo_streams: boto3.DynamoDBStreams.Client
    :ivar table_name: Name of DynamoDB table to read
    :type table_name: str
    :ivar stream_arn: ARN of latest table stream
    :type stream_arn: str
    """

    def __init__(self, session, region, table_name):
        self.dynamo = session.client('dynamodb', region_name=region)
        self.dynamo_streams = session.client('dynamodbstreams', region_name=region)

        self.table_name = table_name
        table_response = self.dynamo.describe_table(TableName=self.table_name)
        table = table_response['Table']
        self.stream_arn = table['LatestStreamArn']

    def list_shards(self):
        """
        Generator that yields the ID of each shard available in the table stream.

        :return: Iterator of shard IDs
        :rtype: Iterator[str]
        """
        last_evaluated_shard = None

        while True:
            if last_evaluated_shard is None:
                stream_response = self.dynamo_streams.describe_stream(StreamArn=self.stream_arn)
            else:
                stream_response = self.dynamo_streams.describe_stream(StreamArn=self.stream_arn,
                                                                      ExclusiveStartShardId=last_evaluated_shard)
            stream = stream_response['StreamDescription']

            shards = stream['Shards']
            for shard in shards:
                print(shard)
                yield shard['ShardId']

            if 'LastEvaluatedShardId' in stream:
                last_evaluated_shard = stream['LastEvaluatedShardId']
            else:
                break

    def read_shard(self, shard_id, last_checkpoint=None):
        """
        Generator that yields each new record from the DynamoDB stream.

        :param shard_id: ID of shard to iterate over
        :type shard_id: str
        :param last_checkpoint: Sequence number after which to start reading records;
        omit for earliest available record (TRIM_HORIZON)
        :type last_checkpoint: str, optional
        :return: Iterator of records from shard
        :rtype: Iterator[object]
        """
        if last_checkpoint is None:
            iterator_response = self.dynamo_streams.get_shard_iterator(StreamArn=self.stream_arn, ShardId=shard_id,
                                                                       ShardIteratorType='TRIM_HORIZON')
        else:
            iterator_response = self.dynamo_streams.get_shard_iterator(StreamArn=self.stream_arn, ShardId=shard_id,
                                                                       ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                                                                       SequenceNumber=last_checkpoint)
        iterator = iterator_response['ShardIterator']

        while iterator is not None:
            records_response = self.dynamo_streams.get_records(ShardIterator=iterator)
            records = records_response['Records']

            for record in records:
                yield record

            iterator = records_response.get('NextShardIterator', None)
