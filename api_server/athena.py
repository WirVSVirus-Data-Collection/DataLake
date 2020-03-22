import time
from typing import Any

import boto3


class AthenaClient:
    def __init__(self, access_key: str, secret_key: str, output_location: str, check_interval=.01):
        self.client = boto3.client('athena',
                                   aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key)
        self.output_location = output_location
        self.check_interval = check_interval

    def query(self, query: str) -> Any:
        query_id = self.client.start_query_execution(QueryString=query,
                                                     ResultConfiguration={'OutputLocation': self.output_location})
        finished = False
        while not finished:
            finished = self.client.get_query_execution(QueryExecutionId=query_id)['Status']['State'] == 'SUCCEEDED'
            time.sleep(self.check_interval)
        return self.client.get_query_results(QueryExecutionId=query_id)
