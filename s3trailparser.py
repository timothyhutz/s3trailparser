"""Written by Timothy Hutz <timothyhutz@gmail.com
for use in AWS lambda only on python 3.6 runtime
"""

import boto3
import gzip


class DataGz(object):
	def __init__(self, region):
		self.region = region
		self.s3 = boto3.client('s3', region_name=self.region)

	def main(self, bucket, key):
		return self.s3.get_object(Bucket=bucket, Key=key)['Body']


def main(event, context):
	for record in event['Records']:
		filedata = DataGz(region=record['awsRegion']).main(key=record['s3']['object']['key'], bucket=record['s3']['bucket']['name'])
		unzipstream = gzip.open(filedata, 'rb')
		streamdata = unzipstream.read().decode('utf8')
		unzipstream.close()
		print(streamdata)
