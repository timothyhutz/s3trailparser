"""Written by Timothy Hutz <timothyhutz@gmail.com
for use in AWS lambda only on python 3.6 runtime
"""

import boto3
import gzip
import logging

LOG_LEVEL = None
if LOG_LEVEL is None:
	log_config_level = logging.INFO
else:
	log_config_level = LOG_LEVEL

logging.getLogger().setLevel(log_config_level)

ELASTISEARCHURL = None
if ELASTISEARCHURL is None:
	logging.error('Missing Elastic Search Endpoint ')
	exit(1)


class DataGz(object):
	def __init__(self, region):
		self.region = region
		self.s3 = boto3.client('s3', region_name=self.region)

	def main(self, bucket, key):
		try:
			action = self.s3.get_object(Bucket=bucket, Key=key)['Body']
			logging.debug(action)
			return action
		except Exception as message:
			logging.error(message)
			exit(2)

class ESload(object):
	def __init__(self):

		self.url = ELASTISEARCHURL

	def __call__(self, *args, **kwargs):
		pass



def main(event, context):
	logging.debug(event)
	for record in event['Records']:
		filedata = DataGz(region=record['awsRegion']).main(key=record['s3']['object']['key'], bucket=record['s3']['bucket']['name'])
		logging.info(record['awsRegion'] )
		logging.info(['bucket=' + record['s3']['bucket']['name'],'File=' + record['s3']['object']['key']])
		unzipstream = None
		try:
			unzipstream = gzip.open(filedata, 'rb')
		except Exception as message:
			logging.error(message)
			exit(3)
		try:
			streamdata = unzipstream.read().decode('utf8')
		except Exception as message:
			logging.error(message)
			exit(4)
		unzipstream.close()
