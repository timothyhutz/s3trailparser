"""Written by Timothy Hutz <timothyhutz@gmail.com
for use in AWS lambda only on python 3.6 runtime
"""

import boto3
import gzip
import logging
import os

log_level = os.environ['LOG_LEVEL']
if log_level is None or log_level is 'info': # Presets the Logging Level if you didn't define it in ENV variables
	log_config_level = logging.INFO
elif log_level is 'debug':
	log_config_level = logging.DEBUG
else:
	log_config_level = logging.INFO # Logging level can only be set to info or debug..

logging.getLogger().setLevel(log_config_level)

es_endpoint = os.environ['ES_ENDPOINT']
if es_endpoint is None: # Fails app because Elastic Search is a critical component to this
	logging.critical('Missing Elastic Search Endpoint ')
	exit(1)


class DataGz(object): # This class gets the S3 GZ object and returns the body data to memory space variable.
	def __init__(self, region): # Prebuilds the Client connection
		self.region = region
		self.s3 = boto3.client('s3', region_name=self.region)

	def main(self, bucket, key): # This is the S3 Object call
		try:
			action = self.s3.get_object(Bucket=bucket, Key=key)['Body']
			logging.debug(action)
			return action
		except Exception as message:
			logging.critical(message)
			exit(2)


class ESload(object): # This class parses the data and pushes it to the indexer...
	def __init__(self, streamdata, es_endpoint):
		self.streamdata = streamdata
		self.url = es_endpoint
	def __call__(self, *args):
		pass



def main(event, context):
	logging.debug(event)
	for record in event['Records']:
		filedata = DataGz(region=record['awsRegion']).main(key=record['s3']['object']['key'], bucket=record['s3']['bucket']['name'])
		logging.info(record['awsRegion'] )
		logging.info(['bucket= ' + record['s3']['bucket']['name'],'File= ' + record['s3']['object']['key']])
		unzipstream = None
		try:
			unzipstream = gzip.open(filedata, 'rb')
			streamdata= unzipstream.read().decode('utf8')
			ESload(streamdata, es_endpoint)
			unzipstream.close()
		except Exception as message:
			logging.error(message)
			exit(3)
