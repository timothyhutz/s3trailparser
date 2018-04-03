"""Written by Timothy Hutz <timothyhutz@gmail.com
for use in AWS lambda only on python 3.6 runtime
"""

import boto3
import gzip
import logging
import os
import json
from botocore.vendored import requests



if os.environ['LOG_LEVEL'] is None or os.environ['LOG_LEVEL'] == 'info': # Presets the Logging Level if you didn't define it in ENV variables
	log_config_level = logging.INFO
else:
	log_config_level = logging.DEBUG # Logging level can only be set to info or debug..

logging.getLogger().setLevel(log_config_level)

es_endpoint = os.environ['ES_ENDPOINT']
if es_endpoint is None: # Fails app because Elastic Search is a critical component to this
	logging.critical('Missing Elastic Search Endpoint ')
	exit(1)

useragent_key = os.environ['KEY']
if useragent_key is None:
	logging.critical('Missing key for shitty AWS authentication')
	exit(1)

class DataGz(object): # This class gets the S3 GZ object and returns the body data to memory space variable.
	def __init__(self, region): # Prebuilds the Client connection
		self.region = region
		self.s3 = boto3.client('s3', region_name=self.region)

	def main(self, bucket, key): # This is the S3 Object call
		try:
			filedataloadunzipped = json.loads(gzip.decompress(self.s3.get_object(Bucket=bucket, Key=key)['Body'].read()))
			logging.info('unzip complete')
			return filedataloadunzipped
		except Exception as message:
			logging.critical(message)
			exit(2)


class ESload(object): # This class parses the data and pushes it to the indexer...
	def __init__(self, filedata, es_endpoint):
		logging.debug(es_endpoint)
		self.filedata = filedata['Records']
		logging.debug('filedata loaded')
		self.url = es_endpoint
		logging.debug('ES endpoint set')
		self.recordparse_data = {}
		for record in self.filedata:
			self.recordparse_data['userIdentity'] = record['userIdentity']
			self.recordparse_data['eventName'] = record['eventName']
			self.recordparse_data['awsRegion'] = record['awsRegion']
			self.recordparse_data['eventSource'] = record['eventSource']
			self.recordparse_data['sourceIPAddress'] = record['sourceIPAddress']
			self.recordparse_data['requestParameters'] = record['requestParameters']
			self.recordparse_data['resources'] = record['resources']

	def __call__(self, *args):
		logging.debug(self.recordparse_data)

		post = requests.post(self.url + '/cloudtrailindex/doc?pretty', json=self.recordparse_data, headers={"Content-Type": "application/json", "User-Agent": "{}".format(useragent_key)})
		logging.info(post.status_code)
		logging.info(post.json())
		return None




def main(event, context):
	logging.debug(event)
	for record in event['Records']:
		filedata = DataGz(region=record['awsRegion']).main(key=record['s3']['object']['key'], bucket=record['s3']['bucket']['name'])
		logging.debug(record['awsRegion'] )
		logging.info(['bucket= ' + record['s3']['bucket']['name'],'File= ' + record['s3']['object']['key']])
		load_index_data = ESload(filedata, es_endpoint)
		logging.debug("ESload init'd")
		load_index_data()
