#! /usr/bin/env python
#
# Mixpanel, Inc. -- http://mixpanel.com/
#
# Python API client library to consume mixpanel.com analytics data.

import os
import hashlib
import csv
import urllib
import time
try:
    import json
except ImportError:
    import simplejson as json


class Mixpanel(object):

	ENDPOINT = 'http://data.mixpanel.com/api'
	VERSION = '2.0'

	def __init__(self, api_key, api_secret):
		self.api_key = api_key
		self.api_secret = api_secret

	def csvify(self, file):
		with open('0621/atal_events_0621.csv', 'wb') as csvfile:

			eventwriter = csv.writer(csvfile, delimiter=',')
			eventwriter.writerow(['Event', 'Time', 'Distinct Id'])
			for line in file:
				try:
					event = json.loads(line)
					eventwriter.writerow([event["event"], event["properties"]["time"], event["properties"]["distinct_id"]])
				except:
					print("----------- BAD LINE -----------")
		
	def request(self, methods, params):
		"""
			methods - List of methods to be joined, e.g. ['events', 'properties', 'values']
					  will give us http://mixpanel.com/api/2.0/events/properties/values/
			params - Extra parameters associated with method
		"""
		params['api_key'] = self.api_key
		params['expire'] = int(time.time()) + 600   # Grant this request 10 minutes.

		if 'sig' in params: del params['sig']
		params['sig'] = self.hash_args(params)

		request_url = '/'.join([self.ENDPOINT, str(self.VERSION)] + methods) + '/?' + self.unicode_urlencode(params)
		#request = urllib.urlretrieve(request_url, output_file)
		response = urllib.urlopen(request_url)
		self.csvify(response)

	def unicode_urlencode(self, params):
		"""
			Convert lists to JSON encoded strings, and correctly handle any 
			unicode URL parameters.
		"""
		if isinstance(params, dict):
			params = params.items()
		for i, param in enumerate(params):
			if isinstance(param[1], list): 
				params[i] = (param[0], json.dumps(param[1]),)

		return urllib.urlencode(
			[(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params]
		)

	def hash_args(self, args, secret=None):
		"""
			Hashes arguments by joining key=value pairs, appending a secret, and 
			then taking the MD5 hex digest.
		"""
		for a in args:
			if isinstance(args[a], list): args[a] = json.dumps(args[a])

		args_joined = ''
		for a in sorted(args.keys()):
			if isinstance(a, unicode):
				args_joined += a.encode('utf-8')
			else:
				args_joined += str(a)

			args_joined += '='

			if isinstance(args[a], unicode):
				args_joined += args[a].encode('utf-8')
			else:
				args_joined += str(args[a])

		hash = hashlib.md5(args_joined)

		if secret:
			hash.update(secret)
		elif self.api_secret:
			hash.update(self.api_secret)
		return hash.hexdigest() 

#api_key = raw_input("API Key: ")
#api_secret = raw_input("API Secret: ")
#from_date = raw_input("From Date: ")
#to_date = raw_input("To Date: ")
#output_file = raw_input("Output Filename: ")

api_key = os.environ['MX_API_KEY']
api_secret = os.environ['MX_API_SECRET']
from_date = '2016-06-21'
to_date = '2016-06-21'
output_file = 'atal_events_0621.csv'

api = Mixpanel(
	api_key = api_key, 
	api_secret = api_secret
)

data = api.request(['export'], {
	'event': ["AOI Draw Clicked","AOI Search Clicked","AOI Rectangle Clicked","Cancel AOI Clicked","Layer Added","App Loaded","Query"],
	'from_date': from_date,
	'to_date': to_date,
	'where': 'boolean(properties["$email"]) != "false"'
	})	