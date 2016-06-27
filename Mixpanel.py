#! /usr/bin/env python
#
# Mixpanel, Inc. -- http://mixpanel.com/
#
# Python API client library to consume mixpanel.com analytics data.

import hashlib
import csv
import urllib
import time
try:
    import json
except ImportError:
    import simplejson as json



class Mixpanel(object):

	EVENTS_ENDPOINT = 'http://data.mixpanel.com/api'
	PEOPLE_ENDPOINT = 'http://mixpanel.com/api'
	VERSION = '2.0'

	def __init__(self, api_key, api_secret):
		self.api_key = api_key
		self.api_secret = api_secret


		
	def request_events(self, methods, params):
		"""
			methods - List of methods to be joined, e.g. ['events', 'properties', 'values']
					  will give us http://mixpanel.com/api/2.0/events/properties/values/
			params - Extra parameters associated with method
		"""
		params['api_key'] = self.api_key
		params['expire'] = int(time.time()) + 600   # Grant this request 10 minutes.

		if 'sig' in params: del params['sig']
		params['sig'] = self.hash_args(params)

		request_url = '/'.join([self.EVENTS_ENDPOINT, str(self.VERSION)] + methods) + '/?' + self.unicode_urlencode(params)
		#request = urllib.urlretrieve(request_url, output_file)
		response = urllib.urlopen(request_url)

		events = []
		total = 0
		for line in response:
			try:
				event_name = json.loads(line)['event']
				distinct_id = json.loads(line)['properties']['distinct_id']
				event_time = json.loads(line)['properties']['time']
				events.append({"event_name" : event_name, "distinct_id" : distinct_id, "time": event_time})
				total += 1
			except:
				print("----------- BAD LINE -----------")
		print "%d (Total Events Downloaded)" % (total)
		return events

	# Given a selector clause, query all the people in Mixpanel and return a single dictionary of
	# {distict_id : email} pairs
	def request_people(self, methods, params):
		response = self.request_people_looper(['engage'], params)
		params['session_id'] = response['session_id']
		params['page']=0

		has_results = True
		total = 0
		total_with_emails = 0
		people = {}

		while has_results:
			responser = response['results']
			for profile in responser:
				distinct_id = profile.get("$distinct_id")
				email = profile['$properties'].get("$email", "")
				if(len(email) > 0):
					total_with_emails += 1
					people[distinct_id] = email

			total += len(responser)
			has_results = len(responser) == 1000
			
			params['page'] += 1 #increment the page
			if has_results:
				response = self.request_people_looper(['engage'],params)

		print "%d / %d (Total With Emails / Total People Downloaded)" % (total_with_emails, total)
		return people


	#Unlike Events, the People Table Paginates 
	def request_people_looper(self, methods, params):
		params['api_key'] = self.api_key
		params['expire'] = int(time.time()) + 600   # Grant this request 10 minutes.

		if 'sig' in params: del params['sig']
		params['sig'] = self.hash_args(params)

		request_url = '/'.join([self.PEOPLE_ENDPOINT, str(self.VERSION)] + methods) + '/?' + self.unicode_urlencode(params)
		response = urllib.urlopen(request_url).read()

		return (json.loads(response))

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
