#! /usr/bin/env python
#
# Mixpanel, Inc. -- http://mixpanel.com/
#
# Python API client library to consume mixpanel.com analytics data.

import os
import csv
import hashlib
import time
import urllib #for url encoding
import urllib2 #for sending requests
import base64
try:
	import json
except ImportError:
	import simplejson as json

class Mixpanel(object):

    def __init__(self, api_key, api_secret, token):
        self.api_key = api_key
        self.api_secret = api_secret
        self.token = token

    def request(self, params, format = 'json'):
        '''let's craft the http request'''
        params['api_key']=self.api_key
        params['expire'] = int(time.time())+600 # 600 is ten minutes from now
        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)

        request_url = 'http://mixpanel.com/api/2.0/engage/?' + self.unicode_urlencode(params)
        print "Request URL: " + request_url

        request = urllib.urlopen(request_url)
        data = request.read()

        #print request_url

        return data

    def hash_args(self, args, secret=None):
        '''Hash dem arguments in the proper way
        join keys - values and append a secret -> md5 it'''

        for a in args:
            if isinstance(args[a], list): args[a] = json.dumps(args[a])

        args_joined = ''
        for a in sorted(args.keys()):
            if isinstance(a, unicode):
                args_joined += a.encode('utf-8')
            else:
                args_joined += str(a)

            args_joined += "="

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

    def unicode_urlencode(self, params):
        ''' Convert stuff to json format and correctly handle unicode url parameters'''

        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        result = urllib.urlencode([(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params])
        return result

if __name__ == '__main__':
    api = Mixpanel(
        api_key = os.environ['MX_API_KEY'],
        api_secret = os.environ['MX_API_SECRET'],
        token = os.environ['MX_TOKEN']
    )
    '''Here is the place to define your selector to target only the users that you're after'''
    '''parameters = {'selector':'(properties["$email"] == "Albany") or (properties["$city"] == "Alexandria")'}'''
    parameters = {'selector': 'boolean(properties["$email"]) != "false"'}
    response = api.request(parameters)
    
    parameters['session_id'] = json.loads(response)['session_id']
    parameters['page']=0
    global_total = json.loads(response)['total']
    
    print "Session id is %s \n" % parameters['session_id']
    print "Here are the # of people %d" % global_total
    fname = "output_people.txt"
    has_results = True
    total = 0
    with open('06210622/people_atla_06210622.csv', 'wb') as csvfile:
        eventwriter = csv.writer(csvfile, delimiter=',')
        eventwriter.writerow(['Distinct Id', 'Email'])
        while has_results:
            responser = json.loads(response)['results']
            total += len(responser)
            has_results = len(responser) == 1000
            for data in responser:
                email = data['$properties'].get("$email", "")
                if len(email) != 0:
                    eventwriter.writerow([data['$distinct_id'], email])
            print "%d / %d" % (total,global_total)
            parameters['page'] += 1
            if has_results:
                response = api.request(parameters)