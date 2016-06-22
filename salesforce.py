'''IMPORTANT: Update information in lines 245-275'''

'''Limited to 50,000,000 SFDC Object Updates (per 24 hours)'''

import hashlib
import time
import urllib #for url encoding
import urllib2 #for sending requests
import base64
import sys
import csv
import requests
import logging
import re
from xml.etree import ElementTree as ET
from simple_salesforce import Salesforce

try:
    import json
except ImportError:
    import simplejson as json

# capture "InsecureRequest" warnings from API requests
logging.captureWarnings(True)

### Mixpanel

class Mixpanel(object):

    def __init__(self, api_key, api_secret):

        self.api_key = api_key
        self.api_secret = api_secret

    def request(self, params, format = 'json'):
        '''Craft the http request to Mixpanel'''

        params['api_key']=self.api_key
        params['expire'] = int(time.time())+600 # 600 is ten minutes from now
        if 'sig' in params: del params['sig']
        params['sig'] = self.hash_args(params)

        request_url = 'http://mixpanel.com/api/2.0/engage/?' + self.unicode_urlencode(params)

        request = urllib.urlopen(request_url)
        data = request.read()

        return data

    def hash_args(self, args, secret=None):
        '''Hash arguments: join keys - values and append a secret -> md5 it'''

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
        '''Convert to json format and correctly handle unicode url parameters'''

        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        result = urllib.urlencode([(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params])
        return result


### Salesforce

class SFDC(object):

    def __init__(self, user_name, password):

        self.user_name = user_name
        self.password = password
        self.sf = None

    def login(self):
        '''Connect to Salesforce API'''
     
        request = u"""<?xml version="1.0" encoding="utf-8" ?>
        <env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
            <env:Body>
                <n1:login xmlns:n1="urn:partner.soap.sforce.com">
                    <n1:username>""" + self.user_name + """</n1:username>
                    <n1:password>""" + self.password + """</n1:password>
                </n1:login>
            </env:Body>
        </env:Envelope>"""
       
        encoded_request = request.encode('utf-8')
        url = "https://login.salesforce.com/services/Soap/u/30.0"
       
        headers = {"Content-Type": "text/xml; charset=UTF-8",
                   "SOAPAction": "login"}
                                 
        response = requests.post(url=url,
                                 headers = headers,
                                 data = encoded_request,
                                 verify=False)

        return unicode(response.text)

    def query_objects(self, updates, objects, fields, externalId = None):
        '''Look for and format Salesforce objects to be updated with Mixpanel data'''

        emails = [email if email is not None else '' for email in updates.keys()]
        formatted_emails = "'" + "','".join(filter(None, emails)) + "'"

        query = "SELECT Id, Email, " + externalId + " FROM " + objects + " WHERE Email In (%s)" % formatted_emails
        query_results = self.sf.query(query)

        sfdc_total = query_results.items()[0][1]
        print '# ' + objects + 's matching Mixpanel People: %d' % sfdc_total 

        records = query_results.items()[2][1]

        data = {}

        for i in range(0, sfdc_total, 10000):

            XML = ''

            for record in records[i:i+10000]:

                length = len(record.items())
                mp_update_id = record.items()[length-1][1]
                email = record.items()[length-2][1]
                Id = record.items()[length-3][1]

                XML += u"""<sObject><Id>""" + Id + """</Id>"""
                for field in fields.keys():
                    XML += """<""" + fields[field] + """>""" + updates[email][field] + """</""" + fields[field] + """>"""

                XML += """<""" + externalId + """>""" + mp_update_id + """</""" + externalId + """>"""

                XML += """</sObject>\n            """

            data[i] = XML

        return data

    def create_job(self, instance, sessionId, operation, object, contentType, externalId = None):
        '''Create a job in Salesforce to prepare for data load'''

        if externalId is not None: 
            request = u"""<?xml version="1.0" encoding="UTF-8"?>
            <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                <operation>""" + operation + """</operation>
                <object>"""+ object + """</object>
                <externalIdFieldName>""" + externalId + """</externalIdFieldName>
                <contentType>""" + contentType + """</contentType>
            </jobInfo>"""
        else:
            request = u"""<?xml version="1.0" encoding="UTF-8"?>
            <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
                <operation>""" + operation + """</operation>
                <object>"""+ object + """</object>
                <contentType>""" + contentType + """</contentType>
            </jobInfo>"""

        encoded_request = request.encode('utf-8')
        url = "https://" + instance + ".salesforce.com/services/async/30.0/job"

        headers = {"X-SFDC-Session": sessionId,
                   "Content-Type": "application/xml; charset=UTF-8"}
                                 
        response = requests.post(url=url,
                                 headers = headers,
                                 data = encoded_request,
                                 verify=False)

        return unicode(response.text)

    def add_batch(self, instance, sessionId, jobId, objects):
        '''Add one "batch" of data to Salesforce job'''

        request = u"""<?xml version="1.0" encoding="UTF-8"?>
        <sObjects xmlns="http://www.force.com/2009/06/asyncapi/dataload">
            """ + objects + """
        </sObjects>"""
     
        encoded_request = request.encode('utf-8')
        url = "https://" + instance + "-api.salesforce.com/services/async/30.0/job/" + jobId + "/batch"
       
        headers = {"X-SFDC-Session": sessionId,
                   "Content-Type": "application/xml; charset=UTF-8"}
                                 
        response = requests.post(url=url,
                                 headers = headers,
                                 data = encoded_request,
                                 verify=False)
     
        return unicode(response.text)

    def close_job(self, instance, sessionId, jobId):
        '''Close Salesforce data load job'''
     
        request = u"""<?xml version="1.0" encoding="UTF-8"?>
        <jobInfo xmlns="http://www.force.com/2009/06/asyncapi/dataload">
            <state>Closed</state>
        </jobInfo>"""
     
        encoded_request = request.encode('utf-8')
        url = "https://" + instance + ".salesforce.com/services/async/30.0/job/" + jobId
       
        headers = {"X-SFDC-Session": sessionId,
                   "Content-Type": "application/xml; charset=UTF-8"}
                                 
        response = requests.post(url=url,
                                 headers = headers,
                                 data = encoded_request,
                                 verify=False)
     
        return unicode(response.text)


if __name__ == '__main__':

    ####################################################################
    # Update the following fields 
    ####################################################################

    # Mixpanel project information (can be found in the "Account" section of Mixpanel)
    mp_api = Mixpanel(
        api_key = '',
        api_secret = ''
        )

    # Salesforce username and password
    sf_api = SFDC(
        user_name = 'example@example.com',
        password = 'password'
        )

    # Object in Salesforce such as Lead, Opportunity, etc.
    object_to_update = 'Lead'

    # External ID set up on that Salesforce object (such as MP_Update_ID__c)
    object_external_id = 'MP_Update_ID__c'

    # Dictionary mapping fields you wish to pull from Mixpanel People profiles 
    # to fields you want to update in the Salesforce object (must be valid People 
    # property and Salesforce field names - CaSe SeNsItIvE)
    fields = {'Times Chosen Platform': 'Times_Chosen_Platform__c', 'Platform Chosen': 'Platform_Chosen__c'}

    # Here is the place to define your selector to target only the users
    # you want to grab from Mixpanel
    # example: 
    # parameters = {'selector':'("mixpanel.com" in string(properties["$email"])) or (properties["$city"] == "San Francisco")'}
    parameters = {'selector':''}

    ####################################################################
    # Do not edit below
    ####################################################################

    print "Querying Mixpanel People"

    response = mp_api.request(parameters)

    parameters['session_id'] = json.loads(response)['session_id']
    parameters['page']=0
    global_total = json.loads(response)['total']
    
    print "# People: %d" % global_total

    has_results = True
    total = 0
    updates = {}

    while has_results:
        responser = json.loads(response)['results']
        for profile in responser:
            distinct_id = profile.get("$distinct_id")
            email = profile["$properties"].get("$email")
            updates[email] = {}
            for field in fields.keys():
                updates[email].update({field: profile["$properties"].get(field)})
        total += len(responser)
        has_results = len(responser) == 1000

        print "%d / %d" % (total,global_total)
        parameters['page'] += 1
        if has_results:
            response = mp_api.request(parameters)
        
    print "Logging into Salesforce"

    login_response = sf_api.login()
    login_response = ET.fromstring(login_response)

    result_pre_text = '{http://schemas.xmlsoap.org/soap/envelope/}Body/{urn:partner.soap.sforce.com}loginResponse/{urn:partner.soap.sforce.com}result/{urn:partner.soap.sforce.com}'

    try:
        sessionId = login_response[0][0][0][4].text
    except IndexError:
        sys.exit("Error: Salesforce login information may be incorrect")
    instance = login_response.find(result_pre_text + 'metadataServerUrl').text
    instance = re.search('://(.*).salesforce.com', instance)
    instance = instance.group(1)

    session = sessionId

    sf_api.sf = Salesforce(instance=instance + '.salesforce.com', session_id=sessionId)

    print "Creating job"

    job_response = sf_api.create_job(instance, sessionId, 'upsert', object_to_update, 'XML', externalId = object_external_id)
    job_response = ET.fromstring(job_response)

    jobId = job_response[0].text

    print "Querying Salesforce " + object_to_update + "s"

    xml_object = sf_api.query_objects(updates, object_to_update, fields, externalId = object_external_id)

    print "Adding batches (10,000 %ss at a time)" % (object_to_update)

    batch = 0

    for xml_batch in xml_object.items():
        batch += 1
        print "Batch %d" % (batch)
        batch = sf_api.add_batch(instance, sessionId, jobId, xml_batch[1])
        # print batch

    print "Closing job"
    
    close = sf_api.close_job(instance, sessionId, jobId)
    # print close
