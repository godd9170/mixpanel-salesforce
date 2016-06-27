import hashlib
import time
import urllib #for url encoding
import urllib2 #for sending requests
import base64
import sys
import requests
import logging
import re
from xml.etree import ElementTree as ET
from simple_salesforce import Salesforce

#Squelch the annoying certificate errors
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

class SFDC(object):

    def __init__(self, user_name, password, token):

        self.user_name = user_name
        self.password = password
        self.token = token
        self.sf = None

    def setSession(self, instance, sessionId):
        self.sf = Salesforce(instance=instance + '.salesforce.com', session_id=sessionId)

    def login(self):
        '''Connect to Salesforce API'''
     
        request = u"""<?xml version="1.0" encoding="utf-8" ?>
        <env:Envelope xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:env="http://schemas.xmlsoap.org/soap/envelope/">
            <env:Body>
                <n1:login xmlns:n1="urn:partner.soap.sforce.com">
                    <n1:username>""" + self.user_name + """</n1:username>
                    <n1:password>""" + self.password + self.token + """</n1:password>
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

    def query_contacts(self, emails):
        formatted_emails = "'" + "','".join(filter(None, emails)) + "'"
        query = "SELECT Id, Email, Name FROM CONTACT WHERE Email In (%s)" % formatted_emails
        query_results = self.sf.query(query)
        return query_results['records']

    def query_usage_history_types(self):
        query = "SELECT Id, Name FROM User_Usage_History_Event_Type__c"
        query_results = self.sf.query(query)
        return query_results['records']

    def json_to_xml_rows(self, updates, fields):
        data = {}
        for i in range(0, len(updates), 10000):

            XML = ''

            for record in updates[i:i+10000]:

                XML += u"""<sObject>"""
                for field in fields.keys():
                    XML += """<""" + fields[field] + """>""" + str(record[field]) + """</""" + fields[field] + """>"""

                XML += """</sObject>\n            """

            data[i] = XML
        return data



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