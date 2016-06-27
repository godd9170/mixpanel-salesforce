import os
from Mixpanel import Mixpanel
from xml.etree import ElementTree as ET
from Salesforce import SFDC
from CSVify import CSVify
import hashlib
import re
try:
    import json
except ImportError:
    import simplejson as json


def generate_event_id(time, distinct_id, event_name):
	"""
		Hashes unix stamp and distinct_id to create an external event id
	"""
	join = str(time) + str(distinct_id)
	hash = hashlib.md5(join)
	return hash.hexdigest()

###############
#  Constants  #
###############
SFDC_USER = os.environ['SFDC_USER']
SFDC_PASSWORD = os.environ['SFDC_PASSWORD']
SFDC_TOKEN = os.environ['SFDC_TOKEN']
MXPNL_API_KEY = os.environ['MX_API_KEY']
MXPNL_API_SECRET = os.environ['MX_API_SECRET']

from_date = '2016-06-22'
to_date = '2016-06-22'

##Algorithm
mixpanel_api = Mixpanel( api_key = MXPNL_API_KEY, api_secret = MXPNL_API_SECRET )

#Get all the People (total) from Mixpanel (In a {distinct_id : email } object)
ppldata = mixpanel_api.request_people(['engage'], {'page': 0, 'selector': 'boolean(properties["$email"]) != "false"'})	#'selector': 'boolean(properties["$email"]) != "false"'

#Get all the Events from Yesterday from Mixpanel (as an array of events)
eventdata = mixpanel_api.request_events(['export'], {
	'event': ["AOI Draw Clicked","AOI Search Clicked","AOI Rectangle Clicked","Cancel AOI Clicked","Layer Added","App Loaded","Query"], #TODO: Make Generic
	'from_date': from_date,
	'to_date': to_date
	})	

#Merge People -> Events = PeopleEvents & get unique emails
people_events = []
unmatched_events = []
unique_emails = []

for i in range(0,len(eventdata)):
	distinct_id = eventdata[i]["distinct_id"]
	if distinct_id in ppldata.keys(): #does that id show up in our people w emails list?
		email = ppldata[distinct_id]
		eventdata[i]["Email"] = email
		eventdata[i]["Product__c"] = "Atla" #TODO: Make Generic
		people_events.append(eventdata[i])
		if email not in unique_emails:
			unique_emails.append(email)
	else:
		unmatched_events.append(eventdata[i])

print "%d / %d (Total Matched with Emails / Total Events)" % (len(people_events), len(eventdata))

#Log into Salesforce
sf_api = SFDC(
    user_name = os.environ['SFDC_USER'],
    password = os.environ['SFDC_PASSWORD'],
    token = os.environ['SFDC_TOKEN']
    )

print "Logging into Salesforce"

login_response = sf_api.login()
login_response = ET.fromstring(login_response)

result_pre_text = '{http://schemas.xmlsoap.org/soap/envelope/}Body/{urn:partner.soap.sforce.com}loginResponse/{urn:partner.soap.sforce.com}result/{urn:partner.soap.sforce.com}'

try:
    sessionId = login_response[0][0][0][4].text
    print ">>>>>>>>>>>>Session ID: " + sessionId
except IndexError:
    sys.exit("Error: Salesforce login information may be incorrect")

instance = login_response.find(result_pre_text + 'metadataServerUrl').text
instance = re.search('://(.*).salesforce.com', instance)
instance = instance.group(1)

sf_api.setSession(instance, sessionId)

#Get Contacts From Salesforce Where Email is one of the #4 unique emails
contacts = sf_api.query_contacts(unique_emails)
sf_contacts = {}
for contact in contacts:
	sf_contacts[contact['Email']] = contact['Id']

#Get User Usage History Types
events = sf_api.query_usage_history_types()
sf_events = {}
for event in events:
	sf_events[event['Name']] = event['Id']

#Merge PeopleEvents + SF Contacts on Email + event ids
updates = []
non_sf_mixpanel_users = []
new_mixpanel_events = []
for i in range(0,len(people_events)):
	email = people_events[i]["Email"]
	event = people_events[i]["event_name"]
	if email in sf_contacts.keys(): #does that email show up in our sf dump?
		sf_contact_id = sf_contacts[email]
		people_events[i]["Contact__c"] = sf_contact_id
		if event in sf_events.keys():
			sf_event_id = sf_events[event]
			people_events[i]["Mixpanel__c"] = generate_event_id(people_events[i]['time'], people_events[i]['distinct_id'], sf_event_id)
			people_events[i]["User_Usage_History_Event_Type__c"] = sf_event_id
			people_events[i]['Event_Date_Created_UNIX__c'] = people_events[i].pop('time') #rename time field
			print "PEOPLE EVENT: %s" % people_events[i]
			updates.append(people_events[i]) # Add the completed User Usage History Event to a matched updates file.
		else:
			new_mixpanel_events.append({'event' : event})
	else:
		non_sf_mixpanel_users.append(people_events[i])

object_to_update = "User_Usage_History__c"
fields = {'Mixpanel__c' : 'Mixpanel__c', 'Product__c' : 'Product__c', 'Event_Date_Created_UNIX__c' : 'Event_Date_Created_UNIX__c', 'User_Usage_History_Event_Type__c': 'User_Usage_History_Event_Type__c', 'Contact__c': 'Contact__c'}

#Insert Into User Usage History Table
print "Creating job"

job_response = sf_api.create_job(instance, sessionId, 'upsert', object_to_update, 'XML', externalId="Mixpanel__c" )
job_response = ET.fromstring(job_response)

jobId = job_response[0].text
print "JOB_ID: %s" % (jobId)


#XMLify the JSON rows
xml_object = sf_api.json_to_xml_rows(updates, fields)

batch = 0
for xml_batch in xml_object.items():
	batch += 1
	print "Batch %d" % (batch)
	print xml_batch[1]
	print sf_api.add_batch(instance, sessionId, jobId, xml_batch[1])
	# print batch

print "Closing job"

close = sf_api.close_job(instance, sessionId, jobId)
# print close

print "Writing Out Logging CSVs"

if len(unmatched_events) > 0:
	unmatched_events_csv = CSVify(filename="AnonymousMixpanelEvents.csv", header=unmatched_events[0].keys())
	unmatched_events_csv.write(unmatched_events)

if len(new_mixpanel_events) > 0:
	new_mixpanel_events_csv = CSVify(filename="NewMixpanelEventNames.csv", header=new_mixpanel_events[0].keys())
	new_mixpanel_events_csv.write(new_mixpanel_events)

if len(non_sf_mixpanel_users) > 0:
	non_sf_mixpanel_users_csv = CSVify(filename="NonSFMixpanelUsers.csv", header=non_sf_mixpanel_users[0].keys())
	non_sf_mixpanel_users_csv.write(non_sf_mixpanel_users)

if len(updates) > 0:
	updates_csv = CSVify(filename="SuccessfulEvents.csv", header=updates[0].keys())
	updates_csv.write(updates)









