import time
import boto3
import csv

class CSVify(object):
	def __init__(self, filename, header):
		self.filename = time.strftime("%d%m%Y") + filename
		self.header = header
		self.csvfile = None

	def write(self, rows):
		with open('/tmp/' + self.filename, 'wb') as csvfile:

			writer = csv.writer(csvfile, delimiter=',')
			writer.writerow(self.header)
			for row in rows:
				try:
					cells = []
					for column in row.keys():
						cells.append(row[column])
					writer.writerow(cells)
				except:
					print("----------- BAD LINE -----------")
		self.uploadS3()

	def uploadS3(self):
		if self.filename is not None:
			s3_client = boto3.client('s3')
			print "Uploading %s to s3" % self.filename
			s3_client.upload_file('tmp/' + self.filename, '<S3 Bucket>', time.strftime("%d%m%Y") +'/' + self.filename)
		else:
			print "Please Write A CSV File First"
