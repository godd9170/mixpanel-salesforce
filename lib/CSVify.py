import time
import csv
import os

class CSVify(object):
	def __init__(self, filename, header):
		cwd = os.getcwd()
		if not os.path.isdir(cwd + '/' + time.strftime("%d%m%Y")):
			os.mkdir(time.strftime("%d%m%Y"))
		self.filename = time.strftime("%d%m%Y") + '/' + filename
		self.header = header

	def write(self, rows):
		with open(self.filename, 'wb') as csvfile:

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