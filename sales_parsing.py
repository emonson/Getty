import os
import re
import codecs
from pymongo import Connection

data_dir = '/Users/emonson/Programming/Getty'

fields_file = 'fields_and_formats.txt'
fields_path = os.path.join(data_dir, catalogue_file)

catalogue_file = 'monson_sales_descriptions.txt'
catalogue_path = os.path.join(data_dir, catalogue_file)

data_file = 'monson_sales_contents.txt'
data_path = os.path.join(data_dir, data_file)

# Make a connection to Mongo.
try:
    db_conn = Connection()
    # db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

db = db_conn['getty']


# Catalogue descriptions
print "Loading Catalogues into DB"
cat_in = codecs.open(catalogue_path, 'r', 'utf-8')

# Read in titles line
titles = cat_in.readline()

# Keep track of catalogue IDs for debugging
cat_ids = []

for line in cat_in:
	fields = line.rstrip().split('\t')
	tag = fields[0]
	cat_ids.append(tag)
	year = int(fields[1])
	# Additional fields...
	# Create catalogue dictionary / document
	# Put catalogue doc in db.catalogues collection


# Sales contents fields
print "Loading Fields into DB"
fields_in = codecs.open(fields_path, 'r', 'utf-8')

# Read in titles line
titles = fields_in.readline()
# Parse column names into field name document fields

for line in fields_in:
	fields = line.rstrip().split('\t')
	tag = fields[0]
	cat_ids.append(tag)
	year = int(fields[1])
	# Additional fields...
	# Create fields dictionary / document
	# Put fields doc in db.fields collection
	
	
# Sales contents actual data
print "Loading Sales Contents into DB"
data_in = codecs.open(data_path, 'r', 'utf-8')

# current_record = {}
# current_field = None
# Go line by line through sales contents file
# If line starts with " "
#   then append line to 
# If line starts with "--RECORD NUMBER--",
#   then new record dictionary