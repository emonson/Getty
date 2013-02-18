import os
import re
import sys
import codecs
from pymongo import Connection

data_dir = '/Users/emonson/Data/Getty'

catalogue_file = 'monson_sales_descriptions.txt'
catalogue_path = os.path.join(data_dir, catalogue_file)

fields_file = 'format_and_fields.txt'
fields_path = os.path.join(data_dir, fields_file)

data_file = 'monson_sales_contents.txt'
data_path = os.path.join(data_dir, data_file)

re_dot = re.compile(r'\.')
re_dotcomma = re.compile(r'[.,]')
re_spdash = re.compile(r'[ -]')
re_spmult = re.compile(r' {2,}')

# Make a connection to Mongo.
try:
    db_conn = Connection()
    # db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

db = db_conn['getty']

# * * DANGER !!!! * *
db.catalogues.drop()
db.fields.drop()
# * *


# Catalogue descriptions
print "Loading Catalogues into DB"
cat_in = codecs.open(catalogue_path, 'r', 'utf-8')

# Read in titles line, strip out periods, turn to lowercase, and put in underscores
titles = cat_in.readline().split('\t')
cat_field_names = [re_spdash.sub('_',re_dot.sub('',x.strip().lower())) for x in titles]

for line in cat_in:
	data = line.rstrip().split('\t')
	cat_doc = {}
	for ii, item in enumerate(data):
		# strip double quotes and turn multiple spaces into single
		item_base = re_spmult.sub(' ', item.strip(' "'))
		if item_base:
			cat_doc[cat_field_names[ii]] = item_base
	db.catalogues.save(cat_doc)


# Sales contents fields
print "Loading Fields into DB"
fields_in = codecs.open(fields_path, 'r', 'utf-8')

# Read in titles line
titles = fields_in.readline().split('\t')
field_field_names = [re_spdash.sub('_',re_dot.sub('',x.strip().lower())) for x in titles]

for line in fields_in:
	data = line.rstrip().split('\t')
	field_doc = {}
	for ii, item in enumerate(data):
		# strip double quotes and turn multiple spaces into single
		item_base = re_spmult.sub(' ', item.strip(' "'))
		if item_base:
			field_doc[field_field_names[ii]] = item_base
			# Reduce full field name to lowercase underscore version
			if field_field_names[ii] == 'full_field_name':	
				item_base_under = re_spdash.sub('_',re_dotcomma.sub('',item_base.strip().lower()))
				field_doc['db_field_name'] = item_base_under
	db.fields.save(field_doc)

sys.exit()

	
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