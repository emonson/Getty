import os
import re
import sys
import codecs
from datetime import datetime
from pymongo import Connection

data_dir = '/Users/emonson/Data/Getty'
script_dir = '/Users/emonson/Programming/ArtMarkets/Getty'

catalogue_file = 'monson_sales_descriptions.txt'
catalogue_path = os.path.join(data_dir, catalogue_file)

fields_file = 'format_and_fields.txt'
fields_path = os.path.join(script_dir, fields_file)

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
db.sales.drop()
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
sales_fields = {}
sales_blocks = {}
sales_repeats = {}
sales_formats = {}

# Read in titles line
titles = fields_in.readline().split('\t')
field_field_names = [re_spdash.sub('_',re_dot.sub('',x.strip().lower())) for x in titles]

for line in fields_in:
	data = line.rstrip().split('\t')
	field_doc = {}
	for ii, item in enumerate(data):
		# strip double quotes and turn multiple spaces into single
		item_base = re_spmult.sub(' ', item.strip(' "'))
		# If anything left after stripping extras
		if item_base:
			field_doc[field_field_names[ii]] = item_base
			# Reduce full field name to lowercase underscore version
			if field_field_names[ii] == 'full_field_name':	
				item_base_under = re_spdash.sub('_',re_dotcomma.sub('',item_base.strip().lower()))
				field_doc['db_field_name'] = item_base_under
				# Record dictionary of file_label -> db_field_name
				sales_fields[field_doc['file_label']] = item_base_under
			if field_field_names[ii] == 'repeat':
				sales_repeats[field_doc['file_label']] = item_base
			if field_field_names[ii] == 'block':
				sales_blocks[field_doc['file_label']] = item_base
			if field_field_names[ii] == 'subfields':
				sales_formats[field_doc['file_label']] = item_base
				
	db.fields.save(field_doc)


# Sales contents actual data
print "Loading Sales Contents into DB"
data_in = codecs.open(data_path, 'r', 'utf-8')

doc = {}
current_field = None
current_block = None
index = 0
field = None
block = None

for line in data_in:
	# TODO: Need some sort of progress indicator...
	if line.startswith(' '):
		doc[current_field] += ' ' + line.strip()
		continue
	key = line[:17].strip()
	value = line[17:].strip()
	if key == '--RECORD NUMBER--':
		if current_field is not None:		# don't save on first line of the file
			db.sales.save(doc)
		doc = {}
		current_field = 'record_number'	# or could do None...
	else:
		# Check first to see if field name is one we know about
		if key not in sales_fields:
			sys.exit('Problem with key ' + key)	
		else:
			field = sales_fields[key]

			# This is where real tests start for constructing document
			
			# Repeats False
			if key not in sales_repeats:
				# key == record number taken care of above, so only F covered here
				current_field = field
				doc[field] = value
			
			# Repeats True
			else:
				
				# Blocks False -- List of items
				if key not in sales_blocks:
										
					# Field == current_field False
					if field != current_field:
						current_field = field
						index = 0
						doc[field] = []
						
					# Field == current_field True
					else:
						index += 1
					
					doc[field].append(value)
					
				
				# Blocks True -- List of dicts / objects
				else:
					block = sales_blocks[key]
					
					# Block == current_block False
					if block != current_block:
						doc[block] = []
						current_block = block
					
					# Block == current_block True
					else:
						
			