import os
import re
import sys
import codecs
from datetime import datetime
import pandas as pd
from pymongo import Connection

data_dir = '/Users/emonson/Data/Getty/18th_century'
script_dir = '/Users/emonson/Programming/ArtMarkets/Getty'

# Sales descriptions data (about the auctions themselves, not the "contents", which are the lots up for sale)
descriptions_file = '18th_cent_french_sales_contents_v2.txt'
descriptions_path = os.path.join(data_dir, descriptions_file)

# Tab-separated values text file describing data fields, their types, and whether they repeat
fields_file = '18th_cent_contents_fields.xlsx'
fields_path = os.path.join(script_dir, fields_file)

re_dot = re.compile(r'\.')
re_dotcomma = re.compile(r'[.,]')
re_spdash = re.compile(r'[ -]')
re_spmult = re.compile(r' {2,}')

# Fast file line count subroutine
# http://stackoverflow.com/questions/845058/how-to-get-line-count-cheaply-in-python
import subprocess
def file_len(fname):
    p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result, err = p.communicate()
    if p.returncode != 0:
        raise IOError(err)
    return int(result.strip().split()[0])
    
# Make a connection to Mongo.
try:
    db_conn = Connection()
    # db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

db = db_conn['getty_18th']

# * * DANGER !!!! * *
db.contents.drop()
db.contents_fields.drop()
# * *

# Contents fields

# Creating dictionaries so when we encounter a field name in the STAR sales descriptions
# text dump file, we'll know other things about it for use later in parsing the dump file:
#    whether it repeats
#    if it repeats, whether it's in a "block" of repeating fields
#    if it's non-string, what the data type is (how it should be interpreted/parsed)
#    
print "Loading Fields into DB"
df = pd.read_excel(open(fields_path,'r'))

# This will be keyed by the STAR_field_name
fields_properties = {}

for d in df.iterrows():
    # DataFrame.iterrows() returns a tuple with (index, Series), so have to pull apart d[1] to get row items
    # Get rid of NaNs from each row before saving doc to database and creating dict for use later
    # That way can test whether 'can_repeat' is in dict
    field_doc = dict([(k.lower(),v) for (k,v) in d[1].to_dict().iteritems() if pd.notnull(v)])
    fields_properties[field_doc['star_field_name']] = field_doc
    db.contents_fields.save(field_doc)

# Contents

print "Loading Sales Contents into DB from", descriptions_file
n_lines = file_len(descriptions_path)
data_in = codecs.open(descriptions_path, 'r', 'iso-8859-1')

doc = {}
current_field = None
current_block = None
index = 0
db_field = None
block = None
star_field_name = None
value = None

for ii, line in enumerate(data_in):
    if ii % 100000 == 0:
        print str(ii).rjust(len(str(n_lines))), '/', n_lines
    
    # continued lines
    if line.startswith(' '):
        # Need to figure out what sort of previous entry, so can append properly
        # ***
        if 'can_repeat' not in fields_properties[star_field_name]:
            doc[current_field] += ' ' + line.strip()
        else:
            # ***
            if 'block' not in fields_properties[star_field_name]:
                doc[current_field][index] += ' ' + line.strip()
            else:
                doc[current_block][index][current_field] += ' ' + line.strip()
        continue

    key = line[:17].strip()
    value = line[17:].strip()

    # blank lines
    if not key:
        continue
        
    # Only assign star_field_name if have not encountered a blank so can use in continuing line dict lookup
    star_field_name = key

    if star_field_name == '--RECORD NUMBER--':
        
        # Save the previous record to DB when hit the next
        if current_field is not None:       # don't save on first line of the file
            db.contents.save(doc)
        doc = {}
        current_field = 'record_number'     # or could do None...
        doc['record_number'] = value
        doc['data_file'] = descriptions_file
    
    else:
        
        # Check first to see if field name is one we know about
        # ***
        if star_field_name not in fields_properties:
            sys.exit('Problem with key ' + star_field_name + ' line ' + str(ii))    
        else:
            # ***
            db_field = fields_properties[star_field_name]['db_field_name']

            # This is where real tests start for constructing document
            # TODO: This is where type changes or additional parsing needs to happen on value...
        
            # Repeats False
            # ***
            if 'can_repeat' not in fields_properties[star_field_name]:
                # key == record number taken care of above, so only F covered here
                current_field = db_field
                doc[db_field] = value
        
            # Repeats True
            else:
            
                # Field == current_field False
                if db_field != current_field:
                    current_field = db_field
                    index = 0
                
                # Field == current_field True
                else:
                    index += 1
            
                # Blocks False -- List of items
                # ***
                if 'block' not in fields_properties[star_field_name]:
                
                    current_block = None

                    if db_field not in doc:
                        doc[db_field] = []
                    
                    doc[db_field].append(value)
            
                # Blocks True -- List of dicts / objects
                else:
                    # ***
                    block = fields_properties[star_field_name]['block']
                
                    # Block == current_block False
                    if block != current_block:
                        current_block = block

                    if block not in doc:
                        doc[block] = []
                
                    if len(doc[block]) < index + 1:
                        doc[block].append({})
                
                    doc[block][index][db_field] = value

# Save the final doc
if current_field is not None:       # don't save on first line of the file
    db.contents.save(doc)
               
        