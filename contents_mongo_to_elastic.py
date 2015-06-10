import sys
from pymongo import Connection
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import pprint
import time

mongo_db_name = 'getty_18th'
es_index_name = 'getty_test'
es_doc_type = 'sale'

DRY_RUN = False

# Get data directly from MongoDB
# Make a connection to Mongo.
try:
    db_conn = Connection()
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

db = db_conn[mongo_db_name]

# Now also make a connection to Elasticsearch
es = Elasticsearch()

# DANGER -- Delete index!!
if not DRY_RUN and es.indices.exists( index = es_index_name ):
    es.indices.delete(es_index_name)

case_mapping = { "properties": {
    "title": { "analyzer": "french", "type": "string" },
    "sale_location": { "index": "not_analyzed", "type": "string" },
    "artist_nationality": { "index": "not_analyzed", "type": "string" },
    "artist_nationality": { "index": "not_analyzed", "type": "string" },
    "lot_number": { "index": "not_analyzed", "type": "string" }
  }
}

# Create the index
if not es.indices.exists( index = es_index_name ):
    es.indices.create( index = es_index_name, body={ "number_of_shards": 1 } )
    es.indices.put_mapping(index=es_index_name, doc_type=es_doc_type, body=case_mapping)


# pp = pprint.PrettyPrinter(indent=2)
# pp.pprint( es.indices.get_settings( index = es_index_name ) )

# Direct from MongoDB method
ii = 0

# Storing iterator of actions to feed to bulk api
# NOTE: Potential memory problems since gatering them all in memory between feeding to ES!
actions = []
start_time = time.time()

# TODO: Make it so you don't have to specify MongoDB doc type here...
for doc in db.contents.find({}):
    if ii % 10000 == 0:
        # Actually feed docs to elasticsearch bulk api for indexing
        res = helpers.bulk(es, actions)
        print ii, res, time.time() - start_time
        actions = []
    
    # Replace a couple pieces that ES can't serialize from the mongo object
    id_str = str(doc['_id'])
    doc['_id'] = id_str
    
    # TYPO: "1784/0426" for sale_begin_date
    if doc['sale_begin_date'] == "1784/0426":
        doc['sale_begin_date'] = "1784/04/26"
        
    # Problems with some dates. Double zeros in either month or day slot.
    # Trying to find out from the Getty what this means...
    # Perhaps making a horrible approximation, but substituting 01 for now...
    if doc['sale_begin_date'].find('/00') >= 0:
        doc['sale_begin_date'] = doc['sale_begin_date'].replace('/00', '/01')
        
    # Data mistake? 1773/02/29 is listed in descriptions as 1773/03/29
    if doc['sale_begin_date'] == '1773/02/29':
        doc['sale_begin_date'] = '1773/03/29'
        
    # Objects in arrays aren't well supported by kibana, so making a new array of artist nationalities
    if len(doc['artist_info']) > 0:
        doc['artist_nationality'] = []
        for aa in doc['artist_info']:
            if 'nationality' in aa:
                doc['artist_nationality'].append(aa['nationality'])
            else:
                doc['artist_nationality'].append('unknown')
        
    # Add doc to actions list
    doc.update({'_index':es_index_name, '_type':es_doc_type, '_op_type':'create' })
    actions.append(doc)
    
    ii += 1

# Actually feed docs to elasticsearch bulk api for indexing
res = helpers.bulk(es, actions)
print res, time.time() - start_time

