#!/usr/bin/python
# -*- coding: utf-8 -*-

import nltk
import os
import re
import sys
import codecs
import collections
from pymongo import Connection
from pymongo.errors import ConnectionFailure

# Get data directly from MongoDB
# Make a connection to Mongo.
# NOTE: start up mongodb with:
# ulimit -n 1024 && mongod --config /usr/local/etc/mongod.conf

try:
    db_conn = Connection()
    # db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

db = db_conn['getty_18th']

# Define tagger
# Early tags well match before late ones, so order matters and can help give priority
# TODO: Should do another version of this that helps pull out some of the misspellings...
# NOTE: lot_number field seems to all contain leading zeroes to four places...
# NOTE: lot_number field often has modifiers actually in square brackets...

tag_patterns = [(r'[0-9]+([\.,][0-9]+)?', 'NUMDIG'),
                        (r'(de )?haut(eur)?\.?', 'HEIGHT'),
                        (r'haut\.r', 'HEIGHT'),
                        (r'hr?\.', 'HEIGHT'),
                        (r'(de )?large?(ur)?\.?', 'WIDTH'),
                        (r'larg\.r', 'WIDTH'),
                        (r'la?r?\.', 'WIDTH'),
                        (r'longueur', 'WIDTH'),
                        (r'profondeur', 'DEPTH'),
                        (r'diam\xe8tre', 'DIAMETER'),
                        (r'pouces?', 'POUCE'),
                        (r'po?u?c?\.?', 'POUCE'),
                        (r'pieds?', 'PIED'),
                        (r'pi\.', 'PIED'),
                        (r'lign?(es)?\.?', 'LIGNE'),
                        (ur'mètre', 'METER'),
                        (r'm\.?', 'METER'),
                        (ur'déc\.', 'DMETER'),
                        (ur'centimètres', 'CMETER'),
                        (r'cm?\.?', 'CMETER'),
                        (r'demi', 'HALF'),
                        (r'\xbd', 'HALF'),
                        (r'\xe9galement', 'EQUAL'),
                        (r'sic', 'SIC'),
                        (r's(ur)?\.?', 'BY'),
                        (r'et', 'AND'),
                        (r'&', 'AND'),
                        (r'\xe0', 'TO'),
                        (r'de', 'OF'),
                        (r'\[?\?\]?', 'QU')
                        ]

numbers_str = """
zéro, une?, deux, trois, quatre, cinq, six, sept, huit, neuf, dix, onze, douze, treize, quatorze, quinze, seize, 
dix-sept, dix-huit, dix-neuf, vingt, vingt et un, vingt-deux, vingt-trois, vingt-quatre, vingt-cinq, 
vingt-six, vingt-sept, vingt-huit, vingt-neuf, trente, trente et un, trente-deux, trente-trois, 
trente-quatre, trente-cinq, trente-six, trente-sept, trente-huit, trente-neuf, quarante, 
quarante et un, quarante-deux, quarante-trois, quarante-quatre, quarante-cinq, quarante-six, 
quarante-sept, quarante-huit, quarante-neuf, cinquante
"""

# Can't do more than 100 named groups...
numbers_extra_str = """
, cinquante et un, cinquante-deux, 
cinquante-trois, cinquante-quatre, cinquante-cinq, cinquante-six, cinquante-sept, cinquante-huit, 
cinquante-neuf, soixante, soixante et un, soixante-deux, soixante-trois, soixante-quatre, 
soixante-cinq, soixante-six, soixante-sept, soixante-huit, soixante-neuf, soixante-dix, 
soixante et onze, soixante-douze, soixante-treize, soixante-quatorze, soixante-quinze, 
soixante-seize, soixante-dix-sept, soixante-dix-huit, soixante-dix-neuf, quatre-vingts, 
quatre-vingt-un, quatre-vingt-deux, quatre-vingt-trois, quatre-vingt-quatre, quatre-vingt-cinq, 
quatre-vingt-six, quatre-vingt-sept, quatre-vingt-huit, quatre-vingt-neuf, quatre-vingt-dix, 
quatre-vingt-onze, quatre-vingt-douze, quatre-vingt-treize, quatre-vingt-quatorze, 
quatre-vingt-quinze, quatre-vingt-seize, quatre-vingt-dix-sept, quatre-vingt-dix-huit, 
quatre-vingt-dix-neuf, cent
"""

numbers_list = unicode(numbers_str.decode('utf-8').replace('\n','')).split(', ')
numbers_zip = zip(numbers_list, ['NUM']*len(numbers_list))

tag_patterns.extend(numbers_zip)
tag_patterns.append((r'\w+', 'X'))

# Defining tokenizing patterns directly from tag patterns so only defined once
token_patterns = '|'.join([x for x,y in tag_patterns]).strip('|')
tokenizer = nltk.tokenize.RegexpTokenizer(token_patterns, flags=re.UNICODE)
regexp_tagger = nltk.RegexpTagger(tag_patterns)

tag_sets_counter = collections.Counter()

# Switch this to False if only want to explore price fields and not really update DB
UPDATE = False
DEBUG = True
# DEBUG = False
# VERBOSE = True
VERBOSE = False

# ---------------------------------------
# First pass for parsing dimensions

query = {'dimensions':{'$exists':True}}
n_records = db.contents.find(query).count()

for ii,entry in enumerate(db.contents.find(query,{'dimensions':True})):
    
    if ii % 10000 == 0:
        print str(ii).rjust(len(str(n_records))), '/', n_records, '(', float(ii)/float(n_records), ')'

    dims = entry['dimensions'].strip().lower()
    # print u'———————————'
    # print entry
    
    # Prepare update document
    ID = {'_id':entry['_id']}
    up = {}
    up['$set'] = {}
    upset = up['$set']
            
    if dims:
        
        # Tokenizing
        tokens = tokenizer.tokenize(dims)
        # print '    ', '_'.join(tokens)
        
        # Tagging
        tagged = regexp_tagger.tag(tokens)
        # print tagged
        
        # See what all of the tag sets are
        if any([(b==None) for (_,b) in tagged]):
            print 'NONE:', ii, '--', dims
            print tagged
        tag_set = ' '.join([b for (_,b) in tagged if b is not None])
        tag_sets_counter[tag_set] += 1
        
        if DEBUG:
            if tag_set.find('NUMDIG X WIDTH') >= 0:
                print dims, tag_set
        
        if UPDATE:

            # -------------------
            # Actual updates

            if tag_set == 'NUM LIVRES':
                upset['price_decimal'] = float(tagged[0][0])
                upset['currency'] = 'livres'
          

    # -------------------
    # Do actual update of document in DB with new fields
    if UPDATE:
        # Had included unset in case running through in multiple passes
        # NOTE: Don't know why, but the update won't work if up['$unset'] is present but empty...
        # if len(upunset) == 0:
        #     del up['$unset']
        
        db.contents.update(ID, up, upsert=False, multi=False)
        # print ID
        # print up


if VERBOSE:
    print
    print 'number of tag sets', len(tag_sets_counter)

#     for tagset in sorted(tag_sets_counter):
#         print str(tag_sets_counter[tagset]).rjust(5), tagset
    for ii,tagset in enumerate(sorted(tag_sets_counter.items(), key=lambda x: x[1], reverse=True)):
        print str(ii).rjust(3), str(tagset[1]).rjust(5), tagset[0]
        # print tagset[1]
