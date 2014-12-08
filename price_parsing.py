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

tag_patterns = [(r'[0-9]+(?:(?:\.|:)[0-9]+){0,2}', 'NUM'),
                        (r'livre(s)?', 'LIVRES'),
                        (r'livrs', 'LIVRES'),
                        (r'lires', 'LIVRES'),
                        (r'lives', 'LIVRES'),
                        (r'ivres', 'LIVRES'),
                        (r'Livres', 'LIVRES'),
                        (r'pouces?', 'POUCES'),
                        (r'lignes?', 'LIGNES'),
                        (r'sous?', 'SOLS'),
                        (r'sols?', 'SOLS'),
                        (r'franc(s)?', 'FRANCS'),
                        (r'[Ff]rs?', 'FRANCS'),
                        (r'assignats', 'ASSIGNATS'),
                        (r'fl', 'CUR'),
                        (ur'\u00A3', 'CUR'),
                        (ur'\u00E9cus', 'ECU'),
                        (ur'\u00E9cu', 'ECU'),
                        (r'\[?ou\]?', 'OR'),
                        (r'\[?or\]?', 'OR'),
                        (r'\[?o\xf9\]?', 'OR'),
                        (r'&', 'AND'),
                        (r'and', 'AND'),
                        (r'\[?et\]?', 'AND'),
                        (ur'\u00E0', 'RNG'),
                        (r'[Pp]our', 'POUR'),
                        (r'[Aa]vec', 'AVEC'),
                        (r'les?', 'LES'),
                        (r'lots?', 'LOTS'),
                        (r'lora', 'LOTS'),
                        (r'n[o\xba\xb0]s?\.?', 'NOS'),
                        (r'\[?coup\xe9\]?', 'COUPE'),
                        (r'\[illisible\]', 'ILLEGIBLE'),
                        (r"Le prix indiqu\xe9 par l'annotation manuscrite est peu lisible.", 'ILLEGIBLE'),
                        (r'[a-z]?\[[A-Za-z][a-z]?[\]\)]', 'LOTMOD'),
                        (r'[a-z]', 'LOTMOD'),
                        (r'\[[a-z][a-z]?-[a-z][a-z]?\]', 'LOTRNG'),
                        (r'\[\?\]', 'QU'),
                        (r'\|[cd]', 'SUBFIELD'),
                        (r'-', 'RNG'),
                        (r'\w+', 'X')
                        ]

# Defining tokenizing patterns directly from tag patterns so only defined once
token_patterns = '|'.join([x for x,y in tag_patterns]).strip('|')
tokenizer = nltk.tokenize.RegexpTokenizer(token_patterns, flags=re.UNICODE)
regexp_tagger = nltk.RegexpTagger(tag_patterns)

re_subfield = re.compile(r'(\|\w) *')
tag_sets_counter = collections.Counter()

# Switch this to False if only want to explore price fields and not really update DB
UPDATE = True
DEBUG = False
VERBOSE = False

# File method
# for line in data_in:
#   cols = line.split(',')
#   price = cols[2].strip(' "\n\r')
# Direct from MongoDB method
query = {'price':{'$exists':True},'country_authority':'France'}
n_records = db.contents.find(query).count()

for ii,entry in enumerate(db.contents.find(query,{'price':True,'lot_number':True,'price_decimal':True})):
    
    if ii % 10000 == 0:
        print str(ii).rjust(len(str(n_records))), '/', n_records, '(', float(ii)/float(n_records), ')'

    price = entry['price']
    # print u'———————————'
    # print entry
    
    # Prepare update document
    ID = {'_id':entry['_id']}
    up = {}
    up['$set'] = {}
    upset = up['$set']
    up['$unset'] = {}
    upunset = up['$unset']
            
    # split on |c type subsection divider
    price_subfields = re_subfield.split(price)
    subfield_flag = None
    
    for price_field in price_subfields:
        
        # Sometimes there are just empty currency or notes fields
        if price_field == '|c':
            subfield_flag = 'currency'
            continue
        elif price_field == '|d':
            subfield_flag = 'notes'
            continue
        
        if price_field:
            
            # Tokenizing
            tokens = tokenizer.tokenize(price_field)
            # print '    ', '_'.join(tokens)
            
            # Tagging
            tagged = regexp_tagger.tag(tokens)
            # print tagged
            
            # See what all of the tag sets are
            if any([(b==None) for (_,b) in tagged]):
                print ii, price_field
                print tagged
            tag_set = ' '.join([b for (_,b) in tagged])
            tag_sets_counter[tag_set] += 1
            
            if DEBUG:
                if tag_set.find('NUM ECU NUM') >= 0:
                    print price_field, '--', entry['lot_number']
                    # print '_'.join(tokens)
            
            if UPDATE:

                # -------------------
                # Actual updates

                if tag_set == 'NUM LIVRES':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = 'livres'
                    upunset['unparsed_price'] = ''
            
                elif tag_set == 'NUM LIVRES NUM':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/20.0
                    upset['currency'] = 'livres'
                    upunset['unparsed_price'] = ''
            
                elif tag_set == 'NUM FRANCS':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = 'francs'
                    upunset['unparsed_price'] = ''

                elif tag_set == 'POUR LES LOTS NOS NUM LOTRNG':
                    upset['unparsed_lots'] = True
                
                # NOTE: Using 100 sub-francs
                elif tag_set == 'NUM FRANCS NUM':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/100.0
                    upset['currency'] = 'francs'
                    upunset['unparsed_price'] = ''
                    
                elif tag_set == 'NUM LIVRES NUM SOLS':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/20.0
                    upset['currency'] = 'livres'
                    upunset['unparsed_price'] = ''

                elif tag_set == 'POUR LES LOTS NOS NUM AND NUM':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NOS NUM LOTMOD AND NUM LOTMOD':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NUM LOTRNG':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NUM LOTMOD AND LOTMOD':
                    upset['unparsed_lots'] = True
                    
                elif tag_set == 'NUM ASSIGNATS':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = 'assignats'
                    upunset['unparsed_price'] = ''
                
                # NOTE: Flag for two prices is existance of price_decimal_or field
                elif tag_set == 'NUM OR NUM LIVRES':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['price_decimal_or'] = float(tagged[2][0])
                    upset['currency'] = 'livres'
                    upunset['unparsed_price'] = ''

                elif tag_set == 'POUR LES LOTS NOS NUM LOTMOD AND LOTMOD':
                    upset['unparsed_lots'] = True
                
                elif tag_set == 'LIVRES':
                    upset['currency'] = 'livres'
                    upunset['unparsed_price'] = ''
                
                elif tag_set == 'NUM':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = 'unknown'
                    upunset['unparsed_price'] = ''
                    
                elif tag_set == 'POUR LES LOTS NOS NUM LOTRNG AND NUM LOTRNG':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NOS NUM RNG NUM':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NOS NUM LOTRNG AND NUM':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NOS NUM AND NUM LOTRNG':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NUM AND NUM':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NUM LOTMOD AND NUM LOTMOD':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS X LOTRNG':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NOS NUM AND NUM LOTMOD':
                    upset['unparsed_lots'] = True

                elif tag_set == 'FRANCS':
                    upset['currency'] = 'francs'
                    upunset['unparsed_price'] = ''

                # NOTE: Flag for two prices is existance of price_decimal_or field
                elif tag_set == 'NUM OR NUM FRANCS':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['price_decimal_or'] = float(tagged[2][0])
                    upset['currency'] = 'francs'
                    upunset['unparsed_price'] = ''

                elif tag_set == 'POUR LES LOTS NOS NUM NUM AND NUM':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NUM LOTRNG AND NUM':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NUM LOTRNG AND NUM LOTRNG':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NOS LOTMOD NUM LOTRNG':
                    upset['unparsed_lots'] = True
                elif tag_set == 'ILLEGIBLE':
                    pass
                elif tag_set == 'POUR LES LOTS NOS NUM LOTMOD LOTMOD AND LOTMOD':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS LOTMOD NUM LOTRNG':
                    upset['unparsed_lots'] = True
                    
                # NOTE: Using 20 sub-assignats
                elif tag_set == 'NUM ASSIGNATS NUM':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/20.0
                    upset['currency'] = 'assignats'
                    upunset['unparsed_price'] = ''
                    
                # NOTE: Flag for two prices is existance of price_decimal_or field
                elif tag_set == 'NUM FRANCS OR NUM FRANCS':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['price_decimal_or'] = float(tagged[3][0])
                    upset['currency'] = 'francs'
                    upunset['unparsed_price'] = ''

                elif tag_set == 'POUR LES LOTS NOS NUM LOTMOD LOTRNG':
                    upset['unparsed_lots'] = True
                
                elif tag_set == 'NUM FRANCS QU':
                    upset['unparsed_price'] = True
                
                elif tag_set == 'POUR LES NOS NUM LOTRNG':
                    upset['unparsed_lots'] = True
                
                elif tag_set == 'NUM LIVRES QU':
                    upset['unparsed_price'] = True
                
                elif tag_set == 'POUR LES LOTS NUM AND NUM LOTRNG':
                    upset['unparsed_lots'] = True

                # NOTE: Flag for two prices is existance of price_decimal_or field
                elif tag_set == 'NUM FRANCS NUM OR NUM FRANCS':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/100.0
                    upset['price_decimal_or'] = float(tagged[4][0])
                    upset['currency'] = 'francs'
                    upunset['unparsed_price'] = ''

                elif tag_set == 'POUR LES LOTS NOS LOTMOD NUM AND LOTMOD NUM':
                    upset['unparsed_lots'] = True

                # NOTE: Using 20 sub-ecus
                elif tag_set == 'NUM ECU NUM':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/20.0
                    upset['currency'] = 'ecus'
                    upunset['unparsed_price'] = ''

                elif tag_set == 'POUR LES LOTS NOS NUM LOTMOD LOTMOD AND NUM LOTMOD LOTMOD':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NOS X LOTMOD AND X LOTMOD':
                    upset['unparsed_lots'] = True
                
                elif tag_set == 'LIVRES NUM':
                    upset['price_decimal'] = float(tagged[1][0])/20.0
                    upset['currency'] = 'livres'
                    upunset['unparsed_price'] = ''
                
                elif tag_set == 'AVEC LES LOTS NUM':
                    upset['unparsed_lots'] = True
                
                elif tag_set == 'NUM LIVRES NUM QU':
                    upset['unparsed_price'] = True
                elif tag_set == 'NUM QU FRANCS':
                    upset['unparsed_price'] = True
                
                elif tag_set == 'POUR LES LOTS NOS NUM LOTRNG AND NUM LOTRNG AND NUM LOTRNG':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES NOS NUM LOTMOD AND NUM LOTMOD':
                    upset['unparsed_lots'] = True
                elif tag_set == 'POUR LES LOTS NOS NUM NUM RNG NUM':
                    upset['unparsed_lots'] = True
                
                elif tag_set == 'NUM CUR LOTMOD':
                    upset['unparsed_price'] = True
                elif tag_set == 'QU':
                    upset['unparsed_price'] = True

                elif tag_set == 'NUM ECU':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = 'ecus'
                    upunset['unparsed_price'] = ''

                elif tag_set.startswith('NUM'):
                    upset['unparsed_price'] = True
                elif tag_set.startswith('POUR') or tag_set.startswith('AVEC'):
                    upset['unparsed_lots'] = True           
            
            
        # -------------------
        # Do actual update of document in DB with new fields
        if UPDATE:
            # HACK: Don't know why, but the update won't work if up['$unset'] is present but empty...
            if len(upunset) == 0:
                upunset['xxx'] = ''
            db.contents.update(ID, up, upsert=False, multi=False)
            # print ID
            # print up

        if VERBOSE: print
        # Only hits this after processing a line, not a tag
        subfield_flag = None

if VERBOSE:
    print
    print 'number of tag sets', len(tag_sets_counter)

    for tagset in sorted(tag_sets_counter):
        print str(tag_sets_counter[tagset]).rjust(5), tagset
    # for ii,tagset in enumerate(sorted(tag_sets_counter.items(), key=lambda x: x[1], reverse=True)):
    #     print str(ii).rjust(3), str(tagset[1]).rjust(5), tagset[0]
    #     # print tagset[1]
