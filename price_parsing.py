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
                        (r'fls?', 'FLORINS'),
                        (ur'\u00A3', 'POUNDS'),
                        (ur'\u00E9cus', 'ECU'),
                        (ur'\u00E9cu', 'ECU'),
                        (r'\[?ou\]?', 'OR'),
                        (r'\[?or\]?', 'OR'),
                        (r'\[?o\xf9\]?', 'OR'),
                        (r'&', 'AND'),
                        (r'and', 'AND'),
                        (r'\[?et\]?', 'AND'),
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
                        (r'\[[A-Za-z]{1,3}-[A-Za-z]{1,3}\]', 'LOTRNG'),
                        (r'\[\?\]', 'QU'),
                        (r'\|[cd]', 'SUBFIELD'),
                        (r'-', 'RNG'),
                        (ur'\u00E0', 'RNG'),
                        (r'\w+', 'X')
                        ]

# Defining tokenizing patterns directly from tag patterns so only defined once
token_patterns = '|'.join([x for x,y in tag_patterns]).strip('|')
tokenizer = nltk.tokenize.RegexpTokenizer(token_patterns, flags=re.UNICODE)
regexp_tagger = nltk.RegexpTagger(tag_patterns)

re_subfield = re.compile(r'(\|\w) *')
re_lotrng = re.compile(r'\[([A-Za-z]{1,3})-([A-Za-z]{1,3})\]')
tag_sets_counter = collections.Counter()

# Switch this to False if only want to explore price fields and not really update DB
UPDATE = False
DEBUG = False
VERBOSE = True

# Functions for parsing lot ranges
# Converting to base 26 since all lot ranges in the French 18th C data so far are of
# the form [d-f] or [a-aa] or [zc-zg]
def base26(s, digits):
    return sum([ (ord(c)-ord('`'))*(26**i) for i,c in enumerate(s.rjust(digits,'`')[::-1]) ])

def n_in_lotrng(lotrng):
    mod_matches = re_lotrng.match(lotrng)
    if mod_matches is None:
        print '** lot range parsing problem! **'
        return 0
    mods = mod_matches.groups()
    digits = max([len(s) for s in mods])
    
    # lots in range are inclusive
    return base26(mods[1],digits) - base26(mods[0],digits) + 1
    

# ---------------------------------------
# First pass for parsing price and lots

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
    # Had included unset in case running through in multiple passes
    # up['$unset'] = {}
    # upunset = up['$unset']
            
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
                if tag_set.find('NUM CUR LOTMOD') >= 0:
                    print price_field, '--', entry['lot_number']
                    # print '_'.join(tokens)
            
            if UPDATE:

                # -------------------
                # Actual updates

                if tag_set == 'NUM LIVRES':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = 'livres'
            
                elif tag_set == 'NUM LIVRES NUM':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/20.0
                    upset['currency'] = 'livres'
            
                elif tag_set == 'NUM FRANCS':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = 'francs'

                elif tag_set == 'POUR LES LOTS NOS NUM LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[5][0])
                
                # NOTE: Using 100 sub-francs
                elif tag_set == 'NUM FRANCS NUM':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/100.0
                    upset['currency'] = 'francs'
                    
                elif tag_set == 'NUM LIVRES NUM SOLS':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/20.0
                    upset['currency'] = 'livres'

                elif tag_set == 'POUR LES LOTS NOS NUM AND NUM':
                    upset['n_lots'] = 2
                    
                elif tag_set == 'POUR LES LOTS NOS NUM LOTMOD AND NUM LOTMOD':
                    upset['n_lots'] = 2

                elif tag_set == 'POUR LES LOTS NUM LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[4][0])

                elif tag_set == 'POUR LES LOTS NUM LOTMOD AND LOTMOD':
                    upset['n_lots'] = 2
                    
                elif tag_set == 'NUM ASSIGNATS':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = 'assignats'
                
                # NOTE: Flag for two prices is existance of price_decimal_or field
                elif tag_set == 'NUM OR NUM LIVRES':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['price_decimal_or'] = float(tagged[2][0])
                    upset['currency'] = 'livres'

                elif tag_set == 'POUR LES LOTS NOS NUM LOTMOD AND LOTMOD':
                    upset['n_lots'] = 2
                
                elif tag_set == 'LIVRES':
                    upset['currency'] = 'livres'
                
                elif tag_set == 'NUM':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = 'unknown'
                    
                elif tag_set == 'POUR LES LOTS NOS NUM LOTRNG AND NUM LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[5][0]) + n_in_lotrng(tagged[8][0])
                    
                # These specifiers with number ranges are problematic because sometimes the
                #   ranges they specify have lots with letter modifiers in them. Marking as mistrust...
                elif tag_set == 'POUR LES LOTS NOS NUM RNG NUM':
                    # lot_tokens = tokenizer.tokenize(entry['lot_number'])
                    # lot_tagged = regexp_tagger.tag(lot_tokens)
                    # lot_tag_set = ' '.join([b for (_,b) in lot_tagged])
                    # if lot_tag_set.find('LOTMOD') >= 0:
                    #     upset['lot_parsing_notes'] = 'mistrust'
                    upset['lot_parsing_notes'] = 'mistrust'
                    upset['n_lots'] = (int(tagged[6][0]) - int(tagged[4][0])) + 1

                elif tag_set == 'POUR LES LOTS NOS NUM LOTRNG AND NUM':
                    upset['n_lots'] = n_in_lotrng(tagged[5][0]) + 1
                    
                elif tag_set == 'POUR LES LOTS NOS NUM AND NUM LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[7][0]) + 1

                elif tag_set == 'POUR LES LOTS NUM AND NUM':
                    upset['n_lots'] = 2

                elif tag_set == 'POUR LES LOTS NUM LOTMOD AND NUM LOTMOD':
                    upset['n_lots'] = 2

                elif tag_set == 'POUR LES LOTS X LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[4][0])

                elif tag_set == 'POUR LES LOTS NOS NUM AND NUM LOTMOD':
                    upset['n_lots'] = 2

                elif tag_set == 'FRANCS':
                    upset['currency'] = 'francs'

                # NOTE: Flag for two prices is existance of price_decimal_or field
                elif tag_set == 'NUM OR NUM FRANCS':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['price_decimal_or'] = float(tagged[2][0])
                    upset['currency'] = 'francs'

                elif tag_set == 'POUR LES LOTS NOS NUM NUM AND NUM':
                    upset['n_lots'] = 3

                elif tag_set == 'POUR LES LOTS NUM LOTRNG AND NUM':
                    upset['n_lots'] = n_in_lotrng(tagged[4][0]) + 1

                elif tag_set == 'POUR LES LOTS NUM LOTRNG AND NUM LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[4][0]) + n_in_lotrng(tagged[7][0])

                elif tag_set == 'POUR LES LOTS NOS LOTMOD NUM LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[6][0])
                
                elif tag_set == 'ILLEGIBLE':
                    upset['lot_parsing_notes'] = 'illegible'
                    
                elif tag_set == 'POUR LES LOTS NOS NUM LOTMOD LOTMOD AND LOTMOD':
                    upset['n_lots'] = 3

                elif tag_set == 'POUR LES LOTS LOTMOD NUM LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[5][0])
                    
                # NOTE: Using 20 sub-assignats
                elif tag_set == 'NUM ASSIGNATS NUM':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/20.0
                    upset['currency'] = 'assignats'
                    
                # NOTE: Flag for two prices is existance of price_decimal_or field
                elif tag_set == 'NUM FRANCS OR NUM FRANCS':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['price_decimal_or'] = float(tagged[3][0])
                    upset['currency'] = 'francs'

                elif tag_set == 'POUR LES LOTS NOS NUM LOTMOD LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[6][0])
                
                elif tag_set == 'NUM FRANCS QU':
                    upset['price_parsing_notes'] = 'unparsed'
                
                elif tag_set == 'POUR LES NOS NUM LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[4][0])
                
                elif tag_set == 'NUM LIVRES QU':
                    upset['price_parsing_notes'] = 'unparsed'
                
                elif tag_set == 'POUR LES LOTS NUM AND NUM LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[6][0]) + 1

                # NOTE: Flag for two prices is existance of price_decimal_or field
                elif tag_set == 'NUM FRANCS NUM OR NUM FRANCS':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/100.0
                    upset['price_decimal_or'] = float(tagged[4][0])
                    upset['currency'] = 'francs'

                elif tag_set == 'POUR LES LOTS NOS LOTMOD NUM AND LOTMOD NUM':
                    upset['n_lots'] = 2

                # NOTE: Using 20 sub-ecus
                elif tag_set == 'NUM ECU NUM':
                    upset['price_decimal'] = float(tagged[0][0]) + float(tagged[2][0])/20.0
                    upset['currency'] = u'écus'

                elif tag_set == 'POUR LES LOTS NOS NUM LOTMOD LOTMOD AND NUM LOTMOD LOTMOD':
                    upset['n_lots'] = 2

                elif tag_set == 'POUR LES LOTS NOS X LOTMOD AND X LOTMOD':
                    upset['n_lots'] = 2
                
                elif tag_set == 'LIVRES NUM':
                    upset['price_decimal'] = float(tagged[1][0])/20.0
                    upset['currency'] = 'livres'
                
                elif tag_set == 'AVEC LES LOTS NUM':
                    upset['n_lots'] = 1
                
                elif tag_set == 'NUM LIVRES NUM QU':
                    upset['price_parsing_notes'] = 'unparsed'
                elif tag_set == 'NUM QU FRANCS':
                    upset['price_parsing_notes'] = 'unparsed'
                
                elif tag_set == 'POUR LES LOTS NOS NUM LOTRNG AND NUM LOTRNG AND NUM LOTRNG':
                    upset['n_lots'] = n_in_lotrng(tagged[5][0]) + n_in_lotrng(tagged[8][0]) + n_in_lotrng(tagged[11][0])

                elif tag_set == 'POUR LES NOS NUM LOTMOD AND NUM LOTMOD':
                    upset['n_lots'] = 2

                # These specifiers with number ranges are problematic because sometimes the
                #   ranges they specify have lots with letter modifiers in them. Marking as 'mistrust'...
                elif tag_set == 'POUR LES LOTS NOS NUM NUM RNG NUM':
                    upset['lot_parsing_notes'] = 'mistrust'
                    upset['n_lots'] = (int(tagged[7][0]) - int(tagged[5][0])) + 2
                
                elif tag_set == 'NUM FLORINS':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = 'florins'

                elif tag_set == 'QU':
                    upset['price_parsing_notes'] = 'unparsed'

                elif tag_set == 'NUM ECU':
                    upset['price_decimal'] = float(tagged[0][0])
                    upset['currency'] = u'écus'
                
                # Catch-alls for unparsed other patterns
                elif tag_set.startswith('NUM'):
                    upset['price_parsing_notes'] = 'unparsed'
                elif tag_set.startswith('POUR') or tag_set.startswith('AVEC'):
                    upset['lot_parsing_notes'] = 'unparsed'           
            
            
        # if VERBOSE: print
        # Only hits this after processing a line, not a tag
        subfield_flag = None

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


# ---------------------------------------
# Second pass for dividing price by lots if present

print
print 'Dividing price by lots'

query = {'price_decimal':{'$exists':True}}
n_records = db.contents.find(query).count()

for ii,entry in enumerate(db.contents.find(query,{'price_decimal':True,'price_decimal_or':True,'n_lots':True})):
    
    if ii % 10000 == 0:
        print str(ii).rjust(len(str(n_records))), '/', n_records, '(', float(ii)/float(n_records), ')'

    # Prepare update document
    ID = {'_id':entry['_id']}
    up = {}
    up['$set'] = {}
    upset = up['$set']
    
    if 'n_lots' in entry:
        upset['price_per_lot'] = float(entry['price_decimal'])/float(entry['n_lots'])
    else:
        upset['price_per_lot'] = float(entry['price_decimal'])
    
    if 'price_decimal_or' in entry:
        if 'n_lots' in entry:
            upset['price_per_lot_or'] = float(entry['price_decimal_or'])/float(entry['n_lots'])
        else:
            upset['price_per_lot_or'] = float(entry['price_decimal_or'])
    
    # -------------------
    # Do actual update of document in DB with new fields
    if UPDATE:
        db.contents.update(ID, up, upsert=False, multi=False)


if VERBOSE:
    print
    print 'number of tag sets', len(tag_sets_counter)

#     for tagset in sorted(tag_sets_counter):
#         print str(tag_sets_counter[tagset]).rjust(5), tagset
    
    cumulative = 0
    n_tagsets = sum([n for s,n in tag_sets_counter.iteritems()])
    for ii,tagset in enumerate(sorted(tag_sets_counter.items(), key=lambda x: x[1], reverse=True)):
        cumulative += tagset[1]
        print str(ii).rjust(3), str(tagset[1]).rjust(5), str(round(float(cumulative)/n_tagsets,4)).rjust(6), tagset[0]
        # print tagset[1]

