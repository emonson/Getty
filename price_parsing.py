#!/usr/bin/python
# -*- coding: utf-8 -*-

import nltk
import os
import re
import sys
import codecs
import collections
from pymongo import Connection

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


# Chunker
# TODO: Still need OR variants...
price_grammar = '''
    AMT:    {<NUM><CUR><NUM>}
                {<NUM><CUR>}
'''
ch_amt = nltk.RegexpParser(price_grammar)

# Note: the chunker seems to be NOT greedy, so it'll quit as soon as it
#   finds a match (and won't continue as long as it can)...
# TODO: still need num, num & num variant...
lots_grammar = '''
    INC:    {<LOTS><NOS>(<NUM><RNG><NUM>)}
                {<LOTS><NOS>(<AND>?<NUM>(<LOTMOD>|<LOTRNG>)?)+}
'''
ch_lots = nltk.RegexpParser(lots_grammar)

re_subfield = re.compile(r'(\|\w) *')

tag_sets_counter = collections.Counter()

VERBOSE = False

# File method
# for line in data_in:
#   cols = line.split(',')
#   price = cols[2].strip(' "\n\r')
# Direct from MongoDB method
for ii,entry in enumerate(db.contents.find({'price':{'$exists':True},'country_authority':'France'},{'price':True,'lot_number':True})):
    
    price = entry['price']
    # print u'———————————'
    # print price
    
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
            
            # DEBUG
            if tag_set.find('LOTMOD LOTMOD LOTMOD') >= 0:
                print price_field, '--', entry['lot_number']
                # print '_'.join(tokens)

#              
#             # Chunking 
#             if subfield_flag == None:
#                 tree = ch_amt.parse(tagged)
#                 print tree
#                 for subtree in tree.subtrees():
#                     if subtree.label() == 'AMT':
#                         print 'AMT', subtree.leaves()
#         
#             elif subfield_flag == 'notes':
#                 tree = ch_lots.parse(tagged)
#                 n_incs = 0
#                 for subtree in tree.subtrees():
#                     if subtree.label() == 'INC':
#                         # Note: problem printing subtree if one of the entries is a unicode non-ascii...
#                         print 'INC', subtree.leaves()
#                         n_incs += 1
#                 if n_incs == 0:
#                     print '*** NOTES NOT CAUGHT **', len(price)
#         
            
        if VERBOSE: print
        # Only hits this after processing a line, not a tag
        subfield_flag = None

print
print 'number of tag sets', len(tag_sets_counter)

# for tagset in sorted(tag_sets_counter):
#     print str(tag_sets_counter[tagset]).rjust(5), tagset
for ii,tagset in enumerate(sorted(tag_sets_counter.items(), key=lambda x: x[1], reverse=True)):
    print str(ii).rjust(3), str(tagset[1]).rjust(5), tagset[0]
    # print tagset[1]
