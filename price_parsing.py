import nltk
import os
import re
import sys
import codecs
from pymongo import Connection

# File method
# data_dir = '/Users/emonson/Data/Getty'
# data_file = 'lotprices.csv'
# data_path = os.path.join(data_dir, data_file)
# data_in = codecs.open(data_path, 'r', 'utf-8')

# Get data directly from MongoDB
# Make a connection to Mongo.
try:
    db_conn = Connection()
    # db_conn = Connection("emo2.trinity.duke.edu", 27017)
except ConnectionFailure:
    print "couldn't connect: be sure that Mongo is running on localhost:27017"
    sys.exit(1)

db = db_conn['getty']

# Tokenizer
# token_patterns = r'''
#          \|\w                 # subsection dividers 
#        | \[\w\]               # lot tags
#        | \[\w-\w\]                # lot tag range
#        | \d+\.?(\d+)?         # numbers (currency amounts)
#        | [&]                          # ampersand
#        | \[\?\]                       # question mark
#        | lots                         # lots indicator
#        | \w+              # other words
#     '''
# 
# # token_patterns = r'\|\w|\[[a-zA-Z- ]+\]|\w+|\d+(?:[.\d]+)?|\W+|\S+'
# tokenizer = nltk.tokenize.RegexpTokenizer(token_patterns, flags=re.VERBOSE)
# tokenizer = nltk.tokenize.RegexpTokenizer(r'\|\w|\[\w\]|\[\w-\w\]|\d+\.?(\d+)?|[&]|\w+')


# Define tagger
tag_patterns = [(r'[0-9]+(?:(?:\.|:)[0-9]+){0,2}', 'NUM'),
                        (r'livre(s)?', 'CUR'),
                        (r'franc(s)?', 'CUR'),
                        (r'fl', 'CUR'),
                        (ur'\u00A3', 'CUR'),
                        (r'^ou$', 'OR'),
                        (r'&', 'AND'),
                        (r'and', 'AND'),
                        (ur'\u00E0', 'RNG'),
                        (r'lots', 'LOTS'),
                        (r'nos\.?', 'NOS'),
                        (r'\[[a-z][a-z]?\]', 'LOTMOD'),
                        (r'\[[a-z][a-z]?-[a-z][a-z]?\]', 'LOTRNG'),
                        (r'\[\?\]', 'QU'),
                        (r'\|[a-z]', 'SUBFIELD'),
                        (r'-', 'RNG'),
                        (r'\w+', 'O')
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

# File method
# for line in data_in:
#   cols = line.split(',')
#   price = cols[2].strip(' "\n\r')
# Direct from MongoDB method
for entry in db.sales.find({'price':{'$exists':True},'country_authority':'France'},{'price':True}):
    price = entry['price']
    
    if price.find(',') >= 0:
        print price
        
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
                # Tagging
                # tokens = nltk.word_tokenize(price)
                tokens = tokenizer.tokenize(price_field)
                # tokens = nltk.regexp_tokenize(price, token_patterns)
                print '    ', '_'.join(tokens)

                tagged = regexp_tagger.tag(tokens)
                print tagged
            
                # Chunking
                if subfield_flag == None:
                    tree = ch_amt.parse(tagged)
                    print tree
                    for subtree in tree.subtrees():
                        if subtree.label() == 'AMT':
                            print 'AMT', subtree.leaves()
            
                elif subfield_flag == 'notes':
                    tree = ch_lots.parse(tagged)
                    n_incs = 0
                    for subtree in tree.subtrees():
                        if subtree.label() == 'INC':
                            # Note: problem printing subtree if one of the entries is a unicode non-ascii...
                            print 'INC', subtree.leaves()
                            n_incs += 1
                    if n_incs == 0:
                        print '*** NOTES NOT CAUGHT **', len(price)
            
                
            print
            # Only hits this after processing a line, not a tag
            subfield_flag = None
