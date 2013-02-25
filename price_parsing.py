import nltk
import os
import re
import codecs

data_dir = '/Users/emonson/Data/Getty'
data_file = 'lotprices.csv'
data_path = os.path.join(data_dir, data_file)

data_in = codecs.open(data_path, 'r', 'utf-8')

# Tokenizer
token_patterns = r'''(?x)    # set flag to allow verbose regexps
         \|\w        			# subsection dividers 
       | \[\w\]        		# lot tags
       | |\d+\.?(\d+)?  	# numbers (currency amounts)
       | |\w+            	# other words
    '''

# token_patterns = r'\|\w|\[[a-zA-Z- ]+\]|\w+|\d+(?:[.\d]+)?|\W+|\S+'
tokenizer = nltk.tokenize.RegexpTokenizer(token_patterns)
tokenizer = nltk.tokenize.RegexpTokenizer(r'\|\w|\[\w\]|\[\w-\w\]|\d+\.?(\d+)?|[&]|\w+')


# Define tagger
chunk_patterns = [(r'[0-9]+(?:\.[0-9]+)?', 'NUM'),
						(r'livre(s)?', 'CUR'),
						(r'franc(s)?', 'CUR'),
						(r'^ou$', 'OR'),
						(r'&', 'AND'),
						(r'\[\w\]', 'LOTMOD'),
						(r'\[\w-\w\]', 'LOTRNG'),
						(r'\[\?\]', 'QU'),
						(r'\|\w', 'NOTE'),
						(r'.*', 'O')
						]

regexp_tagger = nltk.RegexpTagger(chunk_patterns)

re_subfield = re.compile(r'(\|\w) *')

for line in data_in:
	cols = line.split(',')
	price = cols[2].strip(' "\n\r')
	
	if price:
		
		# split on |c type subsection divider
		price_subfields = re_subfield.split(price)
		subfield_flag = None
		
		for price_field in price_subfields:
			if price_field == '|c':
				subfield_flag = 'currency'
				continue
			elif price_field == '|d':
				subfield_flag = 'notes'
				continue
			else:
				subfield_flag = None
			
			# Tagging
			# tokens = nltk.word_tokenize(price)
			tokens = tokenizer.tokenize(price_field)
			# tokens = nltk.regexp_tokenize(price, token_patterns)
			print price_field
			for x in tokens:
				print x + ' ',
			print
			# print
			tagged = regexp_tagger.tag(tokens)
	# 		print price
			print tagged
			print