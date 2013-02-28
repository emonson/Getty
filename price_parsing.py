import nltk
import os
import re
import codecs

data_dir = '/Users/emonson/Data/Getty'
data_file = 'lotprices.csv'
data_path = os.path.join(data_dir, data_file)

data_in = codecs.open(data_path, 'r', 'utf-8')

# Tokenizer
token_patterns = r'''
         \|\w        			# subsection dividers 
       | \[\w\]        		# lot tags
       | \[\w-\w\]				# lot tag range
       | \d+\.?(\d+)?  		# numbers (currency amounts)
       | [&]							# ampersand
       | lots							# lots indicator
       | \w+            	# other words
    '''

# token_patterns = r'\|\w|\[[a-zA-Z- ]+\]|\w+|\d+(?:[.\d]+)?|\W+|\S+'
tokenizer = nltk.tokenize.RegexpTokenizer(token_patterns, flags=re.VERBOSE)
# tokenizer = nltk.tokenize.RegexpTokenizer(r'\|\w|\[\w\]|\[\w-\w\]|\d+\.?(\d+)?|[&]|\w+')


# Define tagger
tag_patterns = [(r'[0-9]+(?:\.[0-9]+)?', 'NUM'),
						(r'livre(s)?', 'CUR'),
						(r'franc(s)?', 'CUR'),
						(r'^ou$', 'OR'),
						(r'&', 'AND'),
						(r'lots', 'LOTS'),
						(r'\[\w\]', 'LOTMOD'),
						(r'\[\w-\w\]', 'LOTRNG'),
						(r'\[\?\]', 'QU'),
						(r'\|\w', 'SUBFIELD'),
						(r'.*', 'O')
						]

regexp_tagger = nltk.RegexpTagger(tag_patterns)


# Chunker
price_grammar = '''
	AMT: 	{<NUM><CUR><NUM>}
				{<NUM><CUR>}
'''
ch_amt = nltk.RegexpParser(price_grammar)

lots_grammar = '''
	INC:	{<LOTS><O>*(<AND>?<NUM>(<LOTMOD>|<LOGRNG>)?)+}
'''
ch_lots = nltk.RegexpParser(lots_grammar)

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
			
			if price_field:
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
			
				# Chunking
				if subfield_flag == None:
					tree = ch_amt.parse(tagged)
					print tree
					for subtree in tree.subtrees():
						if subtree.node == 'AMT':
							print subtree
			
				elif subfield_flag == 'notes':
					tree = ch_lots.parse(tagged)
					n_incs = 0
					for subtree in tree.subtrees():
						if subtree.node == 'INC':
							print subtree
							n_incs += 1
					if n_incs == 0:
						print '*** PRICES NOT CAUGHT **', len(price)
			
				
			print
			# Only hits this after processing a line, not a tag
			subfield_flag = None
