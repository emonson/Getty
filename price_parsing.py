import nltk
import os
import codecs

data_dir = '/Users/emonson/Data/Getty'
data_file = 'lotprices.csv'
data_path = os.path.join(data_dir, data_file)

data_in = codecs.open(data_path, 'r', 'utf-8')

# Tokenizer
token_patterns = r'''(?x)    # set flag to allow verbose regexps
         ([A-Z]\.)+        # abbreviations, e.g. U.S.A. 
       | \w+(-\w+)*        # words with optional internal hyphens
       | \$?\d+\.?\d+?%?  # currency and percentages, e.g. $12.40, 82%
       | \.\.\.            # ellipsis
       | [][.,;"'?():-_`]  # these are separate tokens
    '''
# token_patterns = r'\|\w|\[[a-zA-Z- ]+\]|\w+|\d+(?:[.\d]+)?|\W+|\S+'
# tokenizer = nltk.tokenize.RegexpTokenizer(token_patterns)
tokenizer = nltk.tokenize.RegexpTokenizer(r'\|\w|\[\w\]|\d+\.?(\d+)?|\w+')

# Define tagger
chunk_patterns = [(r'[0-9]+', 'NUM'),
						(r'livre(s)?', 'CUR'),
						(r'franc(s)?', 'CUR'),
						(r'\[\w\]', 'LOTMOD'),
						(r'\[\?\]', 'QU'),
						(r'\|\w', 'NOTE'),
						(r'.*', 'O')
						]

regexp_tagger = nltk.RegexpTagger(chunk_patterns)

for line in data_in:
	cols = line.split(',')
	price = cols[2].strip(' "\n\r')
	
	if len(price) > 15:
		# Tagging
		# tokens = nltk.word_tokenize(price)
		tokens = tokenizer.tokenize(price)
		# tokens = nltk.regexp_tokenize(price, token_patterns)
		print price
		for x in tokens:
			print x + ' ',
		print
		# print
		tagged = regexp_tagger.tag(tokens)
# 		print price
		print tagged
		print