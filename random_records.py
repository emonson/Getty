import random
import numpy as N

fraction = 0.01
min_count = 1000
idx_offset = 1

num_records = {}
num_records['BELGIANSALES'] = 85651
num_records['BRITISHSALES'] =	420207
num_records['DUTCHSALES'] =	50236
num_records['FRENCHSALES'] =	212431
num_records['GERMANSALES  (pre-1800)'] =	50517
num_records['SCANDISALES'] =	7461
num_records['GERMANSALES (1930-1945)'] = 982967

f = open('rand_idxs.txt', 'w')

for db_name, count in num_records.items():
	subset_n = int( count*fraction )
	if subset_n < min_count:
		subset_n = min_count
	
	shuffled_idxs = N.arange(count) + idx_offset
	N.random.shuffle(shuffled_idxs)
	
	f.write(db_name)
	for idx in shuffled_idxs[0:subset_n]:
		f.write(', ')
		f.write(str(idx))
	f.write('\n')

f.close()