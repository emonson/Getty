import os
import codecs
import collections

data_dir = '/Users/emonson/Data/Getty/18th_century'
script_dir = '/Users/emonson/Programming/ArtMarkets/Getty'

# Sales descriptions data (about the auctions themselves, not the "contents", which are the lots up for sale)
descriptions_file = '18th_cent_french_sales_contents_v2.txt'
# descriptions_file = '18th_cent_sales_descriptions_v3.txt'
descriptions_path = os.path.join(data_dir, descriptions_file)

# Fast file line count subroutine
# http://stackoverflow.com/questions/845058/how-to-get-line-count-cheaply-in-python
import subprocess
def file_len(fname):
	p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	result, err = p.communicate()
	if p.returncode != 0:
		raise IOError(err)
	return int(result.strip().split()[0])
	
n_lines = file_len(descriptions_path)
data_in = codecs.open(descriptions_path, 'r', 'iso-8859-1')

fields = collections.Counter()
repeat_fields = set()
record_fields_list = []

for ii, line in enumerate(data_in):
    if ii % 50000 == 0:
        print str(ii).rjust(len(str(n_lines))), '/', n_lines
    
    # continued lines
    if line.startswith(' '):
        continue

    star_field_name = line[:17].strip()
    value = line[17:].strip()

    # blank lines
    if not star_field_name:
        continue

    if star_field_name == '--RECORD NUMBER--':
        fields.update(record_fields_list)
        record_fields_set = set(record_fields_list)
        if len(record_fields_set) != len(record_fields_list):
            record_fields_count = collections.Counter(record_fields_list)
            for field_name,count in record_fields_count.iteritems():
                if count > 1:
                    repeat_fields.add(field_name)
        record_fields_list = []
    else:
        record_fields_list.append(star_field_name)

print 'All fields'
print fields.most_common(100)
print

for k in sorted(fields):
    print k
print

print 'Repeated fields'
for k in sorted(repeat_fields):
    print k



