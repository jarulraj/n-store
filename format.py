# FORMATTER

import pprint

file = open("data.log", "r")          

def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

tput = {}
latency = 0
rw_mix = 0.0
skew = 0.0

for line in file:
    if "LATENCY" in line:
        entry = line.strip().split(' ');
        if entry[0] == "LATENCY":
            latency = entry[1]
            
    if "RW MIX" in line:
        entry = line.strip().split(' ');
        rw_mix = entry[3]
        skew = entry[7]
   
    if "Throughput" in line:
        entry = line.strip().split(':');
        arch = entry[0].split(' ');
        
        tput[(rw_mix, skew, latency, arch[0])] = entry[4] 

read_only = []
read_heavy = []
write_heavy = []
for key in sorted(tput.keys()):
    if key[0] == '0':
        read_only.append(tput[key])
    elif key[0] == '0.1':
        read_heavy.append(tput[key])
    elif key[0] == '0.5':
        write_heavy.append(tput[key])

ro_chunks = list(chunks(read_only, 6))
print('\n'.join('\t'.join(map(str, row)) for row in zip(*ro_chunks)))
print '\n'

rh_chunks = list(chunks(read_heavy, 6))
print('\n'.join('\t'.join(map(str, row)) for row in zip(*rh_chunks)))
print '\n'

wh_chunks = list(chunks(write_heavy, 6))
print('\n'.join('\t'.join(map(str, row)) for row in zip(*wh_chunks)))
print '\n'

file.close()  
