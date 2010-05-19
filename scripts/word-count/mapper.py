# word-count/mapper.py
#
# The mapper for a Python streaming word-count job.

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        words = line.strip().split()
        for word in words:
            print word + "\t1"        
        
