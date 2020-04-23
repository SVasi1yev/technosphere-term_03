#!/usr/bin/env python
import sys
import re

for line in sys.stdin:
    for w in re.split(r'\s', line.strip()):
        if w:
            print "%s\t1" % w
