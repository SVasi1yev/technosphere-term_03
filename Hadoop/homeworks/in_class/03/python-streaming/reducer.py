#!/usr/bin/env python
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)

    if current_word and word == current_word:
        current_count += int(count)
    else:
        if current_word:
            print '%s\t%d' % (current_word, current_count)
        current_word = word
        current_count = int(count)

if current_word:
    print '%s\t%d' % (current_word, current_count)
