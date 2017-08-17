#!/usr/bin/python

import os
import sys

def mapper_func():
    for line in sys.stdin:
        ss = line.strip().split('\t')
        if len(ss) != 3:
            continue
        music_id = ss[0].strip()
        music_name = ss[1].strip()
        music_fealist = ss[2].strip()

        for fea in music_fealist.split(''):
            token, weight = fea.strip().split('')
            print '\t'.join([token, music_name, weight])



if __name__ == "__main__":
    module = sys.modules[__name__]
    func = getattr(module, sys.argv[1])
    args = None
    if len(sys.argv) > 1:
        args = sys.argv[2:]
    func(*args)

