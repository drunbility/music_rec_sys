#!/usr/bin/python

import os
import sys


def reducer_func():

    cur_token = None
    m_list = []

    for line in sys.stdin:
        ss = line.strip().split('\t')
        if len(ss) != 3:
            continue
        token = ss[0].strip()
        name = ss[1].strip()
        weight = float(ss[2].strip())

        if cur_token == None:
            cur_token = token
        if cur_token != token:

            final_list = sorted(m_list, key=lambda x: x[1], reverse=True)
            print '\t'.join([cur_token, ''.join([''.join([name_weight[0], str(name_weight[1])]) for name_weight in final_list])])

            cur_token = token
            m_list = []

        m_list.append((name, weight))

    final_list = sorted(m_list, key=lambda x: x[1], reverse=True)
    print '\t'.join([cur_token, ''.join([''.join([name_weight[0], str(name_weight[1])]) for name_weight in final_list])])



if __name__ == "__main__":
    module = sys.modules[__name__]
    func = getattr(module, sys.argv[1])
    args = None
    if len(sys.argv) > 1:
        args = sys.argv[2:]
    func(*args)

