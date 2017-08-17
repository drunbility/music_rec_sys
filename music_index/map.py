#!/usr/bin/python

import os
import sys

os.system('tar xvzf jieba.tar.gz > /dev/null')

reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.append("./")

import jieba
import jieba.posseg
import jieba.analyse

def mapper_func():
    for line in sys.stdin:
        ss = line.strip().split('\t')
        if len(ss) != 2:
            continue
        music_id = ss[0].strip()
        music_name = ss[1].strip()

        tmp_list = []
        for x, w in jieba.analyse.extract_tags(music_name, withWeight=True):
            tmp_list.append((x, float(w)))

        final_token_score_list = sorted(tmp_list, key=lambda x: x[1], reverse=True)

        print '\t'.join([music_id, music_name, ''.join([''.join([t_w[0], str(t_w[1])]) for t_w in final_token_score_list])])



if __name__ == "__main__":
    module = sys.modules[__name__]
    func = getattr(module, sys.argv[1])
    args = None
    if len(sys.argv) > 1:
        args = sys.argv[2:]
    func(*args)

