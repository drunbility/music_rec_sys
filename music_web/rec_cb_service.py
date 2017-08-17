import web
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

sys.path.append("./jieba/")

import jieba
import jieba.posseg
import jieba.analyse

urls = (
    '/', 'index',
    '/test', 'test',
)

app = web.application(urls, globals())


rec_map = {}
with open('inverted.data', 'r') as fd:
    for line in fd:
	ss = line.strip().split('\t')
	if len(ss) != 2:
	    continue
	token = ss[0].strip().encode('utf8')
	music_rec_list_str = ss[1].strip()

	for music_score in music_rec_list_str.split(''):
	    name, score = music_score.strip().split('')
	    if token not in rec_map.keys():
		rec_map[token] = []
	    rec_map[token].append((name, round(float(score), 2)))



class index:
    def GET(self):
	params = web.input()
	content = params.get('content', '')
	print 'content: ', content


	seg_list = jieba.cut(content, cut_all=False)

	result_map = {}
	for seg in seg_list:
	    print 'seg: ', seg
	    if seg in rec_map.keys():
		for name_score in rec_map[seg.encode('utf8')]:	
		    tmp_name, score = name_score
		    name = tmp_name.encode('utf8')	
		    if name not in result_map:
			result_map[name] = score
		    else:
		        old_score = result_map[name]
			new_score = old_score + score
			result_map[name] = new_score

	rec_list = []
	for k, v in result_map.items():
	    rec_list.append('\t'.join([k, str(v)]))

	return "\r\n".join(rec_list)

class test:
    def GET(self):
	print web.input()
	return '222'

if __name__ == "__main__":
    app.run()
