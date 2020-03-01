
lines=[]

with open('inverted_small.data','r',encoding='utf8') as f:
    for line in f:
        ss = line.strip().split('\t')
        if len(ss)!=2:
            continue
        token=ss[0].strip()
        music_rec_list=ss[1].strip()
        for music_score in music_rec_list.split(''):
            music,score=music_score.strip().split('')
            score=score[:8]
            lines.append('\001'.join([token,music.replace("？","").strip(),score+'\n']))
            
            
with open('tmusic.data','w',encoding='utf8') as wf:
    wf.writelines(lines)