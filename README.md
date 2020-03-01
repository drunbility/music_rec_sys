
### 基于内容的简单的音乐推荐系统
- music_index  处理曲库数据生成倒排索引表，即music_web中的inverted.data
- music_web 提供一个简单的web服务，即rec_cb_service.py
- 运行这个服务，访问这个服务，如：localhost:8080/?content=刘德华冷冷的冰雨 获取歌曲推荐列表和权重

### 基于协同过滤策略生成推荐列表

- music_rec_cf 

  music_index 中通过分词将歌曲切分关键词及权重，若将关键词和对应权重看成用户和相应评分，则可以使用协同过滤算法生成歌曲相似度索引

