# -*- coding: utf8-*-

import sys
sys.path.append('code')
sys.path.append("/root/anaconda3/lib/python3.6/site-packages")
import re
import pymysql
from collections import Counter
from pyspark import SparkConf, SparkContext
from pyhdfs import HdfsClient
# from tools.struct import Music_charts
# from database.utils import showTable, selectAll, deleteAll


# 提取评论中的地区信息和单词
def extract_words(words, region):
    region = region.strip()
    word_list = words.split(" ")
    return [(region, word) for word in word_list]

# 统计完每个地区的高频词汇后整理
def transform(region, list):
    words = ' @#$#@ '.join([word[0] for word in list])
    frequences = ' @#$#@ '.join([str(word[1]) for word in list])
    return (region, words, frequences)


# 统计每个地区评论中的词频
def province_word_count(sc, connection):
    
    print('地区评论词频统计开始')

    playlist_comments_rdd = sc.textFile("hdfs://fwt:9000/cut_data/playlist_comments/*.txt")
    song_comments_rdd = sc.textFile("hdfs://fwt:9000/cut_data/song_comments/*.txt")
    
    # 合并两个rdd
    rdd = playlist_comments_rdd.union(song_comments_rdd)

    # 过滤地区不存在、提取评论与地区、按地区统计词频
    result = rdd.map(lambda line: line.split(' @#$#@ ')) \
                .filter(lambda list: list[5] != 'null') \
                .flatMap(lambda list: extract_words(list[3], list[5])) \
                .map(lambda x: (x, 1)) \
                .reduceByKey(lambda a, b: a + b) \
                .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                .groupByKey() \
                .map(lambda x:(x[0], list(x[1]))) \
                .mapValues(lambda x: sorted(x, key=lambda y:-y[1])) \
                .map(lambda x: transform(x[0], x[1])) \
                .collect()
    
    # print(result)

    cursor = connection.cursor()
    
    # 14 活跃用户地区评论词云表userWord(省份名 cname, 评论词云词语 word, 出现次数 cnt)
    sql = "INSERT INTO userRegionWord (cname, word, cnt) VALUES (%s, %s, %s)"
    cursor.executemany(sql, result)
    connection.commit()

    cursor.close()

    print('地区评论词频统计结束')


# 统计每个歌单评论中的词频
def playlist_word_count(sc, client, connection, Music_charts):

    print('歌单评论词频统计开始')

    inverse_dict = {str(value): key for key, value in Music_charts.items()}

    # 歌单名列表
    playlist_files = client.listdir('/cut_data/playlist_comments/')

    # 歌单rdd列表
    playlist_rdds = [sc.textFile(f'hdfs://fwt:9000/cut_data/playlist_comments/{file}') for file in playlist_files]

    result = []
    for i in range(len(playlist_rdds)):

        word_list = playlist_rdds[i].map(lambda line: line.split(' @#$#@ ')) \
                                    .filter(lambda list: len(list) == 6) \
                                    .flatMap(lambda list: list[3].split(' ')) \
                                    .map(lambda x: (x, 1)) \
                                    .reduceByKey(lambda a, b: a + b) \
                                    .sortBy(lambda tuple: tuple[1], ascending=False) \
                                    .collect()
                    
        id = re.search(r'\d+', playlist_files[i]).group(0)
        words = ' @#$#@ '.join([word[0] for word in word_list])
        frequence = ' @#$#@ '.join([str(word[1]) for word in word_list])
        result.append((id, inverse_dict[id], words, frequence))
        
    cursor = connection.cursor()

    # 16 歌单评论词云表listCommentWord(歌单id lid, 歌单名 lname, 词云词语 word, 出现次数 cnt)
    sql = "INSERT INTO listCommentWord (lid, lname, word, cnt) VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql, result)
    connection.commit()

    cursor.close()

    print('歌单评论词频统计结束')


# song_word_count的辅助函数
def count_lyric(str_word):

    word_list = str_word.split(' ')     # 词汇列表
    word_count_dict = Counter(item for item in word_list if item != '')
    tmp_list = sorted(word_count_dict.items(), key=lambda x: -x[1])
    keys = ' @#$#@ '.join([item[0] for item in tmp_list])
    values = ' @#$#@ '.join([str(item[1]) for item in tmp_list])

    return (keys, values)


# 爬取每首歌歌词的词云表
def song_word_count(sc, connection):

    print('歌曲歌词词频统计开始')

    # 过滤列数不为8、歌词为空的歌曲
    result = sc.textFile('hdfs://fwt:9000/cut_data/info/song_info.txt') \
                .map(lambda line: line.split(' @#$#@ ')) \
                .map(lambda x: (x[0], x)) \
                .reduceByKey(lambda x, y: x) \
                .map(lambda x: x[1]) \
                .filter(lambda list: len(list) == 8) \
                .map(lambda list: [list[0], list[1], count_lyric(list[5])]) \
                .map(lambda x: (x[0], x[1], x[2][0], x[2][1])) \
                .collect()
    
    # print(result)
    cursor = connection.cursor()
    
    # showTable(connection, 'songWord')
    # deleteAll(connection, 'songWord')
    sql = f"delete from songWord"
    cursor.execute(sql)
    connection.commit()

    # 22 歌曲歌词词云表songWord(歌曲id sid, 歌曲名 sname, 歌词词云词语 word, 对应次数 cnt)
    sql = "INSERT INTO songWord (sid, sname, word, cnt) VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql, result)
    connection.commit()

    # selectAll(connection, 'songWord')

    cursor.close()

    print('歌曲歌词词频统计结束')
    

# 爬取每首歌的评论词云
def song_comment_wordcount(sc, connection):

    print('歌曲评论词频统计开始')

    tmp = sc.textFile('hdfs://fwt:9000/cut_data/info/song_info.txt') \
                .map(lambda line: line.split(' @#$#@ ')) \
                .filter(lambda list: len(list) == 8) \
                .map(lambda list: (list[0], list[1])) \
                .collect()
    
    song_id_name_dict = {key:value for key, value in tmp}
                

    # 歌曲名列表
    song_files = client.listdir('/cut_data/song_comments/')

    count = 0
    length = len(song_files)
    result = []
    for i in range(length):
        
        id = re.search(r'\d+', song_files[i]).group(0)
        if id not in song_id_name_dict:
            continue

        song_rdd = sc.textFile(f'hdfs://fwt:9000/cut_data/song_comments/{song_files[i]}')

        word_list = song_rdd.map(lambda line: line.split(' @#$#@ ')) \
                                .filter(lambda list: len(list) == 6) \
                                .flatMap(lambda list: list[3].split(' ')) \
                                .map(lambda x: (x, 1)) \
                                .reduceByKey(lambda a, b: a + b) \
                                .sortBy(lambda tuple: tuple[1], ascending=True) \
                                .collect()
                    
        words = ' @#$#@ '.join([word[0] for word in word_list])
        frequence = ' @#$#@ '.join([str(word[1]) for word in word_list])
        result.append((id, song_id_name_dict[id], words, frequence))
        
        count += 1
        print(f'进度:{count}/{length}')

    cursor = connection.cursor()

    # showTable(connection, 'songCommentWord')
    # deleteAll(connection, 'songCommentWord')
    
    # 23 歌曲评论词云表songCommentWord(歌曲id sid, 歌曲名 sname, 词云词语 word, 出现次数 cnt)
    sql = "INSERT INTO songCommentWord (sid, sname, cword, ccnt) VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql, result)
    connection.commit()

    # selectAll(connection, 'songCommentWord')

    cursor.close()

    print('歌曲评论词频统计结束')


# 爬取每个歌手创建歌曲时的高频词汇
def singer_word_count(sc, connection):
    
    print('歌手创作歌曲歌词词频统计开始')

    # 从歌手信息表中取出歌手id、歌手名、歌手创作歌曲id
    singer_list = sc.textFile('hdfs://fwt:9000/basic_data/info/singer_info.txt') \
            .map(lambda line: line.split(' @#$#@ ')) \
            .filter(lambda list: len(list) == 5) \
            .map(lambda list: [list[0], list[1], list[4].split(' ')]) \
            .collect()
    
    result = []
    rdd = sc.textFile('hdfs://fwt:9000/cut_data/info/song_info.txt') \
            .map(lambda line: line.split(' @#$#@ ')) \
            .filter(lambda list: len(list) == 8)
    
    # 遍历每个歌手
    for singer in singer_list:
        
        word_list = rdd.filter(lambda list: list[0] in singer[2]) \
                        .flatMap(lambda list: list[5].split(' ')) \
                        .map(lambda x: (x, 1)) \
                        .reduceByKey(lambda a, b: a + b) \
                        .sortBy(lambda tuple: tuple[1], ascending=False) \
                        .take(10)

        if word_list == [] or word_list[0][1] == 3:
            continue

        words = ' @#$#@ '.join([word[0] for word in word_list])
        frequence = ' @#$#@ '.join([str(word[1]) for word in word_list])

        result.append((singer[0], singer[1], words, frequence))
    
    cursor = connection.cursor()

    # showTable(connection, 'singerWord')
    # deleteAll(connection, 'singerWord')

    # 29 歌手歌曲歌词词云表singerWord(歌手id seid, 歌手名 sename, 所有歌词词云词语 word, 出现次数 cnt)
    sql = "INSERT INTO singerWord (seid, sename, word, cnt) VALUES (%s, %s, %s, %s)"
    cursor.executemany(sql, result)
    connection.commit()

    # showTable(connection, 'singerWord')

    cursor.close()

    print('歌手创作歌曲歌词词频统计结束')
          



if __name__ == '__main__':

    conf = SparkConf().setMaster("spark://fwt:7077").setAppName("job1")
    sc = SparkContext(conf=conf)

    client = HdfsClient(hosts='fwt:50070', user_name='root')

    connection = pymysql.connect(host='762j782l06.zicp.fun',
                                user='root',
                                password='12345678',
                                db='visualData',
                                port=50919,
                                charset='utf8')


    # 统计地区词云  
    # province_word_count(sc, connection)

    # 统计歌单词云
    # playlist_word_count(sc, client, connection, Music_charts)

    # 统计歌曲歌词词云
    # song_word_count(sc, connection)
    
    # 统计每首歌评论词云
    # song_comment_wordcount(sc, connection)
    
    # 统计歌手创作词云
    singer_word_count(sc, connection)

    connection.close()
    sc.stop()