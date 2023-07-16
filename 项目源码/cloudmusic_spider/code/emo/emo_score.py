import re
import os
from snownlp import *
from tqdm import tqdm
from pyspark import SparkConf, SparkContext
from pyhdfs import HdfsClient
# os.environ["PYSPARK_PYTHON"] = "/root/anaconda3/envs/pyspark_3.6/bin/python"


def create_dir(client, dir_path):
    if client.exists(dir_path):
        client.delete(dir_path, recursive=True)
    client.mkdirs(dir_path)


# 计算一首歌的歌词emo指数
def score_lyr(lyric):

    if lyric == 'null':     # 没有歌词
        
        return -1
    
    else:
        
        depart = lyric.split(' ')   # 获取歌词列表
        score_list = [1 - SnowNLP(item).sentiments for item in depart if item != '']
        if len(score_list) != 0:
            score = sum(score_list) / len(score_list)
            return score

        else:
            return -1


# 将id-emo元组写入文件中
def write_score(x):

    content = f'{x[0]} @#$#@ {x[1]}\n'
    with open('/home/cloudmusic_spider/data/store/lyric_emo.txt', 'a', encoding='utf-8') as file:
        file.write(content)


# 计算所有歌曲的歌词emo指数
def score_lyrics(sc):

    # 获取歌曲(id-歌词emo指数)
    lyric_emo_dict = sc.textFile('hdfs://stu:9000/basic_data/info/song_info.txt') \
                        .map(lambda line: line.split(' @#$#@ ')) \
                        .map(lambda list: (list[0], score_lyr(list[5]))) \
                        .filter(lambda x: x[1] != -1) \
                        .collect()

    list(map(write_score, lyric_emo_dict))    # 写入文件，暂存


# 计算单个评论文件的emo指数（并行）
def score_comment(iterable):
    
    tmp = iterable[0].split("/")
    dir, filename = tmp[-2], tmp[-1]

    comment_emo_list = []
    for line in iterable[1].split("\n"):
        list = line.split(' @#$#@ ')
        comment_emo_list.append((1 - SnowNLP(list[3]).sentiments, int(list[4])))

    sum_likecount = sum(item[1] for item in comment_emo_list) + len(comment_emo_list)   # 点赞总数，每条评论点赞数加1，方式出现0权重
    score = sum((item[1] + 1) / sum_likecount * item[0] for item in comment_emo_list)   # 一首歌的emo指数

    id = re.search(r'\d+', filename).group(0)   # 获取id

    content = f'{id} @#$#@ {score}\n'

    # 将emo指数存入指定位置
    local_path = '/home/cloudmusic_spider/data/store/song_comment_emo.txt' if dir == 'song_comments' else '/home/cloudmusic_spider/data/store/playlist_comment_emo.txt'
    
    with open(local_path, 'a', encoding='utf-8') as file:
        file.write(content)


# 计算所有评论文件的emo指数
def score_comments(sc):

    file_list1 = sc.wholeTextFiles('hdfs://stu:9000/basic_data/playlist_comments/')
    file_list2 = sc.wholeTextFiles('hdfs://stu:9000/basic_data/song_comments/')
    combined_list = file_list1.union(file_list2)
    combined_list.foreach(score_comment)


# 计算单个歌曲的emo指数
def score_song(list, lyric_emo_dict, songs_emo_dict):
    
    song_id = list[0]   # 歌曲id
    lyric_emo_score = lyric_emo_dict.get(song_id, 0)
    songs_emo_score = songs_emo_dict.get(song_id, 0)

    score = 0.3 * lyric_emo_score + 0.7 * songs_emo_score if lyric_emo_score and songs_emo_score else lyric_emo_score or songs_emo_score or -1

    # if len(list) == 8:      # 有emo指数
    #     list[7] = score
    # else:
    #     list.append(score)

    list.append(str(score))
    return list


# 计算所有歌曲的emo指数
def score_songs(sc, songs_emo_dict, lyric_emo_dict):
    
    print('开始计算所有歌曲emo指数')

    client = HdfsClient(hosts='stu:50070', user_name='root')
    rdd = sc.textFile('hdfs://stu:9000/basic_data/info/song_info.txt')
    
    # 对每首歌评emo指数、去除emo指数为-1
    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
                    .map(lambda list: score_song(list, lyric_emo_dict, songs_emo_dict)) \
                    .filter(lambda list: list[8] != '-1') \
                    .map(lambda list: ' @#$#@ '.join(list)) \
                    .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create('/emo_data/info/song_info.txt', data=str)

    print('所有歌曲emo指数计算完毕!\n')


# 计算单个用户的emo指数
def score_user(list, songs_emo_dict):

    signature = list[5]     # 用户个人简介
    signature_score = 1 - SnowNLP(signature).sentiments if signature != 'null' and signature != '' else -1
    
    all_rank = list[7].split(' ') if list[7] != 'null' else []    # 全部听歌排行
    all_list = [songs_emo_dict[song_id] for song_id in all_rank if song_id in songs_emo_dict]
    all_score = sum(all_list) / len(all_list) if len(all_list) != 0 else -1

    week_rank = list[8].split(' ') if list[8] != 'null' else []     # 近一周听歌排行
    week_list = [songs_emo_dict[song_id] for song_id in week_rank if song_id in songs_emo_dict]
    week_score = sum(week_list) / len(week_list) if len(week_rank) != 0 else -1
    
    if signature_score != -1 and week_score != -1:
        score = 0.2 * signature_score + 0.3 * all_score + 0.5 * week_score
    elif signature_score != -1:
        score = signature_score
    elif week_score != -1:
        score = 0.3 * all_score + 0.7 * week_score
    else:
        score = -1

    list.append(str(score))

    return list



# 计算用户的emo指数
def score_users(sc, songs_emo_dict):

    print('开始计算所有用户emo指数')

    rdd = sc.textFile('hdfs://stu:9000/basic_data/info/user_info.txt')

    # 计算每个用户的emo指数、去除emo指数为-1
    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
                    .map(lambda list: score_user(list, songs_emo_dict)) \
                    .filter(lambda list: list[11] != '-1') \
                    .map(lambda list: ' @#$#@ '.join(list)) \
                    .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create('/emo_data/info/user_info.txt', data=str)

    print('所有用户emo指数计算完毕!\n')


# 计算歌手的emo指数
def score_singer(list, songs_emo_dict):

    hotSongs = list[4].split(' ')   # 歌手的热门歌曲
    score_list = [songs_emo_dict[song_id] for song_id in hotSongs if song_id in songs_emo_dict]
    score = sum(score_list) / len(score_list) if len(score_list) != 0 else -1
    
    list.append(str(score))

    return list


# 计算所有歌手的emo指数
def score_singers(sc, songs_emo_dict):

    print('开始计算所有歌手emo指数')

    rdd = sc.textFile('hdfs://stu:9000/basic_data/info/singer_info.txt')

    # 计算每个歌手的emo指数、去除emo指数为-1
    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
                    .map(lambda list: score_singer(list, songs_emo_dict)) \
                    .filter(lambda list: list[5] != '-1') \
                    .map(lambda list: ' @#$#@ '.join(list)) \
                    .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create('/emo_data/info/singer_info.txt', data=str)
    
    print('所有歌手emo指数计算完毕!\n')


# 计算单个歌单的emo指数
def score_playlist(list, playlist_emo_dict, songs_emo_dict):

    hotSongs = list[8].split(' ')   # 歌单内的前十首歌曲id
    score_list = [songs_emo_dict[song_id] for song_id in hotSongs if song_id in songs_emo_dict]
    song_score = sum(score_list) / len(score_list) if len(score_list) != 0 else -1

    comment_score = playlist_emo_dict.get(list[0], -1)

    score = 0.7 * song_score + 0.3 * comment_score if song_score != -1 and comment_score != -1 else song_score or comment_score or -1

    list.append(str(score))

    return list


# 计算所有歌单的emo指数
def score_playlists(sc, playlist_emo_dict, songs_emo_dict):

    print('开始计算所有歌单emo指数')

    rdd = sc.textFile('hdfs://stu:9000/basic_data/info/playlist_info.txt')

    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
                    .map(lambda list: score_playlist(list, playlist_emo_dict, songs_emo_dict)) \
                    .filter(lambda list: list[10] != -1) \
                    .map(lambda list: ' @#$#@ '.join(list)) \
                    .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create('/emo_data/info/playlist_info.txt', data=str)
    
    print('所有歌单emo指数计算完毕!\n')


# 读取存放得分的文件
def read_score(sc, hdfs_path1, hdfs_path2, hdfs_path3):

    # 读取歌单id-评论emo指数
    list1 = sc.textFile('hdfs://stu:9000' + hdfs_path1) \
                .map(lambda line: line.split(' @#$#@ ')) \
                .map(lambda list: (list[0], float(list[1]))) \
                .collect()
    
    playlist_comment_emo = {key:value for key, value in list1}

    # 读取歌曲id-评论emo指数
    list2 = sc.textFile('hdfs://stu:9000' + hdfs_path2) \
                .map(lambda line: line.split(' @#$#@ ')) \
                .map(lambda list: (list[0], float(list[1]))) \
                .collect()

    song_comment_emo = {key:value for key, value in list2}

    # 读取歌曲id-歌词emo指数
    list3 = sc.textFile('hdfs://stu:9000' + hdfs_path3) \
                .map(lambda line: line.split(' @#$#@ ')) \
                .map(lambda list: (list[0], float(list[1]))) \
                .collect()
    
    lyric_emo = {key:value for key, value in list3}
    
    return playlist_comment_emo, song_comment_emo, lyric_emo





if __name__=="__main__":

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    sc = SparkContext(conf=conf)

    client = HdfsClient(hosts='stu:50070', user_name='root')

    create_dir(client, '/emo_data')     # 创建存放emo指数文件的目录
    create_dir(client, '/emo_data/store')     # 创建暂存评论和歌词emo指数的目录
    create_dir(client, '/emo_data/info')     # 创建存放emo指数结果的目录

    local_path1 = '/home/cloudmusic_spider/data/store/playlist_comment_emo.txt'
    local_path2 = '/home/cloudmusic_spider/data/store/song_comment_emo.txt'
    local_path3 = '/home/cloudmusic_spider/data/store/lyric_emo.txt'

    hdfs_path1 = '/emo_data/store/playlist_comment_emo.txt'
    hdfs_path2 = '/emo_data/store/song_comment_emo.txt'
    hdfs_path3 = '/emo_data/store/lyric_emo.txt'

    if not client.exists(hdfs_path1) and not client.exists(hdfs_path2):
        if os.path.exists(local_path1):
            os.remove(local_path1)

        if os.path.exists(local_path2):
            os.remove(local_path2)

        score_comments(sc)      # 歌单id-emo指数，歌曲id-emo指数
        client.copy_from_local(local_path1, hdfs_path1)
        client.copy_from_local(local_path2, hdfs_path2)


    if not client.exists(hdfs_path3):
        if os.path.exists(local_path3):
            os.remove(local_path3)

        score_lyrics(sc)    # 歌曲歌词-emo指数
        client.copy_from_local(local_path3, hdfs_path3)

    
    # 读取已计算好的emo指数
    playlist_emo_dict, songs_emo_dict, lyric_emo_dict = read_score(sc, hdfs_path1, hdfs_path2, hdfs_path3)

    # 计算所有歌曲的emo指数
    score_songs(sc, songs_emo_dict, lyric_emo_dict)

    # # 计算所有用户的emo指数
    score_users(sc, songs_emo_dict)

    # # 计算所有歌手的emo指数
    score_singers(sc, songs_emo_dict)

    # # 计算所有歌单的emo指数
    score_playlists(sc, playlist_emo_dict, songs_emo_dict)