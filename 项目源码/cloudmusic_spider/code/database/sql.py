# import sys
# sys.path.append("/root/anaconda3/lib/python3.6/site-packages")
import pymysql
import re
from tqdm import tqdm
from pyspark import SparkConf, SparkContext
from pyhdfs import HdfsClient
from utils import showTable, selectAll, deleteAll
from collections import Counter



# 9(已存入) 活跃用户emo指数地区分布表userRegion(省份名 cname, 省份emo指数 cemo)
def userRegion(sc, filepath, connection):
    '''
        filepath: user_info.txt
    '''

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)     # 创建画笔
    rdd = sc.textFile(filepath)   # 读取文件，生成rdd
    
    # 取出用户信息文件中的地区和emo指数
    region_emo = rdd.map(lambda line: line.split(" @#$#@ ")) \
                    .map(lambda x: [x[4], float(x[11])]) \
                    .filter(lambda x: x[0] != 'null')
    
    # 为每个区域创建一个累加器，将该区域的所有情感指数累加到累加器中，并计算出该区域的情感指数总和和计数，求得该区域的平均emo指数
    region_avg_emo = region_emo.combineByKey(
                        lambda value: (value, 1),  # 初始值为(emo指数, 1)
                        lambda acc, value: (acc[0] + value, acc[1] + 1),  # 对于每个key的累加器，累加emo指数和计数
                        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 合并不同分区的累加器
                    ).mapValues(lambda acc: acc[0] / acc[1])  # 计算平均值
    
    result = region_avg_emo.collect()

    sql = """INSERT INTO userRegion VALUES(%s, %s)"""

    try:
        cursor.executemany(sql, result)
        connection.commit()   # 提交到数据库执行

    except Exception as e:
        print(e)
        connection.rollback()   # 如果发生错误则回滚

    cursor.close()


# 10(已存入) 活跃用户emo指数年龄分布表userAge(年龄段 age, 年龄段emo指数 aemo, 年龄段人数 enum)
def userAge(sc, filepath, connection):
    '''
      filepath: emo_data/user_info.txt
    '''

    
    age_dict = {0:'0-9', 1:'10-19', 2:'20-29', 3:'30-39', 4:'40-49', 5:'50-59', 6:'60-69', 7:'70-79', 8:'80-89', 9:'90-99'}

    # 从用户信息表中取出age和emo指数
    age_emo_list = sc.textFile(filepath) \
                      .map(lambda line: line.split(" @#$#@ ")) \
                      .filter(lambda list: list[3] != 'null') \
                      .map(lambda list: (int(list[3]) // 10, float(list[11]))) \
                      .filter(lambda x: 0 <= x[0] <= 9) \
                      .groupByKey() \
                      .map(lambda x: (x[0], list(x[1]))) \
                      .map(lambda x: (x[0], sum(x[1]) / len(x[1]), len(x[1]))) \
                      .sortBy(lambda x: x[0]) \
                      .map(lambda x: (age_dict[x[0]], x[1], x[2])) \
                      .collect()
    
    
    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    sql = "INSERT INTO userAge (age, aemo, anum) VALUES (%s, %s, %s)"

    try:
      cursor.executemany(sql, age_emo_list)
      connection.commit()
    except Exception as e:
      print(e)
      connection.rollback()
    
    cursor.close()


# 11(已存入) 活跃用户emo指数性别分布表userSex(性别 sex, 性别emo指数 semo)
def userSex(sc, filepath, connection):
    '''
        filepath: emo_data/user_info.txt
    '''

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    # 取出性别和emo指数
    region_emo = sc.textFile(filepath) \
                    .map(lambda line: line.split(" @#$#@ ")) \
                    .map(lambda x: (x[2], float(x[11]))) \
                    .filter(lambda x: x[0] != 'null')
    
    region_avg_emo = region_emo.combineByKey(
                        lambda value: (value, 1),  # 初始值为(emo指数, 1)
                        lambda acc, value: (acc[0] + value, acc[1] + 1),  # 对于每个key的累加器，累加emo指数和计数
                        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 合并不同分区的累加器
                    ).mapValues(lambda acc: acc[0] / acc[1])  # 计算平均值
    
    result = region_avg_emo.collect()

    sql = """INSERT INTO userSex (sex, semo) VALUES(%s, %s)"""

    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# 12(已存入) 活跃用户数据库统计表userNum(爬取用户总数 unum, 爬取歌单总数 lnum, 爬取歌曲总数 snum, 爬取歌手总数 senum, 爬取评论总数 pnum)
def userNum(sc, client, filepath_user, filepath_playlist, filepath_song, filepath_singer, connection):

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    rdd1 = sc.textFile(filepath_user)
    alluser = rdd1.count()

    rdd2 = sc.textFile(filepath_playlist)
    allplaylist = rdd2.count()

    rdd3 = sc.textFile(filepath_song)
    allsong = rdd3.count()

    rdd4 = sc.textFile(filepath_singer)
    allsinger = rdd4.count()
    
    total_list = sc.textFile(filepath_song) \
                    .map(lambda line: line.split(' @#$#@ ')) \
                    .filter(lambda list: list[6] != 'null') \
                    .map(lambda list: int(list[6])) \
                    .collect()
    
    total = sum(total_list)

    playlist_list = client.listdir('/basic_data/playlist_comments/')
    playlist_rdds = [sc.textFile('hdfs://stu:9000/basic_data/playlist_comments/' + playlist) for playlist in playlist_list]
    
    for rdd in playlist_rdds:
        total += rdd.count()
                    
    try:
        sql = "INSERT INTO userNum (unum, lnum, snum, senum, pnum) VALUES (%s, %s, %s, %s, %s)"
        result = (alluser, allplaylist, allsong, allsinger, total)
        cursor.execute(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# 13(已存入) 活跃用户emo指数前十省份表userTop10City(省份名 cname, 省份emo指数 cemo, 省份人数 cnum)
def userTop10City(sc, filepath, connection):
    '''
        filepath: emo_data/user_info.txt
    '''
    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    rdd = sc.textFile(filepath)

    # 取出用户的省份名、emo指数
    region_emo = rdd.map(lambda line: line.split(" @#$#@ ")) \
                    .map(lambda x: (x[4], float(x[11])))
    
    region_avg_count = region_emo.combineByKey(
                            lambda value: (value, 1),  # 初始值为(emo指数, 1)
                            lambda acc, value: (acc[0] + value, acc[1] + 1),  # 对于每个key的累加器，累加emo指数和计数
                            lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # 合并不同分区的累加器
                        ).mapValues(lambda acc: (acc[0] / acc[1], acc[1])) \
                         .map(lambda x: (x[0], x[1][0], x[1][1]))

    # 按照平均emo指数降序排序，并取前十个地区
    result = region_avg_count.takeOrdered(10, key=lambda x: -x[1])
    
    sql = 'INSERT INTO userTop10City (cname, cemo, cnum) VALUES(%s,%s,%s)'

    try:
        cursor.executemany(sql, result)     # 注意：插入数据库后记录是无序的
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# 15(已存入) 歌单信息收集表listAllNum(歌单id lid, 歌单名 lname, 收藏数 cnum, 评论数 pnum, 播放量 bnum, 收录音乐数 mnum)
def listAllNum(sc, filepath, connetion):
    '''
        filepath: basic_data/playlist_info.txt
    '''

    cursor = connetion.cursor(cursor = pymysql.cursors.DictCursor)

    rdd = sc.textFile(filepath)

    result = rdd.distinct()\
                .map(lambda line: line.split(" @#$#@ "))\
                .map(lambda list: (list[0], list[1], list[3], list[9], list[2], list[7]))\
                .collect()
    
    # print(result)

    try:
        sql = "INSERT INTO listAllNum (lid, lname, cnum, pnum, bnum, mnum) VALUES (%s, %s, %s, %s, %s, %s)"
        cursor.executemany(sql, result)
        connetion.commit()

    except Exception as e:
        print(e)
        connetion.rollback()

    cursor.close()


# listEmo辅助函数
def count_emo(str_id, song_emo_dict):     # 统计一个歌单处于各个emo区间的歌曲数量
    
    emo_count = [0] * 5
    songid_list = str_id.split(' ')   # 歌单包含歌曲id列表
    songEmo_list = [song_emo_dict.get(item, -1) for item in songid_list]  # 获得歌曲的emo指数，不存在置-1

    for item in songEmo_list:
        if item != -1:
            emo_count[int(float(item) // 0.2)] += 1

    result = ' @#$#@ '.join([str(value) for value in emo_count])

    return result


# 17(已存入) 歌单emo分布表listEmo(歌单id lid, 歌单名 lname, emo指数区间 emo, 出现次数 num)
def listEmo(sc, filepath1, filepath2, connection):
    '''
        filepath1: emo_data/song_info.txt       
        filepath2: emo_data/playlist_info.txt
    '''

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    rdd1 = sc.textFile(filepath1)

    # 读取歌曲id-emo指数
    song_emo_list = rdd1.map(lambda line: line.split(" @#$#@ "))\
                        .map(lambda elems: (elems[0], elems[8]))\
                        .collect()
    
    song_emo_dict = {key:value for key, value in song_emo_list}

    rdd2 = sc.textFile(filepath2)

    # 读取歌单id、歌单名、包含的歌曲id
    result = rdd2.map(lambda line: line.split(" @#$#@ "))\
                    .map(lambda list: (list[0], list[1], "积极 稍积极 中性 稍消极 消极", count_emo(list[8], song_emo_dict))) \
                    .collect()
    
    print(result)

    try:
        sql = "INSERT INTO listEmo (lid, lname, emo, num) VALUES (%s, %s, %s, %s)"
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# listStyle辅助函数
def count_style(str_id, song_style_dict):       # 统计歌单不同风格以及出现的次数

    songid_list = str_id.split(' ')    # 歌单中歌曲id列表
    style_dict = Counter(style for songid in songid_list for style in song_style_dict.get(songid, []))
    tmp_list = sorted(style_dict.items(), key=lambda x: -x[1])
    tmp_list = [item for item in tmp_list if item[0] != 'null']
    result1 = ' @#$#@ '.join([item[0] for item in tmp_list])
    result2 = ' @#$#@ '.join([str(item[1]) for item in tmp_list])
    
    return (result1, result2)


# 18(已存入) 歌单风格分布表listStyle(歌单id lid, 歌单名 lname, 歌曲风格 style, 出现次数 num)
def listStyle(sc, filepath1, filepath2, connection):
    '''
        filepath1: basic_data/song_info.txt     
        filepath2: basic_data/playlist_info.txt
    '''
    
    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    # 获取歌曲id-风格标签tags
    song_style_list = sc.textFile(filepath1) \
                        .map(lambda line: line.split(" @#$#@ "))\
                        .map(lambda elems: (elems[0], elems[7].split(' ')))\
                        .collect()
    
    song_style_dict = {key:value for key, value in song_style_list}
    # print(song_style_dict)

    result = sc.textFile(filepath2) \
                .map(lambda line: line.split(" @#$#@ "))\
                .map(lambda list: (list[0], list[1], count_style(list[8], song_style_dict))) \
                .map(lambda x: (x[0], x[1], x[2][0], x[2][1])) \
                .collect()

    try:
        sql = "INSERT INTO listStyle (lid, lname, style, num) VALUES (%s, %s, %s, %s)"
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# songSex辅助函数
def countSex(user_id_sex_dict, song_id_sex_dict, rdd, filename):
    
    songid = re.search(r'\d+', filename).group(0)   # 歌曲id
    
    sex_list = rdd.map(lambda line: line.split(" @#$#@ ")) \
                    .map(lambda list: user_id_sex_dict.get(list[0], -1)) \
                    .filter(lambda x: x != -1) \
                    .collect()
    
    if sex_list == []:
        return
    
    sex_count_dict = Counter(sex for sex in sex_list)
    result = (sex_count_dict['男'], sex_count_dict['女'])
    song_id_sex_dict[songid] = result


# 19(已存入) 歌曲用户性别分布表songSex(歌曲id sid, 歌曲名 sname, 性别 sex, 性别用户数 num)
def songSex(sc, client, dir, filepath1, filepath2, connection):
    '''
        dir: /basic_data/song_comments/
        filepath1: basic_data/song_info.txt
        filepath2: basic_data/user_info.txt
    '''

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    # 获取所有用户的id-性别sex
    user_id_sex_list = sc.textFile(filepath2) \
                            .map(lambda line: line.split(" @#$#@ ")) \
                            .filter(lambda x: x[2] != 'null') \
                            .map(lambda list: (list[0], list[2])) \
                            .collect()
    
    user_id_sex_dict = {key:value for key, value in user_id_sex_list}   # 转换为字典，user_id-sex
    
    print(user_id_sex_dict)

    song_id_sex_dict = {}   # 记录歌曲id-男女人数(a, b)

    song_files = client.listdir('/basic_data/song_comments/')    # 获取所有歌曲评论文件名
    # song_rdds = [sc.textFile(dir + file) for file in song_files]    # 获取所有评论文件的rdd

    # 处理每一个歌曲，获取其男女人数
    for filename in tqdm(song_files, total=len(song_files), desc='歌曲评论提取性别数进度'):
        rdd = sc.textFile(dir + filename)
        countSex(user_id_sex_dict, song_id_sex_dict, rdd, filename)
    
    result = sc.textFile(filepath1) \
                .map(lambda line: line.split(" @#$#@ ")) \
                .map(lambda list: (list[0], list[1], song_id_sex_dict.get(list[0], -1))) \
                .filter(lambda x: x[2] != -1) \
                .flatMap(lambda x: [(x[0], x[1], '男', x[2][0]), (x[0], x[1], '女', x[2][1])]) \
                .collect()
    
    print(result)

    sql = 'INSERT INTO songSex (sid, sname, sex, num) VALUES(%s, %s, %s, %s)'
    
    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# 20(已存入) 歌曲用户年龄分布表songAge(歌曲id sid, 歌曲名 sname, 年龄段 age 年龄段emo指数 emo, 年龄段用户数 num) 
def songAge(sc, client, dir, filepath1, filepath2, connection):
    '''
        dir: /basic_data/song_comments/
        filepath1: basic_data/song_info.txt
        filepath2: emo_data/user_info.txt
    '''

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    # 获取歌曲ID-歌曲名
    songid_name_list = sc.textFile(filepath1) \
                            .map(lambda line: line.split(" @#$#@ ")) \
                            .map(lambda x:(x[0], x[1])) \
                            .collect()
    
    
    result = []     # 承接最终结果
    user_info_rdd = sc.textFile(filepath2) \
                        .map(lambda line: line.split(' @#$#@ '))

    cnt = 0
    age_dict = {0:'0-9', 1:'10-19', 2:'20-29', 3:'30-39', 4:'40-49', 5:'50-59', 6:'60-69', 7:'70-79', 8:'80-89', 9:'90-99'}
    for item in songid_name_list:      # 遍历每一首歌
        
        comment_file = f'{dir}song_{item[0]}.txt'     # 这首歌曲的评论文件路径
        if client.exists(f'/basic_data/song_comments/song_{item[0]}.txt'):
            
            # 获取该歌曲评论区的所有用户id
            user_id_list = sc.textFile(comment_file) \
                            .map(lambda line: line.split(' @#$#@ ')) \
                            .map(lambda list: list[0]) \
                            .collect()
            
            user_id_list = set(user_id_list)
            
            userage_emo_list = user_info_rdd.filter(lambda list: list[0] in user_id_list) \
                                            .filter(lambda list: list[3] != 'null') \
                                            .map(lambda list: (int(list[3]) // 10, (float(list[11]), 1))) \
                                            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
                                            .map(lambda x: (age_dict[x[0]], x[1][0]/x[1][1], x[1][1])) \
                                            .sortBy(lambda x: x[0]) \
                                            .collect()
            
            for tmp in userage_emo_list:
                # print((item[0], item[1], tmp[0], tmp[1], tmp[2]))
                result.append((item[0], item[1], tmp[0], tmp[1], tmp[2]))
            
            cnt += 1
            print(f'进度:{cnt}/{len(songid_name_list)}')
    
    print(result)

    sql = 'INSERT INTO songAge (sid, sname, age, emo, num) VALUES(%s, %s, %s, %s, %s)'

    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()



# 21(已存入) 歌曲用户地区分布表songRegion(歌曲id sid, 歌曲名 sname, 地区名 cname, 对应用户数 cnum)
def songRegion(sc, dir, filepath, connection):   # 插入数据到表songRegion
    '''
        dir: basic_data/song_comments/
        filepath: song_info.txt
    '''
    
    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    # 歌曲id-歌曲名
    songid_name_list = sc.textFile(filepath) \
                            .distinct()\
                            .map(lambda line: line.split(" @#$#@ "))\
                            .map(lambda list: (list[0], list[1]))\
                            .collect()
    
    keys = {"北京", "天津", "河北", "山西", "内蒙古自治区", "辽宁", "吉林", "黑龙江", "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南", "广东", "广西", "海南", "重庆", "四川", "贵州", "云南", "西藏", "陕西", "甘肃", "青海", "宁夏", "新疆", "香港", "澳门", "台湾"}
    result = []

    cnt = 0
    length = len(songid_name_list)
    for i in range(length):      # 遍历每一首歌

        # 拿出所有用户评论的地区
        comment_file = f'{dir}song_{songid_name_list[i][0]}.txt'
        if client.exists(f'/basic_data/song_comments/song_{songid_name_list[i][0]}.txt'):
            
            location_list = sc.textFile(comment_file) \
                                .map(lambda line: line.split(" @#$#@ "))\
                                .map(lambda list: list[5])\
                                .filter(lambda x: x != 'null' and x in keys) \
                                .collect()
            
            tmp = songid_name_list[i]
            id, name = tmp[0], tmp[1]

            count_list = Counter(item for item in location_list)

            for item in count_list.items():
                result.append((id, name, item[0], item[1]))

        cnt += 1
        print(f'进度:{cnt}/{length}')
    

    sql = "INSERT INTO songRegion (sid, sname, cname, cnum) VALUES (%s, %s, %s, %s)"

    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# songInfo辅助函数
def get_recSongs(songid):

    client = HdfsClient(hosts='stu:50070', user_name='root')

    filepath = f'/rec_data/rec_{songid}.txt'

    if client.exists(filepath):     # 存在对应的推荐歌曲信息
        file = client.open(filepath)
        line_list = file.read().decode().split('\n')
        tmp_list = [line.split(' @#$#@ ') for line in line_list]
        tmp_list = [item[1] for item in tmp_list if len(item) >= 2]
        return ' @#$#@ '.join(tmp_list)
    else:
        return ''


# 24(未验证) 歌曲信息推荐表songInfo(歌曲id sid, 歌曲名 sname, 歌曲风格标签 style, 推荐歌曲 re)
def songInfo(sc, filepath1, filepath2):
    ''' 
        filepath1: basic_data/song_info.txt
        filepath2: basic_data/playlist_info.txt
    '''

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    # 获取所有排行榜上的歌曲
    playlist_songs_list = sc.textFile(filepath2) \
                            .map(lambda line: line.split(' @#$#@ ')) \
                            .flatMap(lambda list: list[8].split(' ')) \
                            .collect()
    
    playlist_songs_list = set(playlist_songs_list)
    
    # print(len(playlist_songs_list))

    result = sc.textFile(filepath1) \
                .map(lambda line: line.split(' @#$#@ ')) \
                .filter(lambda list: list[0] in playlist_songs_list) \
                .map(lambda list: (list[0], list[1], ' @#$#@ '.join(list[7].split(' ')) if list[7] != 'null' else '', get_recSongs(list[0]))) \
                .collect()

    # print(result)

    sql = "INSERT INTO songInfo (sid, sname, style, re) VALUES (%s, %s, %s, %s)"

    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# singerAllNum辅助函数
def countAllNum(str_hotSongs, songid_total_dict):

    hotSongs_list = str_hotSongs.split(' ')     # 该歌手热门歌曲id列表
    total_list = [int(songid_total_dict.get(song, -1)) for song in hotSongs_list]
    total_num = sum(item for item in total_list if item != -1)
    
    return (str(total_num), str(len(hotSongs_list)))    # 返回总评论量和热门歌曲数


# 25(已存入) 歌手信息收集表singerAllNum(歌手id seid, 歌手名 sename, 歌手关注数 cnum, 歌曲总评论量 pnum, 创作作品数mnum)
def singerAllNum(sc, filepath1, filepath2, connection):
    '''
        filepath1: basic_data/singer_info.txt;  
        filepath2: basic_data/song_info.txt
    '''
    
    # 获取歌曲id-评论总数
    songid_total_list = sc.textFile(filepath2) \
                            .map(lambda line: line.split(" @#$#@ "))\
                            .filter(lambda list: list[6] != 'null') \
                            .map(lambda list: (list[0], list[6])) \
                            .collect()

    # 转换为字典
    songid_total_dict = {key:value for key,value in songid_total_list}

    result = sc.textFile(filepath1) \
                            .map(lambda line: line.split(" @#$#@ "))\
                            .filter(lambda list: list[4] != 'null') \
                            .map(lambda list: [list[0], list[1], list[3], countAllNum(list[4], songid_total_dict)])\
                            .map(lambda x: (x[0], x[1], x[2] if x[2] != 'null' else 0, x[3][0], x[3][1])) \
                            .collect()

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    sql = "INSERT INTO singerAllNum (seid, sename, cnum, pnum, mnum) VALUES (%s, %s, %s, %s, %s)"

    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# singerEmo辅助函数
def countEmo(str_hotSongs, songid_emo_dict):

    count = [0] * 5
    hotSongs_list = str_hotSongs.split(' ')     # 该歌手热门歌曲id列表
    hotSongs_emo_list = [float(songid_emo_dict.get(item, -1)) for item in hotSongs_list]
    for item in hotSongs_emo_list:
        if item != -1:
            count[int(item // 0.2)] += 1

    result = ' @#$#@ '.join([str(i) for i in count])

    return result


# 26(已存入) 歌手歌曲emo分布表singerEmo(歌手id seid, 歌手名 sename, emo指数区间 emo, 出现次数 num)
def singerEmo(filepath1, filepath2, connection):
    '''
        filepath: basic_data/singer_info.txt        
        filepath1: emo_data/song_info.txt
    '''

    # 获取所有歌曲的id-emo指数
    songid_emo_list = sc.textFile(filepath2) \
                        .map(lambda line: line.split(" @#$#@ "))\
                        .map(lambda list: (list[0], list[8])) \
                        .collect()
    
    # 转换为字典
    songid_emo_dict = {key:value for key,value in songid_emo_list}

    emotype = "积极 @#$#@ 稍积极 @#$#@ 中性 @#$#@ 稍消极 @#$#@ 消极"   #emo指数区间
    
    # 获取歌手id、歌手名、歌手热门歌曲
    result = sc.textFile(filepath1) \
                        .map(lambda line: line.split(" @#$#@ "))\
                        .map(lambda list: (list[0], list[1], emotype, countEmo(list[4], songid_emo_dict)))\
                        .collect()
    
    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)
        
    sql = "INSERT INTO singerEmo (seid, sename, emo, num) VALUES (%s, %s, %s, %s)"

    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()



# 临时表的辅助函数
def get_list_songs(str_songs, songid_name_dict):

    Songs_list = str_songs.split(' ')     # 热门歌曲id列表
    Name = [songid_name_dict[songid] for songid in Songs_list if songid in songid_name_dict]
    
    return ' @#$#@ '.join(Name)


# 临时加入(已存入) 歌单爬取歌曲表listSong(歌单ID lid, 歌单名 lname, 歌名 song)
def listSong(sc, filepath1, filepath2, connection):
    '''
        filepath1: bsic_data/playlist_info.txt
        filepath2: basic_data/song_info.txt
    '''
    
    # 获取歌曲ID以及歌名
    songid_name_list = sc.textFile(filepath2) \
                            .map(lambda line: line.split(" @#$#@ ")) \
                            .map(lambda list: (list[0], list[1])) \
                            .collect()
    
    # 转换为字典
    songid_name_dict = {key:value for key,value in songid_name_list}

    result = sc.textFile(filepath1) \
                .map(lambda line: line.split(" @#$#@ "))\
                .map(lambda list: (list[0], list[1], get_list_songs(list[8], songid_name_dict)))\
                .collect()

    # print(result)

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    sql = 'INSERT INTO listSong (lid, lname, song) VALUES(%s, %s, %s)'

    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# singerSong辅助函数
def get_songname(str_hotSongs, songid_name_dict):

    hotSongs_list = str_hotSongs.split(' ')     # 热门歌曲id列表
    hotName = [songid_name_dict[songid] for songid in hotSongs_list if songid in songid_name_dict]
    
    return ' @#$#@ '.join(hotName)


# 27(已存入) 歌手热门歌曲表singerSong(歌手id seid, 歌手名 sename, 热门歌曲 songs)
def singerSong(sc, filepath1, filepath2, connection):
    '''
        filepath1: bsic_data/song_info.txt
        filepath2: basic_data/singer_info.txt
    '''
    
    # 获取ID以及歌名
    songid_name_list = sc.textFile(filepath1) \
                            .map(lambda line: line.split(" @#$#@ ")) \
                            .map(lambda list: (list[0], list[1])) \
                            .collect()
    
    # 转换为列表
    songid_name_dict = {key:value for key,value in songid_name_list}

    result = sc.textFile(filepath2) \
                .map(lambda line: line.split(" @#$#@ "))\
                .map(lambda list: (list[0], list[1], get_songname(list[4], songid_name_dict)))\
                .collect()

    # print(result)

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    sql = 'INSERT INTO singerSong (seid, sename, songs) VALUES(%s, %s, %s)'

    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


# singerStyle辅助函数
def countStyle(str_hotSongs, songid_style_dict):
    
    hotSongs_list = str_hotSongs.split(' ')     # 热门歌曲id列表
    style_list = [songid_style_dict[songid].split(' ') for songid in hotSongs_list if songid in songid_style_dict]
    style_dict = Counter(i for item in style_list for i in item)
    sort_dict = sorted(style_dict.items(), key=lambda x: -x[1])
    keys = ' @#$#@ '.join([str(item[0]) for item in sort_dict])
    values = ' @#$#@ '.join([str(item[1]) for item in sort_dict])
    return (keys, values)


# 28(已存入) 歌手歌曲风格分布表singerStyle(歌手id seid，歌手名 sename，歌曲风格 style，出现次数 num)
def singerStyle(sc, filepath1, filepath2, connection):
    '''
        filepath1: basic_data/singer_info.txt
        filepath2: basic_data/song_info.txt
    '''
    
    # 获取ID以及歌曲风格
    songid_style_list = sc.textFile(filepath2) \
                            .map(lambda line: line.split(' @#$#@ ')) \
                            .filter(lambda list: list[7] != 'null') \
                            .map(lambda list: (list[0], list[7])) \
                            .collect()
    
    # 转换为字典
    songid_style_dict = {key:value for key,value in songid_style_list}
    
    #歌手ID，歌手名，歌曲ID，相同歌手内容不一样的去重还没完成
    result = sc.textFile(filepath1) \
                .map(lambda line: line.split(" @#$#@ "))\
                .filter(lambda list: list[4] != 'null') \
                .map(lambda list: [list[0], list[1], countStyle(list[4], songid_style_dict)])\
                .map(lambda x: (x[0], x[1], x[2][0], x[2][1]))\
                .collect()

    # print(result)

    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    sql = 'INSERT INTO singerStyle (seid, sename, style, num) VALUES(%s, %s, %s, %s)'

    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()


#  singerTop10Song辅助函数
def countTotal(str_hotSongs, songid_name_total_dict):

    hotSongs_list = str_hotSongs.split(' ')     # 热门歌曲id列表
    songname_total_list = [songid_name_total_dict[songid] for songid in hotSongs_list if songid in songid_name_total_dict]
    sort_list = sorted(songname_total_list, key=lambda x: -int(x[1]))[:3]
    keys = ' @#$#@ '.join([str(item[0]) for item in sort_list])
    values = ' @#$#@ '.join([str(item[1]) for item in sort_list])

    return (keys, values)


# 29(已存入) 歌手综合评价前十榜(歌手id seid，歌手名 sename，前十歌曲 hotsong，评价指标数值 num)
def singerTop10Song(sc, filepath1, filepath2, connection):
    '''
        filepath1: basic_data/song_info.txt
        filepath2: basic_data/singer_info.txt
    '''
    
    cursor = connection.cursor(cursor = pymysql.cursors.DictCursor)

    # 获取ID、歌曲名以及歌曲评论数
    songid_name_total_list = sc.textFile(filepath1) \
                                .map(lambda line: line.split(' @#$#@ ')) \
                                .filter(lambda x: x[6] != 'null') \
                                .map(lambda list: (list[0], (list[1], list[6])))\
                                .collect()

    songid_name_total_dict = {key:value for key, value in songid_name_total_list}
    
    result = sc.textFile(filepath2) \
                .map(lambda line: line.split(" @#$#@ "))\
                .map(lambda list: (list[0], list)) \
                .reduceByKey(lambda x,y: x) \
                .map(lambda x: x[1]) \
                .map(lambda list: [list[0], list[1], countTotal(list[4], songid_name_total_dict)])\
                .map(lambda f: (f[0], f[1], f[2][0], f[2][1]))\
                .collect()

    # print(result)

    sql = 'INSERT INTO singerTop10Song VALUES(%s, %s, %s, %s)'

    try:
        cursor.executemany(sql, result)
        connection.commit()

    except Exception as e:
        print(e)
        connection.rollback()

    cursor.close()



if __name__=='__main__':

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    sc = SparkContext(conf=conf)

    client = HdfsClient(hosts='stu:50070', user_name='root')

    connection = pymysql.connect(host='762j782l06.zicp.fun',
                                user='root',
                                password='12345678',
                                db='visualData',
                                port=50919,
                                charset='utf8')
    

    table_name = 'songInfo'
    
    showTable(connection, table_name)
    deleteAll(connection, table_name)

    song_comments_dir = 'hdfs://stu:9000/basic_data/song_comments/'

    filepath_user = 'hdfs://stu:9000/basic_data/info/user_info.txt'
    filepath_playlist = 'hdfs://stu:9000/basic_data/info/playlist_info.txt'
    filepath_song = 'hdfs://stu:9000/basic_data/info/song_info.txt'
    filepath_singer = 'hdfs://stu:9000/basic_data/info/singer_info.txt'
    
    filepath_emo_user = 'hdfs://stu:9000/emo_data/info/user_info.txt'
    filepath_emo_song = 'hdfs://stu:9000/emo_data/info/song_info.txt'
    filepath_emo_playlist = 'hdfs://stu:9000/emo_data/info/playlist_info.txt'

    # userRegion(sc, filepath_emo_user, connection)
    # userAge(sc, filepath_emo_user, connection)
    # userSex(sc, filepath_emo_user, connection)
    # userNum(sc, client, filepath_user, filepath_playlist, filepath_song, filepath_singer, connection)
    # userTop10City(sc, filepath_emo_user, connection)
    # listAllNum(sc, filepath_playlist, connection)
    # listEmo(sc, filepath_emo_song, filepath_emo_playlist, connection)
    # listStyle(sc, filepath_song, filepath_playlist, connection)
    # songSex(sc, client, song_comments_dir, filepath_song, filepath_user, connection)
    # songAge(sc, client, song_comments_dir, filepath_song, filepath_emo_user, connection)
    # songRegion(sc, song_comments_dir, filepath_song, connection)
    songInfo(sc, filepath_song, filepath_playlist)
    # singerAllNum(sc, filepath_singer, filepath_song, connection)
    # singerEmo(filepath_singer, filepath_emo_song, connection)
    # singerSong(sc, filepath_song, filepath_singer, connection)
    # singerStyle(sc, filepath_singer, filepath_song, connection)
    # singerTop10Song(sc, filepath_song, filepath_singer, connection)
    # listSong(sc, filepath_playlist, filepath_song, connection)
    
    selectAll(connection, table_name)

    connection.close()
    sc.stop()