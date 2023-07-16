import os
import shutil
import sys

from pyhdfs import HdfsClient
sys.path.append("code")
from tools.struct import Music_charts
from pyspark import SparkConf, SparkContext

def get_song(sc, song_id):  #song_id: str
    # 返回值为包括[歌曲id,歌曲名称,emo指数,歌曲标签集合]的嵌套列表
    # TODO
    # return ["111", "t", 0.9, {"华语", "空灵", "平静", "伤感"} ]

    # filepath="hdfs://cons:9000/data/info/song_info.txt"   #带emo指数的song_info.txt文件路径
    # print(f"\n进入get_song {song_id}...")
    
    filepath = "hdfs://stu:9000/emo_data/info/song_info.txt"
    
    song_info = sc.textFile(filepath) \
                        .map(lambda line: line.split(" @#$#@ "))\
                        .filter(lambda line: line[0] == song_id)\
                        .map(lambda list: [list[0], list[1], list[8], list[7]])\
                        .collect()
    if len(song_info)==0 :
        return []
    
    song_info = song_info[0] 
    if song_info[3] == 'null':
        # print(f"\n退出get_song {song_id}...")
        print(f"return song {[song_info[0], song_info[1], float(song_info[2]), set()]}")
        return [song_info[0], song_info[1], float(song_info[2]), set()]
    else:
        # print(f"\n退出get_song {song_id}...")
        print(f"return song{[song_info[0], song_info[1], float(song_info[2]), set(song_info[3].split(' '))]}")
        return [song_info[0], song_info[1], float(song_info[2]), set(song_info[3].split(' '))]


def get_songs_by_emo(sc, target_emo):   #target_emo: float
    # 从歌曲库中查找所有emo指数与target_emo相近的歌曲[前100首？30]
    # TODO
    # print(f"\n进入get_songs_by_emo {target_emo}...")
    filepaths = "hdfs://stu:9000/emo_data/info/song_info.txt"

    # 获取ID、歌曲名、emo以及歌曲标签
    result = sc.textFile(filepaths) \
                .map(lambda line: line.split(" @#$#@ ")) \
                .map(lambda line: [line[0], line[1], line[8], line[7], abs(float(line[8]) - float(target_emo))])\
                .sortBy(lambda x: x[4], ascending=True)\
                .map(lambda list: [list[0], list[1], list[2], set(list[3].split(' ')) if list[3] != 'null' else set()]) \
                .take(30)
    # print(f"\n退出get_songs_by_emo {target_emo}...")
    print(f"return get_songs_by_emo {result}")
    return result


def filter_with_tag(sc, target_song):   #target_song样例: ["111", "t", 0.9, {"华语", "空灵", "平静", "伤感"} ]
    # 根据自身的标签对1中的结果进行二次筛选，保留共同标签较多的歌曲
    
    # print(f"\n进入filter_with_tag {target_song}...")
    # 获取一次筛选的歌曲列表
    song_list = get_songs_by_emo(sc, target_song[2])
    
    # 目标歌曲的tag集合
    target_set = target_song[3]
    
    # 歌曲得分字典
    scores = {}
    
    # 打分
    for song in song_list:
        tag_set = song[3]
        # 根据交集打分
        score = len(target_set & tag_set)
        scores[song[0]] = score
    
    # 根据得分排序
    song_list.sort(key=lambda song : scores[song[0]],reverse = True)   
         
    # print(song_list)
    # print(scores)
    
    # print(f"\n退出filter_with_tag {target_song}...")
    print(f"return filter_with_tag {song_list[:30]}")
    return song_list[:10]


def get_user_rank(sc, user_id):
    # 获取用户最近听歌排行列表[ 周榜5首 + 总榜5首 ]共10首
    # TODO
    # return [ ["123", "a", 0.7, {"华语", "伤感"} ] * 10 ]
    # filepath="hdfs://cons:9000/data/info/user_info.txt"

    # print(f"\n进入get_user_rank{user_id}...")

    filepath1 = "hdfs://stu:9000/basic_data/info/user_info.txt"
    
    # 取出该用户的id、名字、全部听歌排行(前五)、近一周听歌排行(前五)
    rank_list = sc.textFile(filepath1) \
                        .map(lambda line: line.split(" @#$#@ ")) \
                        .filter(lambda list: list[0] == user_id) \
                        .map(lambda list: [list[7].split(" "), list[8].split(" ")]) \
                        .collect()
    
    if rank_list == []:
        return []
    else:
        rank_list = rank_list[0]
    
    filepath2 = "hdfs://stu:9000/emo_data/info/song_info.txt"

    result = sc.textFile(filepath2) \
                .map(lambda line: line.split(" @#$#@ ")) \
                .filter(lambda list: list[0] in rank_list[0] or list[0] in rank_list[1]) \
                .map(lambda list: [list[0], list[1], float(list[8]), set(list[7].split(" "))]) \
                .collect()

    # print(f"\n退出get_user_rank {user_id}...")
    print(f"return get_user_rank {result}")
    return result
    
    
def get_user_hobby(sc, user_id):
    # 用户的喜好来自于他听歌排行的普适标签
    
    # print(f"\n进入get_user_hobby {user_id}...")
    # 获取用户听歌排排行列表
    user_rank = get_user_rank(sc, user_id)
    
    # 无听歌信息
    if len(user_rank) == 0:
        return []   # 返回空hobby
    
    # 用户听歌喜好字典
    hobby_dic = {}
    for song in user_rank:
        tag_set = song[3]
        for tag in tag_set:
            if tag in hobby_dic:
                hobby_dic[tag] += 1
            else:
                hobby_dic[tag] = 1
    
    # 转为列表进行排序
    hobby_list = list(hobby_dic.items())
    hobby_list.sort(key = lambda tag: tag[1], reverse=True)
    
    # 生成用户喜好列表
    res_hobby = [hobby_list[i][0] for i in range(min(5,len(hobby_list)))]
    
    # print(res_hobby)
    # print(hobby_list)
    # print(hobby_dic)
    
    # print(f"\n退出get_user_hobby {user_id}...")
    if len(res_hobby) != 0:
        print(f"return get_user_hobby {res_hobby}")
    
    return res_hobby
    

def get_user_detail(sc, user_id):
    # 获取用户基本信息[性别,年龄(int),地区,emo指数(float)]
    # TODO
    # return ["男", 18, "重庆市", 0.8]

    # print(f"\n进入get_user_detail{user_id}...")
    
    filepath = "hdfs://stu:9000/emo_data/info/user_info.txt"

    # 获取性别、年龄、地区、emo
    result = sc.textFile(filepath) \
                .map(lambda line: line.split(" @#$#@ ")) \
                .filter(lambda list: list[0] == user_id)\
                .map(lambda list: [list[2], int(list[3]) if list[3] != 'null' else 100, list[4], float(list[11])])\
                .collect()
    
    if result == []:
        # print(f"\n退出get_user_detail{user_id}...")
        # print("return []")
        return []
    else:
        # print(f"\n退出get_user_detail{user_id}...")
        print(f"return get_user_detail {result[0]}")
        return result[0]
    


def get_user_profile(sc, user_id):
    # 分析单个用户的用户画像
    # print(f"\n进入get_user_profile {user_id}...")
    
    hobby = get_user_hobby(sc, user_id)
    detail = get_user_detail(sc, user_id)
    
    # 有用户信息时,进行泛化
    if len(detail) != 0:
        # 泛化用户基本信息  [年龄]
        if detail[1] < 18:
            detail[1] = "未成年"
        elif detail[1] < 30:
            detail[1] = "青年"
        elif detail[1] < 50:
            detail[1] = "中年"
        else:
            detail[1] = "其他"
        # 泛化用户基本信息  [emo指数]
        if detail[3] >= 0.5:
            detail[3] = "重度emo"
        else :
            detail[3] = "轻度emo"
    
    # 根据喜好和泛化基本信息生成用户画像
    user_profile = hobby + detail
    
    # print(user_profile)
    
    # print(f"\n退出get_user_profile {user_id}...")
    if len(user_profile) != 0:
        print(f"return get_user_profile {user_profile}")
    
    return user_profile 


def get_related_users(sc, song_id):
    # 在该歌曲下评论的用户群体
    # TODO
    # return ["111", "123", "321"]
    
    # print(f"\n进入get_related_users{song_id}...")

    filepath = f"hdfs://stu:9000/basic_data/song_comments/song_{song_id}.txt"
    
    # 防止不存在该歌曲的评论文件
    client = HdfsClient(hosts='stu:50070', user_name='root')
    if not client.exists(f"/basic_data/song_comments/song_{song_id}.txt"):
        return []

    # 取出歌曲评论文件里面的所有用户id,防止用户名重复
    result = sc.textFile(filepath) \
                .map(lambda line: line.split(" @#$#@ "))\
                .map(lambda list: (list[0], list)) \
                .reduceByKey(lambda x,y: x) \
                .map(lambda x: x[1]) \
                .map(lambda list: list[0])\
                .collect()[:10]
    
    # print(f"\n退出get_related_users{song_id}...")
    print(f"return get_related_users {result}")
    
    return result


def get_song_profile(sc, song_id, isDic = True):
    # 对各听众用户画像进行汇总[字典统计]，找出较为普适的用户画像作为本歌的用户画像
    
    # print(f"\n进入get_song_profile {song_id}, isDic = {isDic}...")
    
    # 获取相关用户群体
    users = get_related_users(sc, song_id)
    # 用户群体画像字典
    users_profile_dic = {}
    for user_id in users:
        profile = get_user_profile(sc, user_id)
        for tag in profile:
            if tag == "null" or tag == " " :
                continue
            elif tag in users_profile_dic:
                users_profile_dic[tag] += 1
            else:
                users_profile_dic[tag] = 1
    
    # 转为列表进行排序
    users_profile = list(users_profile_dic.items())
    users_profile.sort(key = lambda tag: tag[1], reverse=True)
    # print(users_profile)
    
    if (isDic):
        # 生成单曲用户群体画像[字典形式,便于为标签赋权]
        res_profile = {tag[0]:tag[1] for tag in users_profile[:10]}
        
        # print(res_profile)
        
        # print(f"\n退出get_song_profile {song_id}, isDic = {isDic}...")
        print(f"##target: return get_song_profile {res_profile}...")
        
        return res_profile  # dic
    
    else:
        # 生成单曲用户群体画像[列表形式]
        res_profile = [users_profile[i][0] for i in range(min(10,len(users_profile)))]
        
        print(f"return get_song_profile {res_profile}...")
        
        return res_profile  # list

def refilter_with_profile(sc, song_id):
    # 根据歌曲听众群体画像对初筛结果做最后一次筛选，并推荐得分较高的Top n首歌
    
    # print(f"\n进入refilter_with_profile {song_id}...")
    
    # ========初筛========
    # 获取原歌曲
    target_song = get_song(sc, song_id)
    if target_song == []:
        return
    # 基于歌曲本身进行分析,生成候选歌单
    candidate_list = filter_with_tag(sc, target_song)
    
    # ========再筛========
    # 获取本歌听众的群体用户画像[dic]
    profile_dic = get_song_profile(sc, song_id)
    
    # 对初筛结果打分
    # 歌曲得分字典
    scores = {}
    for song in candidate_list:     # song format: ["123", "a", 0.7, {"华语", "悲伤", "空灵"} ]
        candidate_profile = get_song_profile(sc, song[0], isDic=False)
        
        score = 0
        for tag in candidate_profile:
            # 标签符合用户画像则带权重加分
            if tag in profile_dic:
                score += profile_dic[tag]
        scores[song[0]] = score
    
    # 根据得分为候选歌曲进行排序
    candidate_list.sort(key = lambda song: scores[song[0]], reverse=True)
    
    # 生成最终的推荐歌曲
    final_list = candidate_list[:5]
    
    # print(f"\n退出refilter_with_profile {song_id}...")
    print(f"return final_list {final_list}...")
    
    # 写入文件
    filename = "data/rec/rec_" + song_id + ".txt"
    # 目录不存在时创建目录
    if not os.path.exists("data/rec/"):    
        os.mkdir("data/rec/")
    with open(filename,'w',encoding="utf-8") as file:
        # format: [ ["123", "a", 0.7, {"华语", "伤感"} ] * 10 ]
        final_list = [ [song[0],song[1],str(song[2])," ".join(song[3])] for song in final_list ]
        for song in final_list:
            file.write(" @#$#@ ".join(song) + "\n")
    
    return      

def get_playlist_songs(sc, playlist_id):
    # 获取该歌单列表下的所有歌曲id
    # TODO
    # return ["123","321","111"]
    
    filepath = 'hdfs://stu:9000/basic_data/info/playlist_info.txt'  # 歌单信息文件
    tmp = sc.textFile(filepath) \
            .map(lambda line: line.split(' @#$#@ ')) \
            .filter(lambda list: list[0] == playlist_id) \
            .collect()[0][8]
    
    result = tmp.split(' ')     # 转换为字符串列表
    return result    


def entry(sc):
    # 入口函数,便于调用
    for playlist in Music_charts:
        playlist_id = str(Music_charts[playlist])
        songs = get_playlist_songs(sc,playlist_id)
        for song_id in songs:
            refilter_with_profile(sc,song_id)
    print("************successfully exit************")
    

if __name__ == "__main__":
    # # 获取原歌曲
    # target_song = get_song("123")
    
    # # 基于歌曲本身进行分析
    # filter_with_tag(target_song)

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    sc = SparkContext(conf=conf)
    
    entry(sc)

    sc.stop()