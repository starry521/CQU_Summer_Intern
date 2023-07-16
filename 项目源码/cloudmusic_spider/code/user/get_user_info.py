# coding='utf-8'

import sys
import os
sys.path.append("code")
import os
from tools.struct import city_dic
from tools.utils import user_age
from tools.request import get
from tools.struct import file_info_paths
from tools.file import save_csv
from user.get_user_listen_rank import get_user_listen_rank
from user.get_user_playlist import get_user_playlist
from tools.utils import list2str
from song.get_song_info import get_song_info
from song.get_song_comments import get_song_comments
from singer.get_singer_info import get_singer_info

def get_user_info(user_id):
    '''
        获取用户基本信息
    '''
    print(f"\t\t正在爬取用户{user_id}的基本信息...")
    data = {}
    url = f'https://music.163.com/api/v1/user/detail/{user_id}'

    content_json = get(url)
    
    if content_json is None:
        return
    
    # 用户ID
    data['user_id'] = user_id

    # 用户名
    data['nickname'] = content_json['profile']['nickname']

    # 性别
    if content_json['profile']['gender'] == 1:
        data['gender'] = '男'
    elif content_json['profile']['gender'] == 2:
        data['gender'] = '女'
    else:
        data['gender'] = 'null'

    # 年龄
    if content_json['profile']['birthday'] < 0:     # 时间戳小于0，该用户未填年龄
        data['age'] = 'null'
    else: 
        data['age'] = user_age(content_json['profile']['birthday'])

    # 省份
    data['province'] = city_dic[content_json['profile']['province']] if (content_json['profile']['province'] in city_dic) else "海外"

    # 个人介绍
    if content_json['profile']['signature'] == "":
        data['signature'] = "null"
    else:
        data['signature'] = content_json['profile']['signature'].replace("\n","").replace("\u200b", "")

    # 累积听歌总数
    data['listenSongs'] = content_json['listenSongs']

    # 获取用户听歌排行(10首)
    print("\t\t正在爬取TA的听歌情况...")
    alldatalist, weeklist = get_user_listen_rank(user_id)

    if alldatalist != []:
        data['all_rank'] = list2str(alldatalist)
    else:
        data['all_rank'] = 'null'

    if  weeklist != []:  
        data['week_rank'] = list2str(weeklist)
    else:
        data['week_rank'] = 'null'

    # 获取用户创建和收藏的歌单
    create_playlists, collect_playlists = get_user_playlist(data['nickname'], user_id)
    if create_playlists != []:
        data['create_play'] = list2str(create_playlists)
    else:
        data['create_play'] = 'null'

    if collect_playlists != []:
        data['collect_play'] = list2str(collect_playlists)
    else:
        data['collect_play'] = 'null'
        
    save_csv(file_info_paths['user'], data)
    
    print("\t\t正在爬取TA的听歌总榜评论...")
    for i in range(0,len(alldatalist)):
        print(f"总榜{i+1}/{len(alldatalist)}")
        filepath = f"data/song_comments/song_{alldatalist[i]}.txt"
        if os.path.exists(filepath):    # 这首歌已经爬取过数据
            continue
        users, total = get_song_comments(alldatalist[i])
        get_song_info(alldatalist[i], total)
        
    print("\n\t\t正在爬取TA的听歌周榜评论...")
    for i in range(0,len(weeklist)):
        print(f"周榜{i+1}/{len(weeklist)}")
        filepath = f"data/song_comments/song_{weeklist[i]}.txt"
        if os.path.exists(filepath):    # 这首歌已经爬取过数据
            continue
        users, total = get_song_comments(weeklist[i])
        get_song_info(weeklist[i], total)


    # save_csv(file_info_paths['user'], data)

    
    # for song_id in alldatalist:
    #     filepath = f"data/song_comments/song_{song_id}.txt"
    #     if os.path.exists(filepath):    # 这首歌已经爬取过数据
    #         continue
    #     singer_id = get_song_info(song_id)
    #     get_song_comments(song_id)
    #     get_singer_info(singer_id)

    # for song_id in weeklist:
    #     filepath = f"data/song_comments/song_{song_id}.txt"
    #     if os.path.exists(filepath):    # 这首歌已经爬取过数据
    #         continue
    #     singer_id = get_song_info(song_id)
    #     get_song_comments(song_id)
    #     get_singer_info(singer_id)


    # return data
    


if __name__ == "__main__":

    data = get_user_info(3343232527)    # 获取指定用户的基本信息
    print(data)