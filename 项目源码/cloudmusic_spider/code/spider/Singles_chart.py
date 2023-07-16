from itertools import zip_longest
import sys
import time
sys.path.append("code")
from tools.struct import Music_charts, file_headers, file_info_paths
from tools.file import add_header
from playlist.get_playlist_info import get_playlist_info
from playlist.get_playlist_comments import get_playlist_comments
from song.get_song_info import get_song_info
from song.get_song_comments import get_song_comments
from singer.get_singer_info import get_singer_info
from user.get_user_info import get_user_info
from tools.progressBar import progress_bar
from tools.sleep import sleep
from tools.file import cleardir
from multiprocessing import Process,Queue,Pool,Manager,Semaphore
from threading import Thread
import multiprocessing as mp

# 进程数量限制
def limit_list():
    return 1
def limit_track():
    return 12
def limit_user():
    return 12

def init():
    # return
    cleardir(r'data/info')
    cleardir(r'data/song_comments')
    cleardir(r'data/playlist_comments')
    
    # add_header(file_info_paths['playlist'], file_headers['playlist'])
    # add_header(file_info_paths['song'], file_headers['song'])
    # add_header(file_info_paths['singer'], file_headers['singer'])
    # add_header(file_info_paths['user'], file_headers['user'])
    
def anauser(user_id,i,size):
    # 拥塞控制
    sleep(2.5)
    get_user_info(user_id)
    # if ((i+1) % 10 == 0 or i ==size-1):
    # progress_bar(i+1,size)
    print(f"爬取用户中...进度:{i+1} / {size} ...")
    sleep()

def anasong(track_id,i,size,q):
    print(f"\t\t单曲idx:{i+1}/{size}")
    # 拥塞控制
    sleep(2.5)
    tmp_users, total = get_song_comments(track_id)           # 爬取歌曲评论

    singer_id = get_song_info(track_id, total)   # 爬取歌曲基本信息

    get_singer_info(singer_id)           # 爬取歌手基本信息

    # tmp_users = list(set(tmp_users))

    # 取每首歌的前10个用户
    tmp_users = tmp_users[0:10]
    
    q.put(tmp_users)
    
def analist(chart_id, sem):
    # 占用信号量
    sem.acquire()
    
    users, total = get_playlist_comments(chart_id)     # 爬取排行榜的评论
    # print("\n!!!!!!!!!!get_playlist_comments结束!!!!!!!!!!!!")
    
    trackIds = get_playlist_info(chart_id, total)     # 爬取排行榜的基本信息
    # print("\n!!!!!!!!!!get_playlist_info结束!!!!!!!!!!!!")
    
    # 取每个歌单的前20个用户
    users = users[0:20]
    print(f"\n=====从歌单中取出{len(users)}个用户====\n")

    # tracks = []
    # queue = [Queue() for i in range(len(trackIds))]
    # 不限制进程数量
    # for i in range(0,len(trackIds)):  # 遍历该排行榜中的所有歌曲
    #     q = queue[i]
    #     track = Process(target=anasong,args=(trackIds[i],i,len(trackIds),q))
    #     track.start()
    #     tracks.append(track)

    # for p in tracks:
    #     p.join()
    
    # 设置进程数量限制
    queue = [Manager().Queue() for i in range(len(trackIds))]
    pool_songs = Pool(processes=limit_track())
    size_t = [len(trackIds) for i in range(len(trackIds))]
    params_songs = zip(trackIds,range(len(trackIds)),size_t,queue)
    # print (list(params))
    
    # # 异步并发，效果更佳，但CPU招架不住
    # pool_songs.starmap_async(anasong,params_songs)
    # 同步
    pool_songs.starmap(anasong,params_songs)
    # 先关闭进程池
    pool_songs.close()
    pool_songs.join()
    
    for i in range(len(queue)):
        users += queue[i].get()
        print(f"===Now the size is {len(users)}")
    
    # Light对于每个歌单只取20*1+10*10个用户
    users = list(set(users))
    # print(f"\n\n{users}\n\n")
    print("\t正在爬取与本排行榜相关的用户信息...")
    print(f"=====待爬取用户总数为{len(users)}=====")
    
    # # 单开
    # for i in range(0,len(users)):
    #     anauser(users[i],i,len(users))
    
    # # 不限制进程数量
    # users_process = []
    # for i in range(0,len(users)):
    #     sleep()
    #     uid = users[i]
    #     print(uid)
    #     us = Process(target=anauser,args=(uid,i,len(users)))
    #     users_process.append(us)
    #     us.start()
    # for us in users_process:
    #     us.join()
        
    # 设置进程数量限制
    pool_users = Pool(processes=limit_user())
    size = [len(users) for i in range(len(users))]
    params = zip(users,range(len(users)),size)
    # print (list(params))
    # # 异步并发，效果更佳，但CPU招架不住
    # pool_users.starmap_async(anauser,params)
    pool_users.starmap(anauser,params)
    pool_users.close()
    pool_songs.join()
    
    # 释放信号量
    sem.release()
    return

if __name__ == "__main__":    
    init()
    # pool = mp.Pool()
    # pool.map(analist,Music_charts.values())
    
    # 最大一级子进程信号量
    maxSem = Semaphore(limit_list())
    processes = []
    for chart_id in Music_charts.values():
        
        time.sleep(7)
        pl = Process(target=analist, args=(chart_id, maxSem))
        pl.start()
        processes.append(pl)
    
    for pl in processes:
        pl.join()
    
    print("**************successfully exit**************")
    
    
   