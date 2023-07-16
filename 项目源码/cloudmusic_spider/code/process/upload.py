# 先安装simplejson，再安装pyhdfs
from pyhdfs import HdfsClient
import os
from tqdm import tqdm

def create_dir(client, dir_path):
    if client.exists(dir_path):
        client.delete(dir_path, recursive=True)
    client.mkdirs(dir_path)


if __name__=="__main__":

    client = HdfsClient(hosts='stu:50070', user_name='root')

    path = '/home/cloudmusic_spider/data/'  # 本机目录
    remote_path = '/data/'

    info_path = path + 'info/'  # info目录路径
    playlist_comments_path = path + 'playlist_comments/' # playlist_comments目录路径
    song_comments_path = path + 'song_comments/' # song_comments目录路径

    remote_info_path = remote_path + 'info/'  # info目录路径
    remote_playlist_comments_path = remote_path + 'playlist_comments/' # playlist_comments目录路径
    remote_song_comments_path = remote_path + 'song_comments/' # song_comments目录路径

    create_dir(client, remote_info_path)
    create_dir(client, remote_playlist_comments_path)
    create_dir(client, remote_song_comments_path)


    print('开始上传文件...')

    info_list = os.listdir(info_path)
    for file in tqdm(info_list, total=len(info_list), desc='上传info目录进度:'):
        path = info_path + file
        hdfs_path = remote_info_path + file
        client.copy_from_local(path, hdfs_path)

    playlist_comments_list = os.listdir(playlist_comments_path)
    for file in tqdm(playlist_comments_list, total=len(playlist_comments_list), desc='上传playlist_comments目录进度:'):
        path = playlist_comments_path + file
        hdfs_path = remote_playlist_comments_path + file
        client.copy_from_local(path, hdfs_path)

    song_comments_list = os.listdir(song_comments_path)
    for file in tqdm(song_comments_list, total=len(song_comments_list), desc='上传song_comments目录进度:'):
        path = song_comments_path + file
        hdfs_path = remote_song_comments_path + file
        client.copy_from_local(path, hdfs_path)   

    print('上传完毕')