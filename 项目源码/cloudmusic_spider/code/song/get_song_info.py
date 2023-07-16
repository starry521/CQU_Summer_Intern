# coding='utf-8'

import sys
sys.path.append("code")
from tools.request import get
from song.get_song_lyric import get_song_lyric
from tools.file import save_csv
from tools.struct import file_info_paths
from song.get_song_tag import get_song_tag


def get_song_info(songid, total):
    '''
        获取歌曲基本信息
    '''
    print(f"\t\t开始爬取单曲{songid}的详细信息...")
    url = f'https://music.163.com/api/song/detail/?id={songid}&ids=[{songid}]'
    content_json = get(url)
    
    if content_json is None:
        return -1
    
    data = {}
    for song in content_json['songs']:
        
        # 歌曲id
        data['song_id'] = songid

        # 获取歌曲名
        data['song_name'] = song['name']

        # 获取歌手ID
        data['singer_id'] = song['artists'][0]['id']
        
        # 获取歌手名(只获取第一个)
        data['singer_name'] = song['artists'][0]['name']
        
        # 获取所属专辑
        data['album'] = song['album']['name']

        # 获取歌词
        data['lyric'] = get_song_lyric(songid)
        
        # 评论数
        data['total'] = total
        
        # get tag [from playlists that includes it]
        
        tags = get_song_tag(songid)
        data['tag'] =  tags if len(tags)!=0 else "null"
        # print(f"get= {data['tag']}")
        
        save_csv(file_info_paths['song'], data)

        return data['singer_id']




if __name__ == "__main__":
    a = 0
    get_song_info(1964443044,a)