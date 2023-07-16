# coding='utf-8'

import sys
sys.path.append("code")

from tools.request import get


def get_song_lyric(songid):
    '''
        根据歌曲ID获取歌词,每句歌词之间以逗号间隔
    '''

    url = f'https://music.163.com/api/song/lyric?id={songid}&lv=1&kv=1&tv=-1'
    content_json = get(url)

    if content_json is None:
        return "null"
    
    if content_json['lrc']['lyric'] != '':
        lyric = content_json['lrc']['lyric'].replace("\n", " ")
    else:
        lyric = 'null'

    return lyric


if __name__ == "__main__":

    lyric = get_song_lyric(2058598792)

    print(lyric)