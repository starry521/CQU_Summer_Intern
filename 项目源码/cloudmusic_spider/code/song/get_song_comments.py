# coding='utf-8'
import sys
sys.path.append("code")

import math
import os
from tools.request import get
from tools.comment import hotcomments, comments
from tools.sleep import sleep
from tools.progressBar import progress_bar
from tools.file import add_header
from tools.struct import file_headers



def get_song_comments(songid):
    '''
        获取歌曲评论
    '''

    filename = f"song_{songid}"
    filepath = f"data/song_comments/{filename}.txt"

    if os.path.exists(filepath):
        return [], 'null'

    # add_header(filepath, file_headers['comment'])

    print(f'\t\t开始爬取单曲评论!==>{filename}')

    page = 0    # 第一页
    
    url = f'https://music.163.com/api/v1/resource/comments/R_SO_4_{songid}?limit=20&offset={page}'

    # 获取第一页评论，json格式
    content_json = get(url)
    
    if content_json is None:
        return [], 'null'
    
    # 评论总数
    total = content_json['total']

    # 总页数
    pages = math.ceil(total / 20)

    print("\t\t总共有{}页{}条评论\n".format(pages, total))

    users = []
    # double check
    users += hotcomments(content_json, filepath) if content_json is not None else []
    users += comments(content_json, filepath) if content_json is not None else []

    # 开始获取歌曲的全部评论
    page = 1

    # 取size个页
    size = 20
    while page < pages and page < size:
        if(page == 1):
            progress_bar(page,min(pages,size))
            
        url = f'https://music.163.com/api/v1/resource/comments/R_SO_4_{songid}?limit=20&offset={page}'
        content_json = get(url)

        # 从第二页开始获取评论
        # double check
        users += comments(content_json, filepath) if content_json is not None else []
        page += 1


        if ((page+1) % 5 == 0 or page ==min(pages,size)-1):
            progress_bar(page+1,min(pages,size))

        sleep()
    
    print("爬取song评论结束...")
    return users, total



if __name__ == "__main__":

    # songname = "song_comments"
    # filepath = f"data\{songname}.txt"
    get_song_comments(4944428)
