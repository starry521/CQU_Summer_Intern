import sys
import time
import requests
from lxml import etree
sys.path.append("code")
from tools.struct import get_header
from tools.request import get
from tools.sleep import sleep

def get_playlist_tag(playlistid):
    '''
        获取指定歌单的标签
    '''
    # print(f"开始爬取歌单{playlistid}的tags...")
    try:
        url = f"https://music.163.com/playlist?id={playlistid}"
        response = requests.get(url, headers=get_header())
        if response.status_code == 200:
            response.close()
    except:
        print("爬取歌单Tag失败!")
    if response is None:
        return ""   
    html = etree.HTML(response.text)
    # list
    html_data = html.xpath("//div[@class='tags f-cb']//i/text()")
    # print(html_data)

    return html_data


def get_song_tag(song_id):

    try:
        url = f"https://music.163.com/song?id={song_id}"
        response = requests.get(url, headers=get_header())
        if response.status_code == 200:
            response.close()
    except:
        print("爬取单曲Tag失败!")
    
    if response is None:
        return "null"    
    html = etree.HTML(response.text)
    # time.sleep(1)
    # list
    html_data = html.xpath("//div[@class='info']//@data-res-id")
    html_data = [int(i) for i in html_data]
    # print(html_data)
    tags = []
    for id in html_data:
        tags += get_playlist_tag(id)
    # 去重
    tags = list(set(tags))
    # str
    res = str(" ".join(tags))
    # print(res)
    return res
        

if __name__ == "__main__":
    get_song_tag(1964443044)