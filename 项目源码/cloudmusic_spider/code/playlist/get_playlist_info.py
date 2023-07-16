# 爬取相关数据
import sys
sys.path.append("code")
from tools.struct import file_info_paths
from tools.file import save_csv
from tools.request import get
from tools.utils import list2str

def get_playlist_info(playlistid, total):
    '''
        获取指定歌单的基本信息
    '''
    print(f"开始爬取歌单{playlistid}的详细信息...")
    data = {}
    url = f'https://music.163.com/api/v1/playlist/detail?id={playlistid}'

    content_json = get(url)

    if content_json is None:
        return []
    
    # 歌单ID
    data['playlist_id'] = content_json['playlist']['id']
    
    # 歌单名称
    data['playlist_name'] = content_json['playlist']['name']
    
    # 播放量
    data['playCount'] = content_json['playlist']['playCount']
    
    # 收藏数
    data['subscribedCount'] = content_json['playlist']['subscribedCount']
    
    # 歌单描述
    if content_json['playlist']['description'] != None:
        data['description'] = content_json['playlist']['description'].replace("\n", "")
    else:
        data['description'] = 'null'
        
    # 歌单标签
    if content_json['playlist']['tags'] != []:
        data['tags'] = ' '.join(content_json['playlist']['tags'])
    else:
        data['tags'] = 'null'

    

    # 创建者ID
    data['creator'] = content_json['playlist']['creator']['userId']
    
    # 歌曲id列表
    ids = [track['id'] for track in content_json['playlist']['trackIds']]

    # 歌单收录音乐的数量
    data['song_num'] = len(ids)

    # 只记录前十首歌
    ids = ids[:10] if len(ids)>=10 else ids
    data['trackIds'] = list2str(ids)


    # 评论数
    data['total'] = total

    save_csv(file_info_paths['playlist'], data)
    
    return ids    # 只分析前十首歌
    


if __name__ == "__main__":

    get_playlist_info(60198)    # 获取指定歌单的基本信息