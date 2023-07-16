import sys
sys.path.append("code")

from tools.request import get

# 爬取用户创建和用户收藏的歌单，返回创建歌单id和收藏歌单id
def get_user_playlist(user_name, user_id): 
    '''
        获取用户创建与收藏的歌单
    '''

    url = f'https://music.163.com/api/user/playlist/?offset=0&limit=200&uid={user_id}'
    content_json = get(url)

    create_playlists = []   # 该用户创建的歌单
    collect_playlists = []  # 该用户收藏的歌单
    
    if content_json is None:
        return create_playlists,collect_playlists

    # 遍历歌单
    for playlist in content_json['playlist']:

        if playlist['creator']['nickname'] == user_name:    # 歌单创建者是该用户，即是创建歌单

            create_playlists.append(playlist['id'])

        else:

            collect_playlists.append(playlist['id'])

    return create_playlists[0:5], collect_playlists[0:5]


if __name__=="__main__":

    create_playlists, collect_playlists = get_user_playlist('次姬', 31475897)
    print(create_playlists)
    print(collect_playlists)








