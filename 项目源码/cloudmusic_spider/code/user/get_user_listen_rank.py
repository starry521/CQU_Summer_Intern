import sys
sys.path.append("code")

from tools.request import post
from tools.post_params import get_params_id

def get_user_listen_rank(user_id):

    # 所有歌曲
    alldatalist = []

    #最近一周歌曲
    weeklist = []

    url='https://music.163.com/weapi/v1/play/record?csrf_token='
    params, encSecKey = get_params_id(user_id)
    data={'params':params, 'encSecKey':encSecKey}
    
    content_json = post(url, data)

    if(content_json is None):
        return alldatalist, weeklist
    
    i = j = 0

    for item in content_json['allData']:
        if(i<10):
            alldatalist.append(str(item['song']['id']))
            i=i+1

    for item in content_json['weekData']:
        if(j<10):
            weeklist.append(str(item['song']['id']))
            j=j+1

    return alldatalist, weeklist


if __name__=="__main__":

    user_id = 2139247395
    alldatalist, weekdatalist = get_user_listen_rank(user_id)
    print(alldatalist, weekdatalist)
