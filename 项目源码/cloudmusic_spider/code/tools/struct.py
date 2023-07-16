import random
from fake_useragent import UserAgent
# 伪造请求头
headers={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            # 'Origin': 'https://music.163.com',
            # 'Referer': 'https://music.163.com/playlist?id=750115024',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Cookie': 'WM_TID=36fj4OhQ7NdU9DhsEbdKFbVmy9tNk1KM; _iuqxldmzr_=32; _ntes_nnid=26fc3120577a92f179a3743269d8d0d9,1536048184013; _ntes_nuid=26fc3120577a92f179a3743269d8d0d9; __utmc=94650624; __utmz=94650624.1536199016.26.8.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); WM_NI=2Uy%2FbtqzhAuF6WR544z5u96yPa%2BfNHlrtTBCGhkg7oAHeZje7SJiXAoA5YNCbyP6gcJ5NYTs5IAJHQBjiFt561sfsS5Xg%2BvZx1OW9mPzJ49pU7Voono9gXq9H0RpP5HTclE%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6eed5cb8085b2ab83ee7b87ac8c87cb60f78da2dac5439b9ca4b1d621f3e900b4b82af0fea7c3b92af28bb7d0e180b3a6a8a2f84ef6899ed6b740baebbbdab57394bfe587cd44b0aebcb5c14985b8a588b6658398abbbe96ff58d868adb4bad9ffbbacd49a2a7a0d7e6698aeb82bad779f7978fabcb5b82b6a7a7f73ff6efbd87f259f788a9ccf552bcef81b8bc6794a686d5bc7c97e99a90ee66ade7a9b9f4338cf09e91d33f8c8cad8dc837e2a3; JSESSIONID-WYYY=G%5CSvabx1X1F0JTg8HK5Z%2BIATVQdgwh77oo%2BDOXuG2CpwvoKPnNTKOGH91AkCHVdm0t6XKQEEnAFP%2BQ35cF49Y%2BAviwQKVN04%2B6ZbeKc2tNOeeC5vfTZ4Cme%2BwZVk7zGkwHJbfjgp1J9Y30o1fMKHOE5rxyhwQw%2B%5CDH6Md%5CpJZAAh2xkZ%3A1536204296617; __utma=94650624.1052021654.1536048185.1536199016.1536203113.27; __utmb=94650624.12.10.1536203113',
            'Host': 'music.163.com',
            'Referer': 'http://music.163.com/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}

# 使用随机User-Agent
User_Agent = {0:"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
              1:"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
            #   2:"Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1",
            #   3:"Mozilla/5.0 (Linux; Android 8.0.0; SM-G955U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Mobile Safari/537.36",
            #   4:"Mozilla/5.0 (iPad; CPU OS 13_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/87.0.4280.77 Mobile/15E148 Safari/604.1",
              2:"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36",
              }



# 获取随机生成的请求头
def get_header(idx = -1):
    # agent_idx = random.randint(0, 2)
    # if idx != -1:
    #     agent_idx = idx
    # print(f"now using=={agent_idx}")
    # headers['User-Agent'] = User_Agent[agent_idx]
    
    # 改用fake_useragent
    headers['User-Agent'] = str(UserAgent().random)
    # print(headers['User-Agent'])

    return headers

# 城市字典
city_dic = {
    0: 'null',
    110000: '北京市',
    120000: '天津市',
    130000: '河北省',
    140000: '山西省',
    150000: '内蒙古自治区',
    210000: '辽宁省',
    220000: '吉林省',
    230000: '黑龙江省',
    310000: '上海市',
    320000: '江苏省',
    330000: '浙江省',
    340000: '安徽省',
    350000: '福建省',
    360000: '江西省',
    370000: '山东省',
    410000: '河南省',
    420000: '湖北省',
    430000: '湖南省',
    440000: '广东省',
    450000: '广西壮族自治区',
    460000: '海南省',
    500000: '重庆市',
    510000: '四川省',
    520000: '贵州省',
    530000: '云南省',
    540000: '西藏自治区',
    610000: '陕西省',
    620000: '甘肃省',
    630000: '青海省',
    640000: '宁夏回族自治区',
    650000: '新疆维吾尔自治区',
}


# 需要爬取的单曲排行榜
Music_charts = {
    '飙升榜':19723756,
    '新歌榜':3779629,
    '原创榜':2884035,
    '热歌榜':3778678,
    '云音乐说唱榜':991319590,
    '云音乐古典榜':71384707,
    '云音乐电音榜':1978921795,
    '黑胶VIP爱听榜':5453912201,
    '云音乐ACG榜':71385702,
    '云音乐韩语榜':745956260,
    '云音乐国电榜':10520166,
    'UK排行榜周榜':180106,
    '美国Billboard榜':60198,
    'Beatport全球电子舞曲榜':3812895,
    'KTV唛榜':21845217,
    '日本Oricon榜':60131,
    '云音乐欧美热歌榜':2809513713,
    '云音乐欧美新歌榜':2809577409,
    '法国 NRJ Vos Hits 周榜':27135204 ,
    '云音乐ACG动画榜':3001835560,
    '云音乐ACG游戏榜':3001795926,
    '云音乐ACG VOCALOID榜':3001890046,
    '云音乐日语榜':5059644681,
    '云音乐摇滚榜':5059633707,
    '云音乐国风榜单':5059642708,
    '潜力爆款榜':5338990334,
    '云音乐民谣榜':5059661515,
    '听歌识曲榜':6688069460,
    '网络热歌榜':6723173524,
    '俄语榜':6732051320,
    '越南语榜':6732014811,
    '中文DJ榜':6886768100,
    '俄罗斯top hit流行音乐榜':6939992364,
    '泰语榜':7095271308,
    'BEAT排行榜':7356827205,
    '编辑推荐榜VOL.55 张碧晨以笼为喻，洞察复杂幽暗的人心':7325478166,
    'LOOK直播歌曲榜':7603212484,
    '赏音榜':7775163417,
    '黑胶VIP新歌榜':7785123708,
    '黑胶VIP热歌榜':7785066739,
    '黑胶VIP爱搜榜':7785091694,
    '实时热度榜':8246775932
}


# 文件头
file_headers = {
    'song': ["song_id", "song_name", "singer_id", "singer_name", "album", "lyric", "total", "tags", "emo"],
    'singer': ["singer_id", "singer_name", "accountId", "fans", "hotsongs", "emo"],
    'playlist': ["playlist_id", "playlist_name", "playCount", "subscribedCount", "description", "tags", "creator", "song_num", "trackIds", "total", "emo"],
    'comment': ["user_id", "user_name", "comment_id", "comment", "likecount", "location"],
    'user': ["user_id", "nickname", "gender", "age", "province", "signature", "listenSongs", "all_rank", "week_rank", "create_play", "collect_play", "emo"]
}


# 文件路径
file_info_paths = {
    'song': r'data/info/song_info.txt',
    'singer': r'data/info/singer_info.txt',
    'playlist': r'data/info/playlist_info.txt',
    'user': r'data/info/user_info.txt'
}