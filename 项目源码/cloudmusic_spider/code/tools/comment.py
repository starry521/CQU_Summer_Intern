import sys
sys.path.append("code")

from tools.file import save_csv
# from tools.utils import timestamp2date
# from user.get_user_info import get_user_info

# 从json中提取热评
def hotcomments(content_json, filepath): 

    users = []

    # 键在字典中则返回users, 否则返回[]
    if 'hotComments' in content_json:

        # 遍历每一条热评
        for item in content_json['hotComments']:

            data = {}   # 存储数据

            # 热评的用户
            user = item['user']

            # 热评的用户ID
            data['user_id'] = user['userId']

            # 热评的用户名
            data['user_name'] = user['nickname']

            # 热评ID
            data['comment_id'] = item['commentId']

            # 热评内容
            data['comment'] = item['content'].replace("\n"," ")
            
            # 热评时间
            # data['date'], data['time'] = timestamp2date(item['time'])

            # 热评点赞数
            data['likecount'] = item['likedCount']

            # 评论为空，跳过
            if(data['comment'] == None):
                continue
                
            # 评论省份
            if item['ipLocation']['location'] == "":
                data['location'] = "null"
            else:
                data['location'] = item['ipLocation']['location']

            save_csv(filepath, data)

            # get_user_info(data['user_id'])  # 爬取用户信息
            users.append(data['user_id'])
            
        return users
    
    return []


# 从json提取普通评论
def comments(content_json, filepath):

    users = []
    if "comments" in content_json:
        for item in content_json['comments']:

            data = {}
            
            # 发表评论的用户
            user = item['user']

            # 发表评论的用户ID
            data['user_id'] = user['userId']

            # 发表评论的用户名
            data['user_name'] = user['nickname']

            # 发表评论ID
            data['comment_id'] = item['commentId']

            # 发表评论内容
            data['comment'] = item['content'].replace("\n"," ")
            
            # 发表评论时间
            # data['date'], data['time'] = timestamp2date(item['time'])

            # 发表评论点赞数
            data['likecount'] = item['likedCount']

            # 发表评论为空，跳过
            if(data['comment'] == None):
                continue
                
            # 发表评论省份
            if item['ipLocation']['location'] == "":
                data['location'] = "null"
            else:
                data['location'] = item['ipLocation']['location']


            save_csv(filepath, data)

            # get_user_info(data['user_id'])  # 爬取用户信息
            users.append(data['user_id'])

        return users
    
    return []
