import codecs
import math
import random
import base64
from Crypto.Cipher import AES


# 生成指定长度的随机字符串
def generate_random_strs(length):

    # 生成随机字符串的字符池(大小写字母+数字)
    char_pool = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    # 循环次数
    i = 0

    # 初始化随机字符串
    random_str  = ""
    while i < length:

        # random.random()生成0-1随机数，* 指定长度并向下取整，得到字符池里的随机一个下标
        e = math.floor(random.random() * len(char_pool))

        # 从字符池中取出并加上
        random_str += list(char_pool)[e]

        i = i + 1

    return random_str


# AES加密
def AESencrypt(msg, key):

    # 如果不是16的倍数则进行填充，计算需要填充的位数
    padding = 16 - len(msg) % 16

    # 这里使用padding对应的单字符进行填充
    msg = msg + padding * chr(padding)

    # 用来加密或者解密的初始向量(必须是16位)
    iv = '0102030405060708'

    key=key.encode('utf-8')
    iv=iv.encode('utf-8')
    msg=msg.encode('utf-8')

    cipher = AES.new(key, AES.MODE_CBC, iv)

    # 加密后得到的是bytes类型的数据
    encryptedbytes = cipher.encrypt(msg)

    # 使用Base64进行编码,返回byte字符串
    encodestrs = base64.b64encode(encryptedbytes)

    # 对byte字符串按utf-8进行解码
    enctext = encodestrs.decode('utf-8')

    return enctext
    
    
# RSA加密
def RSAencrypt(randomstrs, key, f):

    # 随机字符串逆序排列
    string = randomstrs[::-1]

    # 将随机字符串转换成byte类型数据
    text = bytes(string, 'utf-8')

    seckey = int(codecs.encode(text, encoding='hex'), 16)**int(key, 16) % int(f, 16)

    return format(seckey, 'x').zfill(256)


# 获取参数:两次AES加密生成encText，再进行RSA加密生成encSecKey
def get_params(page):

    # msg也可以写成msg = {"offset":"页面偏移量=(页数-1) *　20", "limit":"20"},offset和limit这两个参数必须有(js)
    # limit最大值为100,当设为100时,获取第二页时,默认前一页是20个评论,也就是说第二页最新评论有80个,有20个是第一页显示的
    # msg = '{"rid":"R_SO_4_1302938992","offset":"0","total":"True","limit":"100","csrf_token":""}'

    # 偏移量用于确定从哪个位置开始获取评论数据
    offset = (page-1) * 20

    # 生成一个包含评论请求参数的JSON字符串
    # offset--偏移量，total--是否包含总评论数，limit--每页的评论数量，csrf_token--一个可选的CSRF令牌
    # offset和limit是必选参数,其他参数是可选的,其他参数不影响data数据的生成
    msg = '{"offset":' + str(offset) + ',"total":"True","limit":"20","csrf_token":""}'
    key = '0CoJUm6Qyw8W8jud'    # 密钥用于加密评论请求参数
    enctext = AESencrypt(msg, key)  # 对评论请求参数msg进行加密，加密的结果存储在enctext变量中

    # 生成长度为16的随机字符串
    i = generate_random_strs(16)

    # 两次AES加密之后得到encText
    encText = AESencrypt(enctext, i)

    f = '00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7'
    e = '010001'

    # RSA加密之后得到encSecKey的值
    encSecKey = RSAencrypt(i, e, f)

    return encText, encSecKey


def get_params_id(user_id):
    # msg也可以写成msg = {"offset":"页面偏移量=(页数-1) *　20", "limit":"20"},offset和limit这两个参数必须有(js)
    # limit最大值为100,当设为100时,获取第二页时,默认前一页是20个评论,也就是说第二页最新评论有80个,有20个是第一页显示的
    # msg = '{"rid":"R_SO_4_1302938992","offset":"0","total":"True","limit":"100","csrf_token":""}'
    # 偏移量
    # offset = (page-1) * 20
    # offset和limit是必选参数,其他参数是可选的,其他参数不影响data数据的生成
    msg='{"uid":' + str(user_id) +',"type":"-1","limit":"1000","offset":"0","total":"True","csrf_token":""}'
    key = '0CoJUm6Qyw8W8jud'
    f = '00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7'
    e = '010001'

    enctext = AESencrypt(msg, key)

    # 生成长度为16的随机字符串
    i = generate_random_strs(16)

    # 两次AES加密之后得到params的值
    encText = AESencrypt(enctext, i)

    # RSA加密之后得到encSecKey的值
    encSecKey = RSAencrypt(i, e, f)

    return encText, encSecKey
