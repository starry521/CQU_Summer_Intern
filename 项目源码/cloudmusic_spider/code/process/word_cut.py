import jieba
from pyhdfs import HdfsClient
from pyspark import SparkConf, SparkContext


# 读取停用词列表
def stopwordslist():
    stopwords = [line.strip() for line in open('/home/cloudmusic_spider/code/tools/stop_words.txt', 'r', encoding='utf-8').readlines()]
    return set(stopwords)   # 转集合，加快速度


# 增加分词
def jieba_addwords():
    jieba.add_word('这首歌')


# 对句子进行中文分词
def depart(sentence):

    # 增加指定分词
    jieba_addwords()

    # 进行中文分词
    word_list = jieba.lcut(sentence.strip())
    
    # 读取停用词列表
    stopwords = stopwordslist()

    # 筛选
    result = [word for word in word_list if word not in stopwords and len(word) > 1]

    # 返回最终结果，空格分割的字符串
    return ' '.join(result)



def create_dir(client, dir_path):
    if client.exists(dir_path):
        client.delete(dir_path, recursive=True)
    client.mkdirs(dir_path)


# 创建新的目录用以存放分词及词云所需内容
def new_dir(client, new_data_path):

    # 新建data目录
    create_dir(client, new_data_path)

    # 新建info目录
    new_info_path = new_data_path + 'info/'
    create_dir(client, new_info_path)

    # 新建playlist_comments目录
    new_playlist_comments_path = new_data_path + 'playlist_comments/'
    create_dir(client, new_playlist_comments_path)
    
    # 新建song_comments目录
    new_song_comments_path = new_data_path + 'song_comments/'
    create_dir(client, new_song_comments_path)


# 对用户个人简介分词
def signature_cut(client, rdd, filepath):

    print('开始个人简介分词')

    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
                .map(lambda list: [list[i] if i != 5 else depart(list[i]) for i in range(len(list))]) \
                .map(lambda list: ' @#$#@ '.join(list)) \
                .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create(filepath, data=str)

    print('个人简介分词结束\n')
    

# 对歌曲歌词进行分词
def lyric_cut(client, rdd, filepath):

    print('开始歌词分词')

    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
                .map(lambda list: [list[i] if i != 5 else depart(list[i]) for i in range(len(list))]) \
                .map(lambda list: ' @#$#@ '.join(list)) \
                .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create(filepath, data=str)
    
    print('歌词分词结束\n')

# 对评论内容进行分词
# def comment_cut(client, rdd, filepath):
    
#     tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
#                 .map(lambda list: [list[i] if i != 3 else depart(list[i]) for i in range(len(list))]) \
#                 .filter(lambda list: list[3] != '') \
#                 .map(lambda list: ' @#$#@ '.join(list)) \
#                 .collect()
    
#     str = '\n'.join(tmp_list).encode()

#     client.create(filepath, data=str)


def comment_cut(iterable):   

    client = HdfsClient(hosts='stu:50070', user_name='root')

    processed_data = ''

    tmp = iterable[0].split("/")
    dir, filename = tmp[-2], tmp[-1]
    
    for line in iterable[1].split("\n"):
        list = line.split(' @#$#@ ')
        list[3] = depart(list[3])
        if list[3] != '':
            processed_data += ' @#$#@ '.join(list)
            processed_data += "\n"

    # 将处理后的数据存储回hdfs
    hdfs_path = f"/cut_data/{dir}/{filename}"

    client.create(hdfs_path, data=processed_data.encode())



if __name__ == '__main__':

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    sc = SparkContext(conf=conf)

    client = HdfsClient(hosts='stu:50070', user_name='root')

    # 创建new_data目录
    new_dir(client, '/cut_data/')

    print('开始分词...')

    # 用户简介分词
    rdd1 = sc.textFile('hdfs://stu:9000/basic_data/info/user_info.txt')
    filepath1 = '/cut_data/info/user_info.txt'
    signature_cut(client, rdd1, filepath1)
    
    # 歌曲歌词分词
    rdd2 = sc.textFile('hdfs://stu:9000/basic_data/info/song_info.txt')
    filepath2 = '/cut_data/info/song_info.txt'
    lyric_cut(client, rdd2, filepath2)
    
    # playlist_files = client.listdir('/basic_data/playlist_comments/')
    # playlist_paths = ['/cut_data/playlist_comments/' + file for file in playlist_files]
    # playlist_rdds = [sc.textFile('hdfs://stu:9000/basic_data/playlist_comments/' + file) for file in playlist_files]

    # song_files = client.listdir('/basic_data/song_comments/')
    # song_paths = ['/cut_data/song_comments/' + file for file in song_files]
    # song_rdds = [sc.textFile('hdfs://stu:9000/basic_data/song_comments/' + file) for file in song_files]

    # # 歌单评论分词
    # for rdd, filepath in zip(playlist_rdds, playlist_paths):
    #     comment_cut(rdd, filepath)

    # # 歌曲评论分词
    # for rdd, filepath in zip(song_rdds, song_paths):
    #     comment_cut(rdd, filepath)

    print('开始评论分词')

    file_list1 = sc.wholeTextFiles(r'hdfs://stu:9000/basic_data/playlist_comments')
    file_list2 = sc.wholeTextFiles(r'hdfs://stu:9000/basic_data/song_comments/')
    combined_list = file_list1.union(file_list2)
    
    combined_list.foreach(comment_cut)

    print('评论分词结束\n')

    sc.stop()

    print('分词结束')