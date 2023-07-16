import re
from tqdm import tqdm
from pyhdfs import HdfsClient
from pyspark import SparkConf, SparkContext


def lyric_filter(lyric):

    lyric = re.sub('\[[^\]]*\]', '', lyric)    # 去除时间标签
    lyric = re.sub(r'\s*\S+\s*[：:]\s*\S+', '', lyric).lstrip()

    return lyric


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


# 对歌手信息文件进行清洗
def singer_info_filter(client, rdd, filepath):

    print('开始清洗歌手')

    # 去重、去除列数不为5
    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
                    .map(lambda list: (list[0], list)) \
                    .reduceByKey(lambda x,y: x) \
                    .map(lambda x: x[1]) \
                    .filter(lambda list: len(list) == 5) \
                    .map(lambda list: ' @#$#@ '.join(list)) \
                    .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create(filepath, data=str)

    print('歌手清洗完毕')

# 对歌单信息文件进行清洗
def playlist_info_filter(client, rdd, filepath):

    print('开始清洗歌单')

    # 去重、去除列数不为8
    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
                .map(lambda list: (list[0], list)) \
                .reduceByKey(lambda x,y: x) \
                .map(lambda x: x[1]) \
                .filter(lambda list: len(list) == 10) \
                .map(lambda list: ' @#$#@ '.join(list)) \
                .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create(filepath, data=str)

    print('歌单清洗完毕')

# 对用户信息文件进行清洗
def user_info_filter(client, rdd, filepath):

    print('开始清洗用户')

    # 去重、去除列数不为11、去除年龄不在范围内、不清洗个人简介为空的用户
    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
                .map(lambda list: (list[0], list)) \
                .reduceByKey(lambda x,y: x) \
                .map(lambda x: x[1]) \
                .filter(lambda list: len(list) == 11 and ( list[3] == 'null' or 0 < int(list[3]) < 100)) \
                .map(lambda list: ' @#$#@ '.join(list)) \
                .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create(filepath, data=str)

    print('用户清洗结束')

# 对歌曲信息文件进行清洗
def song_info_filter(client, rdd, filepath):

    print('开始清洗歌曲')

    # 去重、去除列数不为6、去除歌词为空、去除歌词无关信息
    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
                .map(lambda list: (list[0], list)) \
                .reduceByKey(lambda x,y: x) \
                .map(lambda x: x[1]) \
                .filter(lambda list: len(list) == 8 and list[5] != 'null') \
                .map(lambda list: [list[i] if i != 5 else lyric_filter(list[i]) for i in range(len(list))]) \
                .map(lambda list: ' @#$#@ '.join(list)) \
                .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create(filepath, data=str)

    print('清洗歌曲结束')

# 对评论文件进行清洗
def comment_filter(client, rdd, filepath):
      
    # 去除列数不为6、以及评论为空的数据
    tmp_list = rdd.map(lambda line: line.split(' @#$#@ ')) \
            .filter(lambda list: len(list) == 6 and list[3] != '') \
            .map(lambda list: ' @#$#@ '.join(list)) \
            .collect()
    
    str = '\n'.join(tmp_list).encode()

    client.create(filepath, data=str)
        

def process_files(client, files, paths):
    for file, filepath in tqdm(zip(files, paths), total=len(files), desc='歌曲评论处理进度'):
        rdd = sc.textFile('hdfs://stu:9000/data/song_comments/' + file)
        comment_filter(client, rdd, filepath)


if __name__ == '__main__':

    conf = SparkConf().setMaster("spark://stu:7077").setAppName("job1")
    sc = SparkContext(conf=conf)

    client = HdfsClient(hosts='stu:50070', user_name='root')

    # 创建basic_data目录
    # new_dir(client, '/basic_data/')

    print('开始清洗...')

    # 清洗歌单信息文件
    # rdd1 = sc.textFile('hdfs://stu:9000/data/info/playlist_info.txt')
    # filepath1 = '/basic_data/info/playlist_info.txt'
    # playlist_info_filter(client, rdd1, filepath1)

    # 清洗歌手信息文件
    # rdd2 = sc.textFile('hdfs://stu:9000/data/info/singer_info.txt')
    # filepath2 = '/basic_data/info/singer_info.txt'
    # singer_info_filter(client, rdd2, filepath2)

    # 清洗用户信息文件
    # rdd3 = sc.textFile('hdfs://stu:9000/data/info/user_info.txt')
    # filepath3 = '/basic_data/info/user_info.txt'
    # user_info_filter(client, rdd3, filepath3)
    
    # 清洗歌曲信息文件
    # rdd4 = sc.textFile('hdfs://stu:9000/data/info/song_info.txt')
    # filepath4 = '/basic_data/info/song_info.txt'
    # song_info_filter(client, rdd4, filepath4)


    # playlist_files = client.listdir('/data/playlist_comments/')
    # playlist_paths = ['/basic_data/playlist_comments/' + file for file in playlist_files]
    # playlist_rdds = [sc.textFile('hdfs://stu:9000/data/playlist_comments/' + file) for file in playlist_files]

    song_files = client.listdir('/data/song_comments/')
    song_paths = ['/basic_data/song_comments/' + file for file in song_files]
    # song_rdds = [sc.textFile('hdfs://stu:9000/data/song_comments/' + file) for file in song_files]

    # total_length1 = len(playlist_rdds)

    # 清洗歌单评论文件
    # for rdd, filepath in tqdm(zip(playlist_rdds, playlist_paths), total= total_length1, desc='歌单评论处理进度'):
    #     comment_filter(client, rdd, filepath)
    
    total_length2 = len(song_files)
   
    # 清洗歌曲评论文件
    for file, filepath in tqdm(zip(song_files, song_paths), total= total_length2, desc='歌曲评论处理进度'):
        rdd = sc.textFile('hdfs://stu:9000/data/song_comments/' + file)
        comment_filter(client, rdd, filepath)
    

    # batch_size = 100  # 每批处理的文件数量
    # num_batches = len(song_files) // batch_size

    # for i in range(num_batches):
    #     batch_files = song_files[i * batch_size: (i + 1) * batch_size]
    #     batch_paths = song_paths[i * batch_size: (i + 1) * batch_size]
    #     process_files(client, batch_files, batch_paths)

    # remaining_files = song_files[num_batches * batch_size:]
    # remaining_paths = song_paths[num_batches * batch_size:]
    # process_files(client, remaining_files, remaining_paths)

    sc.stop()

    print('清洗结束')