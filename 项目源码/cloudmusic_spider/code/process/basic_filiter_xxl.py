import os
import re
import shutil
from pyhdfs import HdfsClient
from pyspark import SparkConf, SparkContext

def resetdir(path,clear = False): # local
    if os.path.exists(path):    # 目录已存在
        if clear:
            shutil.rmtree(path)
        return
    os.mkdir(path)

def create_dir(client, dir_path, clear = False):   # hdfs
    if client.exists(dir_path):
        if clear:
            client.delete(dir_path, recursive=True)
        return
    client.mkdirs(dir_path)


def custom_function(iterable):   
    # 创建HdfsClient对象
    client = HdfsClient(hosts='stu:50070', user_name='root')

    # 对每个分区进行处理
    processed_data = ""

    filename = iterable[0].split("/")[-1]
    for line in iterable[1].split("\n"):
        data = line.split(' @#$#@ ')
        if len(data) == 6 and data[3] != '':
            processed_data += ' @#$#@ '.join(data)
            processed_data += "\n"

    # 写本地测试
    local_dir = "/home/cloudmusic_spider/data/done/song_comments/"
    local_path = local_dir + filename
    resetdir(local_dir)
    with open(local_path, 'w') as file:
        file.write(processed_data)
        file.close()

    # 将处理后的数据存储回hdfs
    hdfs_dir = "/new_data/song_comments/"
    hdfs_path = hdfs_dir + filename
    create_dir(client,hdfs_dir)
    client.copy_from_local(local_path,hdfs_path)


if __name__ == '__main__':
    conf = SparkConf().setAppName("job1")
    sc = SparkContext(conf=conf)

    # 重置本地及hdfs的相关文件夹
    resetdir("/home/cloudmusic_spider/data/done/song_comments/",True)
    client = HdfsClient(hosts='stu:50070', user_name='root')
    create_dir(client,"/new_data/song_comments/",True)

    # 获取HDFS上指定路径下的所有文件
    hdfs_path = "hdfs://stu:9000/data/song_comments/"
    file_list = sc.wholeTextFiles(hdfs_path)
    file_list.foreach(custom_function)

    sc.stop()