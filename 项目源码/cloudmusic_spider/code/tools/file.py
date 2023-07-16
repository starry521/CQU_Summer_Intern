import openpyxl
import shutil
import os

# 将json格式的值列表转换为逗号分割的字符串
def json2str(data):

    return ' @#$#@ '.join([str(i) for i in list(data.values())])


# 向指定文件添加文件头
def add_header(filepath, header):
    with open(filepath, 'a', encoding='utf-8') as file:
        # file.seek(0)  # 定位到文件起始位置
        # file.truncate()     # 清空文件内容
        file.write(','.join([str(i) for i in header]) + "\n")
    
    file.close()
    return


# 将json格式值数据写入csv文件
def save_csv(path, data):
    
    with open(path, 'a', encoding='utf-8') as f:
        f.write(json2str(data) + "\n")
        
    f.close()

# 清空非空目录
def cleardir(path):
    if os.path.exists(path):    # 目录已存在
        shutil.rmtree(path)
    os.mkdir(path) 

    


# def create_xlsx(path, header):

#     # 创建工作簿
#     workbook = openpyxl.Workbook()

#     # 获取表
#     sheet = workbook.active

#     # 设置表头
#     # sheet.append(header)
    
#     # 保存文件
#     workbook.save(path)

#     return sheet


if __name__=="__main__":
    
    path = r'data\info'
    cleardir(path)