import sys
# import time
sys.path.append("code")
from tools.sleep import sleep
import requests
from tools.struct import get_header


# get请求，返回json数据
def get(url,retry = 5):

    if retry != 5:
        sleep(5)
    if retry == 0:
        print("retry overflows!")
        raise Exception("retry overflows!")
    try:
        response = requests.get(url, headers=get_header())
        content_json = response.json()

    
        if content_json['code'] == 200:

            response.close()
            return content_json
        
        else:
            
            print(content_json['code'])
            if content_json['code'] != -2:
                return get(url,retry=retry-1)
            else:
                print("None Request!")
                raise Exception("None Request!")
        
    except:

        print("爬取失败[from get]!")
    

# post请求
def post(url, data, retry = 5):
    
    if retry != 5:
        sleep(5)
    if retry == 0:
        print("retry overflows!")
        raise Exception("retry overflows!")
    
    try:
        response = requests.post(url, headers = get_header(), data = data)
        content_json = response.json()

        if content_json['code'] == 200:
            
            response.close()    
            return content_json
        
        else:
            
            if content_json['code'] == 406:
                return get(url,retry=retry-1)
            else:
                if content_json['code'] == -2:
                    print("None Request!")
                    raise Exception("None Request!")
                elif content_json['code'] == 404:
                    print("404!")
                    raise Exception("404")
        
    except:

        print("爬取失败[from post]!")
