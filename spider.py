import requests
from urllib.parse import urlencode
import os
from hashlib import md5
import pymongo
from multiprocessing.pool import Pool

MONGO_URL = 'localhost'
MONGO_DB = 'toutiao'
MONGO_TABLE = 'toutiao' #数据集合Collection

client = pymongo.MongoClient(MONGO_URL)  # MongoClient 对象，并且指定连接的 URL 地址
db = client[MONGO_DB] #要创建的数据库名

def getPage(offset):
    params = {
        'offset': offset,
        'format': 'json',
        'keyword': '街拍',
        'autoload': 'true',
        'count': '20',
        'cur_tab': '3',
        'from': 'gallery',
    }
    url = 'https://www.toutiao.com/search_content/?' + urlencode(params)
    try:
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()
    except requests.ConnectionError:
        return ""

def getImage(json):
    data = json.get('data')
    for item in data:
        title = item.get('title')
        image_list = item.get('image_list')
        if image_list:
            for item in image_list:
                yield{
                    'title': title,
                    'image': item.get('url')
                }

def saveImage(item):
    img_path = 'img' + os.path.sep + item.get('title')
    if not os.path.exists(img_path):
        os.makedirs(img_path)
    local_image_url = item.get('image')
    new_image_url = local_image_url.replace('list', 'large')
    r = requests.get('http:' + new_image_url)
    if r.status_code == 200:
        file_path = img_path + os.path.sep +'{0}.{1}'.format(md5(r.content).hexdigest(), 'jpg')
        if not os.path.exists(file_path):
            with open(file_path, 'wb') as f:
                f.write(r.content)

def saveToMongo(item):
    if db[MONGO_TABLE].insert(item):
        print('储存到MONGODB成功', item)
    return False

def main(offset):
    json = getPage(offset)
    for item in getImage(json):
        saveImage(item)
        saveToMongo(item)

if __name__ == '__main__':
    pool = Pool()
    groups = [x * 20 for x in range(2)] #爬取五页
    pool.map(main, groups)
    pool.close() #关闭进程池（pool），使其不在接受新的任务
    pool.join() #主进程阻塞等待子进程的退出
