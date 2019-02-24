# Author : ZhangTong
import time

import requests
import pymysql
from lxml import etree
from multiprocessing.pool import Pool

from config import *

class News_sina():

    def __init__(self):
        self.start_url = 'http://news.sina.com.cn/guide/'
        self.now = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'
        }
        self.table = 'news'
        self.dct1 = {}
        self.dct2 = {}
        self.dct3 = {}

    def download(self, url):
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                response.encoding = 'utf-8'
                return response
        except Exception as e:
            print('%s + download + %s' % (e, url))

    def parse1(self, response):
        try:
            html = etree.HTML(response.text)
            links = html.xpath('//div[@id="tab01"]//ul/li/a/@href')
            titles = html.xpath('//div[@id="tab01"]//ul/li/a/text()')
            for i in range(len(links)):
                self.dct1[links[i]] = titles[i]
            return self.dct1
        except Exception as e:
            print('%s + parse1 + %s' % (e, response.url))

    def parse2(self, response):
        try:
            html = etree.HTML(response.text)
            links = html.xpath('//a/@href')
            lst = []
            for link in links:
                if self.now in link and link not in lst:
                    lst.append(link)
            return lst, self.dct1[response.url]
        except Exception as e:
            print('%s + parse2 + %s' % (e, response.url))

    def parse3(self, response, type):
        try:
            html = etree.HTML(response.text)
            title = html.xpath('//h1/text() | //h2/text()')[0]
            content = html.xpath('//div[@id="artibody"]//p/text() | //div[@id="artibody"]//img/@src | //div[@id="artibody"]//video/@src')
            self.dct3['title'] = title
            self.dct3['content'] = News_sina.del_with_content(self, content)
            self.dct3['type'] = type
            self.dct3['date'] = self.now
            self.dct3['source'] = response.url
            # self.dct3['time'] = self.now
            # print(self.dct3)
            return self.dct3
        except Exception as e:
            print('%s + parse3 + %s' % (e, response.url))

    def save(self, item):
        keys = ','.join(item.keys())
        values = ','.join(['%s']*len(item))
        sql = 'insert into {table}({keys}) values({values})'.format(table=self.table, keys=keys, values=values)
        try:
            if cursor.execute(sql, tuple(item.values())):
                print('Successful')
                db.commit()
        except Exception as e:
            print('Failed')
            print(e)
            db.rollback()


    def del_with_content(self, content):
        new_content = []
        for i in content:
            i = i.strip()
            if '\u3000' in i:
                i.replace('\u3000', '')
            new_content.append(i)
        content = ','.join(new_content)
        return content

    def main(self, url):
        response2 = News_sina.download(self, url)
        if response2 is not None:
            lst, type = News_sina.parse2(self, response2)
            if lst != []:
                for i in lst:
                    response3 = News_sina.download(self, i)
                    if response3 is not None:
                        news = News_sina.parse3(self, response3, type)
                        # print(news)
                        News_sina.save(self, news)

    def run(self):
        try:
            response1 = news.download(self.start_url)
            if response1 is not None:
                dct1 = news.parse1(response1)
                if dct1 is not None:
                    all_links = list(dct1.keys())
                    pool = Pool()
                    pool.map(news.main, all_links)
                    pool.close()
                    pool.join()
        except Exception as e:
            print(e)
        finally:
            db.close()

if __name__ == '__main__':
    start = time.time()
    news = News_sina()
    news.run()
    end = time.time()
    print('Total Spend Time: %d s' % (end-start))
