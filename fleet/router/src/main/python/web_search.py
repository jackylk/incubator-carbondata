#coding:utf-8

import os
import sys
import urllib
import json
import socket
import urllib.request
import urllib.parse
import urllib.error
import time

timeout = 5
socket.setdefaulttimeout(timeout)


class Crawler:
    # sleep time
    __time_sleep = 0.1

    # number of url to get
    __num_url = 0

    # counter to start
    __start_counter = 0

    # number of url got
    __counter = 0

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}

    def __init__(self, t=0.1):
        self.time_sleep = t

    def output_image_info(self, rsp_data):
        for image_info in rsp_data['data']:
            try:
                if len(image_info) > 0:
                    print(image_info['middleURL'] + "|" + image_info['fromPageTitleEnc'])
                    self.__counter += 1
                else:
                    continue
            except urllib.error.HTTPError as urllib_err:
                print(urllib_err)
                continue
            except Exception as err:
                time.sleep(1)
                print("exception: " + err)
                continue

    def get_images(self, word):
        search = urllib.parse.quote(word)
        counter = self.__start_counter
        while counter < self.__num_url:

            url = "https://image.baidu.com/search/acjson?tn=resultjson_com&ipn=rj&ct=201326592&is=&fp=result&cl=2&lm=-1&ie=utf-8&oe=utf-8&adpicid=&st=-1&z=&ic=0&word={0}&s=&se=&tab=&width=&height=&face=0&istype=2&qc=&nc=1&fr=&pn={0}&rn=30&gsm=3c&1507915209449=".format(search, counter)

            try:
                time.sleep(self.time_sleep)
                req = urllib.request.Request(url=url, headers=self.headers)
                page = urllib.request.urlopen(req)
                rsp = page.read()
            except UnicodeDecodeError as e:
                print(e)
                print('-----UnicodeDecodeErrorurl:', url)
            except urllib.error.URLError as e:
                print(e)
                print("-----urlErrorurl:", url)
            except socket.timeout as e:
                print(e)
                print("-----socket timout:", url)
            else:
                rsp_data = json.loads(rsp)
                self.output_image_info(rsp_data)
                # read next page
                counter += 60
            finally:
                page.close()
        return

    def start(self, key_word, spider_page_num=1, start_page=1):
        """
        start crawling
        :param key_word: key word to search
        :param spider_page_num: total page num to crawl
        :param start_page: starting page
        :return:
        """
        self.__start_counter = (start_page - 1) * 60
        self.__num_url = spider_page_num * 60 + self.__start_counter
        self.get_images(key_word)


stdout_bin = os.fdopen(sys.stdout.fileno(), 'wb', 4)

if __name__ == '__main__':
    crawler = Crawler(0.05)  # sleep for 0.05s between each crawling
    key_word = sys.argv[1]
    page_num = sys.argv[2]
    crawler.start(key_word, int(page_num), 1)  # every page is 60 entries
