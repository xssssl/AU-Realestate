#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Spidering is a class, and it contain some methods that are coroutine.
Class spidering is able to launch serval coroutines to reduce the time consumption when crawling pages.
The main functions of the instance of spidering is to fetch a web page and then parse it.
'''

import aiohttp
import asyncio
from bs4 import BeautifulSoup
import random
import json
import time
import reporting
import sys


url_root = u'https://postcodez.com.au/postcode/'
headers_base = {'Host':'postcodez.com.au',
                'Connection':'keep - alive',
                'Accept-Language':'zh-CN,zh;q=0.8'}
headers_referer_base = 'https://postcodez.com.au/postcodes.cgi?search_suburb=%s&search_state=&type=search'
headers_useragent_pool = [  {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:52.0) Gecko/20100101 Firefox/52.0'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'},\
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0'},\
                            {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv,2.0.1) Gecko/20100101 Firefox/4.0.1'},\
                            {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)'},\
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)'},\
                            {'User-Agent':'Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5'},\
                            {'User-Agent':'Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5'},\
                            {'User-Agent':'Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10'},\
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'},\
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)'},\
                            {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
                            {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'},\
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER'},\
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E) '},\
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)'},\
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)'},\
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre'},\
                            {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0'},\
                            {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'},\
                            {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16'},\
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)'},\
                            {'User-Agent':'Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124'},\
                            {'User-Agent':'Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5'},\
                            {'User-Agent':'Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5'},\
                            {'User-Agent':'Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5'},\
                            {'User-Agent':'Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1'},\
                            {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
                            {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
                            {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
                            {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
                            {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
                            {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
                            {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
                            {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
                            {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
                            {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'},\
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)'},\
                            {'User-Agent':'Mozilla/5.0 (Linux; Android 6.0.1; SM-G920V Build/MMB29K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.98 Mobile Safari/537.36'},\
                            {'User-Agent':'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'},\
                            {'User-Agent':'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36'},\
                            {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9'},\
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'},\
                            {'User-Agent':'Mozislla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1'},\
                            {'User-Agent':'Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 950) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Mobile Safari/537.36 Edge/13.10586'},\
                            {'User-Agent':'Mozilla/5.0 (Linux; Android 5.0.2; SAMSUNG SM-T550 Build/LRX22G) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/3.3 Chrome/38.0.2125.102 Safari/537.36'}]
QUEUE_SIZE = 128
EXCEPTION_PATH = './Exceptions.txt'

class Location(object):
    __slots__ = ('id', 'postcode', 'state', 'suburb', 'region', 'sub_region', 'phone_area', 'latitude', 'longitude')
    def data_clean(self):
        self.id = ''
        self.postcode = ''
        self.state = ''
        self.suburb = ''
        self.region = ''
        self.sub_region = ''
        self.phone_area = ''
        self.latitude = ''
        self.longitude = ''
    def __init__(self):
        self.data_clean()

    def location2dict(self):
        return {'id':self.id,
                'postcode':self.postcode,
                'state':self.state,
                'suburb':self.suburb,
                'region':self.region,
                'sub_region':self.sub_region,
                'phone_area':self.phone_area,
                'latitude':self.latitude,
                'longitude':self.longitude}

    def dict2location(self, d):
        self.id = d['id']
        self.postcode = d['postcode']
        self.state = d['state']
        self.suburb = d['suburb']
        self.region = d['region']
        self.sub_region = d['sub_region']
        self.phone_area = d['phone_area']
        self.latitude = d['latitude']
        self.longitude = d['longitude']
        return self

# place this sentence here mainly because scheduler import Location, and if placing it at the begining, there will be a error
from scheduler import Scheduler

class Spider(object):
    '''
    It manages two queues: task_queue and result_queue. It fetch information from database and put in task_queue.
    Every coroutine will get task from task_queue and do its work and then put results in result_queue.
    '''
    def __init__(self, max_tries, max_tasks, timeout):
        global url_root, headers_base, headers_referer_base, headers_useragent_pool, QUEUE_SIZE
        self.max_tries = max_tries
        self.max_tasks = max_tasks
        self.timeout = timeout
        self.task_queue = asyncio.Queue(QUEUE_SIZE)
        self.result_queue = asyncio.Queue(QUEUE_SIZE)
        self.url_root = url_root
        self.headers_base = headers_base.copy()
        self.headers_referer_base = headers_referer_base
        self.headers_useragent_pool = headers_useragent_pool.copy()
        # If just use a assignment like hds = self.headers_base,
        # the latter give its pointer to the former so that when you make a change to hds, headers_base will change synchronously
        #self.hds = self.headers_base.copy()
        #self.location = Location()
        self.loop = asyncio.get_event_loop()
        self.scheduler = Scheduler(self.task_queue, self.result_queue, 1)
        self.start_time = time.time()
        self.end_time = None
        self.report = reporting.Report()
        self.done = 0
        self.failed = 0

        with open(EXCEPTION_PATH, 'a') as f:
            f.write('***************************************************************\n')
            f.write('****  %s\n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.start_time)))
            f.write('***************************************************************\n')
            #self.data_clean()

    # Close resources
    def close(self):
        for w in self.workers:
            w.cancel()
        self.end_time = time.time()
        with open(EXCEPTION_PATH, 'a') as f:
            self.report.summary(self, f)

    # generate url and header
    def req_gen(self, location):
        #print(sys._getframe().f_code.co_name)
        url = self.url_root + location.state.lower() + '/' + location.suburb.replace(' ', '-').lower()
        # add user agent to the header
        hds = self.headers_base.copy()
        hds.update(self.headers_useragent_pool[random.randint(0, len(self.headers_useragent_pool) - 1)])
        # add referer to the header
        ref_list = [location.postcode, location.suburb.upper().replace(' ', '+')]
        hds.update({'Referer': self.headers_referer_base % ref_list[random.randint(0, len(ref_list)-1)]})
        return url, hds

    # parse a page and add informaion to the instance of class Location. Then put the instance to the result queue.
    async def page_parse(self, response, location):
        source_code = await response.read()
        soup = BeautifulSoup(source_code, 'lxml')
        info = soup.find_all('div', {'class': 'small-6 large-7 columns'})
        # the page contain valid information
        if len(info) > 10:
            location.phone_area = info[2].get_text()
            location.latitude = info[3].get_text()
            location.longitude = info[4].get_text()
            location.sub_region = info[5].get_text()
            location.region = info[6].get_text()
            try:
                self.result_queue.put_nowait(json.dumps(location, default=Location.location2dict))
                print ('[*]Crawl a page successfully: id=%s, postcode=%s, suburb=%s, state=%s' % (location.id,
                                                                                                  location.postcode,
                                                                                                  location.suburb,
                                                                                                  location.state))
                self.done += 1
            except asyncio.queues.QueueFull as e:
                print ('[!]Error: ', e)
                self.failed += 1
        # the page don't contain valid information
        else:
            print ('[*]No valid information is found for id=%s, postcode=%s, suburb=%s, state=%s' % (location.id,
                                                                                                     location.postcode,
                                                                                                     location.suburb,
                                                                                                     location.state))
            with open(EXCEPTION_PATH, 'a') as f:
                self.report.write_exception('NO_VALID_INFORMATION', json.dumps(location, default=Location.location2dict), f)
            self.failed += 1


    # fetch a url
    async def fetch(self, url, header, location):
        tries = 0
        while tries < self.max_tries:
            tries += 1
            try:
                async with aiohttp.ClientSession(loop = self.loop) as self.session:
                    with aiohttp.Timeout(self.timeout, loop = self.session.loop):
                        async with self.session.get(url, headers=header) as resp:
                            await self.page_parse(resp, location)
                            break
            except:
                # to deal with exceptions like timeout and urlerror
                print ('[*]Try %s time(s) for id=%s, postcode=%s, suburb=%s, state=%s' % (tries,
                                                                                          location.id,
                                                                                          location.postcode,
                                                                                          location.suburb,
                                                                                          location.state))
            finally:
                pass
        else:
            # write information to a txt
            print ('[*]Can\'t crawl the page: id=%s, postcode=%s, suburb=%s, state=%s' % (location.id,
                                                                                          location.postcode,
                                                                                          location.suburb,
                                                                                          location.state))
            with open(EXCEPTION_PATH, 'a') as f:
                self.report.write_exception('CANNOT_CRAWL', json.dumps(location, default=Location.location2dict), f)
            self.failed += 1

    # Process queue items forever
    async def work(self):
        location = Location()
        try:
            while True:
                try:
                    lo = await self.task_queue.get()
                    location = json.loads(lo, object_hook=location.dict2location)
                except asyncio.queues.QueueEmpty as e:
                    print ('[!]Error: ', e)
                except ValueError as e:
                    print ('[!]Error: ', e)
                url, header = self.req_gen(location)
                await self.fetch(url, header, location)
                location.data_clean()
                self.task_queue.task_done()
        except asyncio.CancelledError as e:
            print ('[*]An coroutine has been cancelled.', e)
        except:
            print ('[!]Error: Stopped unexpectedly')
            self.close()


    # the main of the coroutine
    async def crawl(self):
        self.workers = [asyncio.ensure_future(self.work(), loop = self.loop)
                   for _ in range(self.max_tasks)]
        while True:
            # read information from database and put them in task_queue
            # if all the work have been assigned then the loop will break
            if self.scheduler.assign_work() == 'done':
                break
            # wait all the tasks in task_queue are done
            await self.task_queue.join()
            if self.scheduler.rev_result() == 'abondon':
                break

        # terminate all the coroutines
        self.close()


