#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Spidering is a class, and it contain some methods that are coroutine.
Class spidering is able to launch serval coroutines to reduce the time consumption when crawling pages.
The main functions of the instance of spidering is to fetch a web page and then parse it.
Specifically, the diverse functions of the spider is divided with an unique task ID.
Task 1: fetch the information  of median price of different suburbs
Task 2: fetch the position of suburbs which is a json and is actually included in the 'task 1' pages using ajax

'''

__author__ = 'zht'

import aiohttp
import asyncio
from bs4 import BeautifulSoup
import random
import json
import time
import reporting
from enum import Enum
import re
import sys
import resuming

'''
url_root = {'1': u'https://www.realestate.com.au/neighbourhoods/',
            '2': u'https://investor-api.realestate.com.au/states/%s/suburbs/%s/polygons.json'}
headers_base = {'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'accept-encoding':'gzip, deflate, sdch, br',
                'upgrade-insecure-requests':'1',
                'Accept-Language':'zh-CN,zh;q=0.8'}
headers_referer_base = {'1': u'https://www.realestate.com.au/neighbourhoods/'}
'''
headers_useragent_pool = [  {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:52.0) Gecko/20100101 Firefox/52.0'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'},
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0'},
                            {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv,2.0.1) Gecko/20100101 Firefox/4.0.1'},
                            {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)'},
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)'},
                            {'User-Agent':'Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5'},
                            {'User-Agent':'Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5'},
                            {'User-Agent':'Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10'},
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'},
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)'},
                            {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},
                            {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER'},
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER'},
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E) '},
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)'},
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)'},
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre'},
                            {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0'},
                            {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11'},
                            {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16'},
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)'},
                            {'User-Agent':'Mozilla/5.0 (SymbianOS/9.4; Series60/5.0 NokiaN97-1/20.0.019; Profile/MIDP-2.1 Configuration/CLDC-1.1) AppleWebKit/525 (KHTML, like Gecko) BrowserNG/7.1.18124'},
                            {'User-Agent':'Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5'},
                            {'User-Agent':'Mozilla/5.0 (iPod; U; CPU iPhone OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5'},
                            {'User-Agent':'Mozilla/5.0 (iPad; U; CPU OS 4_3_3 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8J2 Safari/6533.18.5'},
                            {'User-Agent':'Mozilla/5.0 (Linux; U; Android 2.3.7; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1'},
                            {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},
                            {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},
                            {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},
                            {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},
                            {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
                            {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},
                            {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},
                            {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},
                            {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},
                            {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},
                            {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'},
                            {'User-Agent':'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)'},
                            {'User-Agent':'Mozilla/5.0 (Linux; Android 6.0.1; SM-G920V Build/MMB29K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.98 Mobile Safari/537.36'},
                            {'User-Agent':'Mozilla/5.0 (Linux; Android 5.1.1; SM-G928X Build/LMY47X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.83 Mobile Safari/537.36'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246'},
                            {'User-Agent':'Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36'},
                            {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9'},
                            {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36'},
                            {'User-Agent':'Mozislla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1'},
                            {'User-Agent':'Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 950) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Mobile Safari/537.36 Edge/13.10586'},
                            {'User-Agent':'Mozilla/5.0 (Linux; Android 5.0.2; SAMSUNG SM-T550 Build/LRX22G) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/3.3 Chrome/38.0.2125.102 Safari/537.36'}]
QUEUE_SIZE = 2
EXCEPTION_PATH = './Exceptions.txt'
RESUMING_PATH = './Resume.bin'

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

class Medi_price_sub(object):
    __slots__ = ('id', 'postcodes_id',
                 'house_buy', 'house_buy_update_date', 'house_buy_2br', 'house_buy_3br', 'house_buy_4br',
                 'house_rent', 'house_rent_update_date', 'house_rent_2br', 'house_rent_3br', 'house_rent_4br',
                 'unit_buy', 'unit_buy_update_date', 'unit_buy_2br', 'unit_buy_3br', 'unit_buy_4br',
                 'unit_rent', 'unit_rent_update_date', 'unit_rent_2br', 'unit_rent_3br', 'unit_rent_4br',
                 'trend', 'position')

    def data_clean(self):
        self.id = ''
        self.postcodes_id = ''
        self.house_buy = ''
        self.house_buy_update_date = ''
        self.house_buy_2br = ''
        self.house_buy_3br = ''
        self.house_buy_4br = ''
        self.house_rent = ''
        self.house_rent_update_date = ''
        self.house_rent_2br = ''
        self.house_rent_3br = ''
        self.house_rent_4br = ''
        self.unit_buy = ''
        self.unit_buy_update_date = ''
        self.unit_buy_2br = ''
        self.unit_buy_3br = ''
        self.unit_buy_4br = ''
        self.unit_rent = ''
        self.unit_rent_update_date = ''
        self.unit_rent_2br = ''
        self.unit_rent_3br = ''
        self.unit_rent_4br = ''
        self.trend = ''
        self.position = ''

    def __init__(self):
        self.data_clean()

    def mediprice2dict(self):
        return {'id': self.id,
                'postcodes_id': self.postcodes_id,
                'house_buy': self.house_buy,
                'house_buy_update_date': self.house_buy_update_date,
                'house_buy_2br': self.house_buy_2br,
                'house_buy_3br': self.house_buy_3br,
                'house_buy_4br': self.house_buy_4br,
                'house_rent': self.house_rent,
                'house_rent_update_date': self.house_rent_update_date,
                'house_rent_2br': self.house_rent_2br,
                'house_rent_3br': self.house_rent_3br,
                'house_rent_4br': self.house_rent_4br,
                'unit_buy': self.house_rent,
                'unit_buy_update_date': self.unit_buy_update_date,
                'unit_buy_2br': self.unit_buy_2br,
                'unit_buy_3br': self.unit_buy_3br,
                'unit_buy_4br': self.unit_buy_4br,
                'unit_rent': self.unit_rent,
                'unit_rent_update_date': self.unit_rent_update_date,
                'unit_rent_2br': self.unit_rent_2br,
                'unit_rent_3br': self.unit_rent_3br,
                'unit_rent_4br': self.unit_rent_4br,
                'trend': self.trend,
                'position': self.position}

    def dict2mediprice(self, d):
        self.id = d['id']
        self.postcodes_id = d['postcodes_id']
        self.house_buy = d['house_buy']
        self.house_buy_update_date = d['house_buy_update_date']
        self.house_buy_2br = d['house_buy_2br']
        self.house_buy_3br = d['house_buy_3br']
        self.house_buy_4br = d['house_buy_4br']
        self.house_rent = d['house_rent']
        self.house_rent_update_date = d['house_rent_update_date']
        self.house_rent_2br = d['house_rent_2br']
        self.house_rent_3br = d['house_rent_3br']
        self.house_rent_4br = d['house_rent_4br']
        self.unit_buy = d['unit_buy']
        self.unit_buy_update_date = d['unit_buy_update_date']
        self.unit_buy_2br = d['unit_buy_2br']
        self.unit_buy_3br = d['unit_buy_3br']
        self.unit_buy_4br = d['unit_buy_4br']
        self.unit_rent = d['unit_rent']
        self.unit_rent_update_date = d['unit_rent_update_date']
        self.unit_rent_2br = d['unit_rent_2br']
        self.unit_rent_3br = d['unit_rent_3br']
        self.unit_rent_4br = d['unit_rent_4br']
        self.trend = d['trend']
        self.position = d['position']
        return self

class Switch(object):
    '''
    This class provides the functionality we want. You only need to look at
    this if you want to know how this works. It only needs to be defined
    once, no need to muck around with its internals.
    The following example is pretty much the exact use-case of a dictionary,
    but is included for its simplicity. Note that you can include statements
    in each suite.
    v = 'ten'
    for case in switch(v):
        if case('one'):
            print 1
            break
        if case('two'):
            print 2
            break
        if case('ten'):
            print 10
            break
        if case('eleven'):
            print 11
            break
        if case(): # default, could also just omit condition or 'if True'
            print "something else!"
            # No need to break here, it'll stop anyway
    '''
    def __init__(self, value):
        self.value = value
        self.fall = False

    def __iter__(self):
        """Return the match method once, then stop"""
        yield self.match
        raise StopIteration

    def match(self, *args):
        """Indicate whether or not to enter a case suite"""
        if self.fall or not args:
            return True
        elif self.value in args:  # changed for v1.5, see below
            self.fall = True
            return True
        else:
            return False


# place this sentence here mainly because scheduler import Location, and if placing it at the begining, there will be a error
from scheduler import Scheduler

class Spider(object):
    '''
    It manages two queues: task_queue and result_queue. It fetch information from database and put in task_queue.
    Every coroutine will get task from task_queue and do its work and then put results in result_queue.
    '''

    def __init__(self, task_id, max_tries, max_tasks, check_exceptions, timeout):
        global url_root, headers_base, headers_referer_base, headers_useragent_pool, QUEUE_SIZE
        self.task_id = task_id
        self.max_tries = max_tries
        self.max_tasks = max_tasks
        self.check_exceptions = check_exceptions
        self.timeout = timeout
        self.task_queue = asyncio.Queue(QUEUE_SIZE)
        self.result_queue = asyncio.Queue(QUEUE_SIZE)
        # If just use a assignment like hds = self.headers_base,
        # the latter give its pointer to the former so that when you make a change to hds, headers_base will change synchronously
        #self.url_root = url_root.copy()
        #self.headers_base = headers_base.copy()
        #self.headers_referer_base = headers_referer_base
        self.headers_useragent_pool = headers_useragent_pool.copy()
        self.loop = asyncio.get_event_loop()
        self.scheduler = Scheduler(self.task_id, self.task_queue, self.result_queue, 1)
        self.start_time = time.time()
        self.end_time = None
        self.time_consumption = 0
        self.report = reporting.Report()
        self.done = 0
        self.failed = 0
        self.status = 0         # normal task =0, exception=1, it's a flag to indicate which process the instance is in
        self.resume = resuming.Resume()

        # check if the user want to resume
        try:
            with open(RESUMING_PATH, 'rb') as f:
                r = self.resume.verify(f, object_hook=self.dict2spider, **self.spider2dict())
                if r != None:
                    self = r
        except FileNotFoundError as e:
            print('[*]No resuming file is found.')
        with open(EXCEPTION_PATH, 'a') as f:
            if self.done == 0 and self.failed == 0:
                f.write('\n***************************************************************\n')
                f.write('****  %s\n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.start_time)))
                f.write('***************************************************************\n')
            else:
                f.write('    ----------------------  RESUME  -----------------------\n')
                f.write('    ---------------  %s  -----------------\n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.start_time)))

    # transform some attributes of Class Spider into a dict
    def spider2dict(self):
        r = {'task_id': self.task_id,
                'max_tries': self.max_tries,
                'max_tasks': self.max_tasks,
                'check_exceptions': self.check_exceptions,
                'timeout': self.timeout,
                'time_consumption': self.time_consumption,
                'done': self.done,
                'failed': self.failed,
                'status': self.status}
        r.update(self.scheduler.scheduler2dict())
        return r

    # transform a dict back into Class Spider
    def dict2spider(self, d):
        self.task_id = d['task_id']
        self.max_tries = d['max_tries']
        self.max_tasks = d['max_tasks']
        self.check_exceptions = d['check_exceptions']
        self.timeout = d['timeout']
        self.time_consumption = d['time_consumption']
        self.done = d['done']
        self.failed = d['failed']
        self.status = d['status']
        self.scheduler = self.scheduler.dict2scheduler(d)
        return self

    # Close resources
    def close(self):
        for w in self.workers:
            w.cancel()
        self.end_time = time.time()
        with open(EXCEPTION_PATH, 'a') as f:
            self.report.summary(self, f)
        #self.session.close()       # U don't need this, as this work has been done by with...as in fetch().


    # generate url and header
    def req_gen(self, **kw):
        #print(sys._getframe().f_code.co_name)
        # simulate switch...case in C language
        # choose different methods according to task_id
        for case in Switch(self.task_id):
            # crawl median price of suburbs
            if case(1):
                url_root = [u'https://www.realestate.com.au/neighbourhoods/', ]
                headers_base = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                                #'accept-encoding': 'gzip, deflate, sdch, br',
                                'connection': 'keep - alive',
                                'upgrade-insecure-requests': '1',
                                'accept-Language': 'zh-CN,zh;q=0.8',
                                'referer': 'https://www.realestate.com.au/neighbourhoods/'}
                # generate url
                url = url_root[0] + kw['location'].suburb.replace(' ', '%20').lower() + '-' + kw['location'].postcode + '-' + kw['location'].state.lower()
                # generate header
                headers_base.update(self.headers_useragent_pool[random.randint(0, len(self.headers_useragent_pool) - 1)])
                hds = headers_base  # just assign the pointer
                break

            # crawl positon of suburbs, actually they are ploygens with latitude and longitude
            if case(2):
                url_root = [u'https://investor-api.realestate.com.au/states/%s/suburbs/%s/polygons.json', ]
                headers_base = {'accept': '*/*',
                                'origin': 'https://www.realestate.com.au'}
                referer_root = [u'https://www.realestate.com.au/neighbourhoods/', ]
                # generate url
                url = url_root[0] % (kw['location'].state.lower(), kw['location'].suburb.replace(' ', '%20').lower())
                # generate referer
                referer = referer_root[0] + kw['location'].suburb.replace(' ', '%20').lower() + '-' + kw['location'].postcode + '-' + kw['location'].state.lower()
                headers_base['referer'] = referer
                headers_base.update(self.headers_useragent_pool[random.randint(0, len(self.headers_useragent_pool) - 1)])
                hds = headers_base
                break

            # default
            if case():
                print ('[!]Error: invalid task id')
        return url, hds

    # dealing with median price of suburbs and put the result to result queue
    # kw must have instances of Location and Medi_price_sub
    async def median_price_parse(self, response, **kw):
        #print(sys._getframe().f_code.co_name)
        source_code = await response.read()
        lackofinfo = 0
        location = kw.get('location')
        medi_price_sub = kw.get('medi_price_sub')
        soup = BeautifulSoup(source_code, 'lxml')


        # wrap some work with find_all, mainly for replacing invalid information
        def info_find(info, name, attrs, num):
            results = info.find_all(name, attrs)
            # to verify whether the amount of information that been found is correct
            if len(results) == num:
                for i in range(num):
                    r = results[i].get_text().replace('$', '').replace(',', '').replace(' PW', '')
                    if r == '-' or r == 'no data':
                        results[i] = None
                    else:
                        results[i] = r
                #return results
            else:
                results = []
                for _ in range(num):
                    results.append(None)
            return results

        # mainly for dealing with the date format to meet the requirements of MySQL
        Month = Enum('Month', ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'))
        def str2date(string):
            strformat = re.compile(r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s(\d+)(st|nd|rd|th)\s(\d{4})$')
            date_origin = strformat.match(string).groups()
            if len(date_origin) == 4:
                mm = str(getattr(Month, date_origin[0]).value).zfill(2)
                dd = date_origin[1].zfill(2)
                date = date_origin[3] + '-' + mm + '-' + dd
            else:
                print ('[!]Error: function \'str2date\' receive invalid params')
                raise ValueError
            return date

        # house
        info = soup.find_all('div', {'class': 'slide-section default-slide-section median-price-subsections houses'})
        if len(info) == 1:
            info_buy = info[0].find_all('div', {'class': 'median-price-subsection buy-subsection'})
            if len(info_buy) == 1:
                result = info_find(info_buy[0], 'div', {'class': 'price h1 strong'}, 1)
                medi_price_sub.house_buy = result[0]
                result = info_find(info_buy[0], 'div', {'class': 'price strong'}, 3)
                medi_price_sub.house_buy_2br = result[0]
                medi_price_sub.house_buy_3br = result[1]
                medi_price_sub.house_buy_4br = result[2]
                result = info_find(info_buy[0], 'span', {'class': 'strong'}, 1)
                if result[0] != None:
                    result[0] = str2date(result[0])
                medi_price_sub.house_buy_update_date = result[0]
            else:
                lackofinfo += 1
                print('[*]No valid information: house_buy: %s, %s, %s' % (location.postcode, location.state, location.suburb))

            info_rent = info[0].find_all('div', {'class': 'median-price-subsection rent-subsection'})
            if len(info_rent) == 1:
                result = info_find(info_rent[0], 'div', {'class': 'price h1 strong'}, 1)
                medi_price_sub.house_rent = result[0]
                result = info_find(info_rent[0], 'div', {'class': 'price strong'}, 3)
                medi_price_sub.house_rent_2br = result[0]
                medi_price_sub.house_rent_3br = result[1]
                medi_price_sub.house_rent_4br = result[2]
                result = info_find(info_rent[0], 'span', {'class': 'strong'}, 1)
                if result[0] != None:
                    result[0] = str2date(result[0])
                medi_price_sub.house_rent_update_date = result[0]
            else:
                print('[*]No valid information: house_rent: %s, %s, %s' % (location.postcode, location.state, location.suburb))
                lackofinfo += 1
        else:
            print('[*]No valid information: house: %s, %s, %s' % (location.postcode, location.state, location.suburb))
            lackofinfo += 2

        # units
        info = soup.find_all('div', {'class': 'slide-section median-price-subsections units'})
        if len(info) == 1:
            info_buy = info[0].find_all('div', {'class': 'median-price-subsection buy-subsection'})
            if len(info_buy) == 1:
                result = info_find(info_buy[0], 'div', {'class': 'price h1 strong'}, 1)
                medi_price_sub.unit_buy = result[0]
                result = info_find(info_buy[0], 'div', {'class': 'price strong'}, 3)
                medi_price_sub.unit_buy_2br = result[0]
                medi_price_sub.unit_buy_3br = result[1]
                medi_price_sub.unit_buy_4br = result[2]
                result = info_find(info_buy[0], 'span', {'class': 'strong'}, 1)
                if result[0] != None:
                    result[0] = str2date(result[0])
                medi_price_sub.unit_buy_update_date = result[0]
            else:
                print('[*]No valid information: unit_buy: %s, %s, %s' % (location.postcode, location.state, location.suburb))
                lackofinfo += 1

            info_rent = info[0].find_all('div', {'class': 'median-price-subsection rent-subsection'})
            if len(info_rent) == 1:
                result = info_find(info_rent[0], 'div', {'class': 'price h1 strong'}, 1)
                medi_price_sub.unit_rent = result[0]
                result = info_find(info_rent[0], 'div', {'class': 'price strong'}, 3)
                medi_price_sub.unit_rent_2br = result[0]
                medi_price_sub.unit_rent_3br = result[1]
                medi_price_sub.unit_rent_4br = result[2]
                result = info_find(info_rent[0], 'span', {'class': 'strong'}, 1)
                if result[0] != None:
                    result[0] = str2date(result[0])
                medi_price_sub.unit_rent_update_date = result[0]
            else:
                print('[*]No valid information: unit_rent: %s, %s, %s' % (location.postcode, location.state, location.suburb))
                lackofinfo += 1

        else:
            print('[*]No valid information: unit: %s, %s, %s' % (location.postcode, location.state, location.suburb))
            lackofinfo += 2

        # trend
        info = soup.find_all('div', {'class': 'slide-section median-price-subsections trend'})
        if len(info) == 1:
            info_trend = info[0].attrs['data-trend']
            medi_price_sub.trend = info_trend
        else:
            medi_price_sub.trend = None
            print('[*]No valid information: trend: %s, %s, %s' % (location.postcode, location.state, location.suburb))
            lackofinfo += 1

        # the page seems to have something interesting
        if lackofinfo < 5:
            # transform medi_price_sub into json first and then put it in the queue.
            # when u do that, u need to transform the medi_price_sub's instance to dict by method 'mediprice2dict'
            # if put orgin instance into queue, you actually put a pointer into the queue
            # put the result in the queue without blocking
            try:
                medi_price_sub.postcodes_id = location.id
                self.result_queue.put_nowait(json.dumps(medi_price_sub, default=Medi_price_sub.mediprice2dict))
                print('[*]Crawl a page successfully: id=%s, postcode=%s, suburb=%s, state=%s' % (location.id,
                                                                                                 location.postcode,
                                                                                                 location.suburb,
                                                                                                 location.state))
                self.done += 1
            except asyncio.queues.QueueFull as e:
                print('[!]Error: ', e)
                self.failed += 1

        # the page don't contain any valid information
        else:
            print('[*]No any valid information is found for id=%s, postcode=%s, suburb=%s, state=%s' % (location.id,
                                                                                                    location.postcode,
                                                                                                    location.suburb,
                                                                                                    location.state))
            with open(EXCEPTION_PATH, 'a') as f:
                self.report.write_exception('TASK %s: NO_VALID_INFORMATION' % self.task_id,
                                            'id=%s, postcode=%s, suburb=%s, state=%s'
                                                % (location.id, location.postcode, location.suburb, location.state),
                                            f)
            self.failed += 1

    async def suburb_position_parse(self, response, **kw):
        # print(sys._getframe().f_code.co_name)
        source_code = await response.read()
        location = kw.get('location')
        medi_price_sub = kw.get('medi_price_sub')
        soup = BeautifulSoup(source_code, 'lxml')
        info = soup.p.text

        if len(info) > 10:
            try:
                medi_price_sub.postcodes_id = location.id
                medi_price_sub.position = info
                self.result_queue.put_nowait(json.dumps(medi_price_sub, default=Medi_price_sub.mediprice2dict))
                print('[*]Crawl a page successfully: id=%s, postcode=%s, suburb=%s, state=%s' % (location.id,
                                                                                                     location.postcode,
                                                                                                     location.suburb,
                                                                                                     location.state))
                self.done += 1
            except asyncio.queues.QueueFull as e:
                print('[!]Error: ', e)
                self.failed += 1
        # the page don't contain any valid information
        else:
            print('[*]No any valid information is found for id=%s, postcode=%s, suburb=%s, state=%s' % (location.id,
                                                                                                        location.postcode,
                                                                                                        location.suburb,
                                                                                                        location.state))
            with open(EXCEPTION_PATH, 'a') as f:
                self.report.write_exception('TASK %s: NO_VALID_INFORMATION' % self.task_id,
                                            'id=%s, postcode=%s, suburb=%s, state=%s'
                                            % (location.id, location.postcode, location.suburb, location.state),
                                            f)
            self.failed += 1

    # parse a page
    # as parsing a page is sometimes a little of complicated, so put the code in separate functions with task id
    async def page_parse(self, response, **kw):
        #print(sys._getframe().f_code.co_name)
        for case in Switch(self.task_id):
            if case(1):
                await self.median_price_parse(response, **kw)
                break
            if case(2):
                await self.suburb_position_parse(response, **kw)
                break
            if case():
                print ('[!]Error: invalid task id')


    # fetch a url
    async def fetch(self, url, header, **kw):
        #print(sys._getframe().f_code.co_name)
        tries = 0
        while tries < self.max_tries:
            tries += 1
            try:
                async with aiohttp.ClientSession(loop = self.loop) as self.session:
                    with aiohttp.Timeout(self.timeout, loop = self.session.loop):
                        async with self.session.get(url, headers=header) as resp:
                            await self.page_parse(resp, **kw)
                            break
            except:
                # to deal with exceptions like timeout and urlerror
                if tries > 1:
                    print ('[*]Try %s time(s) for url: %s' % (tries, url))
            finally:
                pass
        else:
            # write information to a txt
            print ('[*]Can\'t crawl the page: %s' % url)
            with open(EXCEPTION_PATH, 'a') as f:
                for case in Switch(self.task_id):
                    if case(1):
                        self.report.write_exception('TASK %s: CANNOT_CRAWL' % self.task_id,
                                                    'id=%s, postcode=%s, suburb=%s, state=%s'
                                                        % (kw['location'].id, kw['location'].postcode, kw['location'].suburb, kw['location'].state),
                                                    f)
                        break
                    if case(2):
                        self.report.write_exception('TASK %s: CANNOT_CRAWL' % self.task_id,
                                                    'id=%s, postcode=%s, suburb=%s, state=%s'
                                                        % (kw['location'].id, kw['location'].postcode, kw['location'].suburb, kw['location'].state),
                                                    f)
                        break
                    if case():
                        print('[!]Error: invalid task id')
            self.failed += 1

    # Process queue items forever
    async def work(self):
        #print(sys._getframe().f_code.co_name)
        try:
            for case in Switch(self.task_id):
                if case(1):
                    location = Location()
                    medi_price_sub = Medi_price_sub()
                    while True:
                        try:
                            #lo = self.task_queue.get_nowait()
                            lo = await self.task_queue.get()
                            location = json.loads(lo, object_hook=location.dict2location)
                            # Can't do this here otherwise an error 'Task was destroyed but it is pending' will occur.
                            #self.task_queue.task_done()
                        except asyncio.queues.QueueEmpty as e:
                            print ('[!]Error: ', e)
                        except ValueError as e:
                            print ('[!]Error: ', e)
                        url, header = self.req_gen(location = location)
                        await self.fetch(url, header, location = location, medi_price_sub = medi_price_sub)
                        # must after result_queue.put()
                        location.data_clean()
                        medi_price_sub.data_clean()
                        self.task_queue.task_done()
                    break
                if case(2):
                    location = Location()
                    medi_price_sub = Medi_price_sub()
                    while True:
                        try:
                            #lo = self.task_queue.get_nowait()
                            lo = await self.task_queue.get()
                            location = json.loads(lo, object_hook=location.dict2location)
                            # Can't do this here otherwise an error 'Task was destroyed but it is pending' will occur.
                            #self.task_queue.task_done()
                        except asyncio.queues.QueueEmpty as e:
                            print ('[!]Error: ', e)
                        except ValueError as e:
                            print ('[!]Error: ', e)
                        url, header = self.req_gen(location = location)
                        await self.fetch(url, header, location = location, medi_price_sub = medi_price_sub)
                        # must after result_queue.put()
                        location.data_clean()
                        medi_price_sub.data_clean()
                        self.task_queue.task_done()
                    break
                if case():
                    print ('[!]Error: invalid task id')
        except asyncio.CancelledError as e:
            print ('[*]An coroutine has been cancelled.', e)
        #except:
        #    print ('[!]Error: Stopped unexpectedly')
        #    self.close()


    # the main of the coroutine, you can allocate different tasks to crawl() by giving different task_id
    async def crawl(self):
        """Run the crawler until all finished."""
        self.workers = [asyncio.ensure_future(self.work(), loop = self.loop)
                   for _ in range(self.max_tasks)]
        time_consumption_origin = self.time_consumption
        # normal work
        if self.status == 0:
            while True:
                # read information from database and put them in task_queue
                # if all the work have been assigned then the loop will break
                if self.scheduler.assign_work() == 'done':
                    if self.check_exceptions == 1:
                        self.status = 1
                        with open(RESUMING_PATH, 'wb') as f:
                            self.time_consumption = time_consumption_origin + (time.time() - self.start_time)
                            self.resume.save2file(f, **self.spider2dict())
                    else:
                        with open(RESUMING_PATH, 'wb') as f:
                            self.resume.cleanfile(f)
                    break
                # wait all the tasks in task_queue are done
                await self.task_queue.join()
                if self.scheduler.rev_result() == 'abondon':
                    break
                with open(RESUMING_PATH, 'wb') as f:
                    self.time_consumption = time_consumption_origin + (time.time() - self.start_time)
                    self.resume.save2file(f, **self.spider2dict())
                    #self.resume.save2file(f, **self.scheduler.scheduler2dict())


        # deal with exceptions
        if self.check_exceptions == 1:
            with open(EXCEPTION_PATH, 'a') as f:
                f.write('-------------------------  RE-CRAWL ---------------------------\n')
            with open(EXCEPTION_PATH, 'r') as f:
                lines = self.report.read_exception(f)
            while True:
                if self.scheduler.manage_exceptions(lines) == 'done':
                    with open(RESUMING_PATH, 'wb') as f:
                        self.resume.cleanfile(f)
                    break
                await self.task_queue.join()
                self.scheduler.rev_result()
                with open(RESUMING_PATH, 'wb') as f:
                    self.resume.save2file(f, **self.spider2dict())

        # terminate all the coroutines
        self.close()


