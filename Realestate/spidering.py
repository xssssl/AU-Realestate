#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Spidering is a class, and it contain some methods that are coroutine.
Class spidering is able to launch serval coroutines to reduce the time consumption when crawling pages.
The main functions of the instance of spidering is to fetch a web page and then parse it.
Specifically, the diverse functions of the spider is divided with an unique task ID.
Task 1: fetch the information  of median price of different suburbs
Task 2: fetch the position of suburbs which is a json and is actually included in the 'task 1' pages using ajax
Task 3: fetch sold history of properties by surburbs
Task 4: fetch lastest 50 pages of sold history
Task 5: fetch auction results from realestate.com.au, but actually these data are from corelogic

'''

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
from contextlib import suppress
import math
import datetime
import base64

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
#QUEUE_SIZE = 128
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
                 'trend', 'update_date', 'position')

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
        self.update_date = ''
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
                'update_date': self.update_date,
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
        self.update_date = d['update_date']
        self.position = d['position']
        return self

class Propt_sold_history(object):
    __slot__ = ('id', 'suburb_id', 'full_address', 'property_type', 'price', 'bedrooms', 'bathrooms', 'carspaces',
                'sold_date', 'agent', 'url', 'update_date')

    def data_clean(self):
        self.id = ''
        self.state = ''
        self.postcode = ''
        self.suburb = ''
        self.suburb_id = ''
        self.full_address = ''
        self.property_type = ''
        self.price = ''
        self.bedrooms = ''
        self.bathrooms = ''
        self.carspaces = ''
        self.sold_date = ''
        self.agent = ''
        self.url = ''
        self.update_date = ''

    def __init__(self):
        self.data_clean()

    def soldhistory2dict(self):
        return {'id': self.id,
                'state': self.state,
                'postcode': self.postcode,
                'suburb': self.suburb,
                'suburb_id': self.suburb_id,
                'full_address': self.full_address,
                'property_type': self.property_type,
                'price': self.price,
                'bedrooms': self.bedrooms,
                'bathrooms': self.bathrooms,
                'carspaces': self.carspaces,
                'sold_date': self.sold_date,
                'agent': self.agent,
                'url': self.url,
                'update_date': self.update_date}

    def dict2soldhistory(self, d):
        self.id = d['id']
        self.state = d['state']
        self.postcode = d['postcode']
        self.suburb = d['suburb']
        self.suburb_id = d['suburb_id']
        self.full_address = d['full_address']
        self.property_type = d['property_type']
        self.price = d['price']
        self.bedrooms = d['bedrooms']
        self.bathrooms = d['bathrooms']
        self.carspaces = d['carspaces']
        self.sold_date = d['sold_date']
        self.agent = d['agent']
        self.url = d['url']
        self.update_date = d['update_date']
        return self

class Auction_result(object):
    __slot__ = ('id', 'state', 'scheduled_auctions', 'results_available', 'clearance_rate', 'sold_prior_to_auction',
                'sold_at_auction', 'sold_after_auction', 'withdrawn', 'passed_in', 'auction_date', 'update_date')

    def data_clean(self):
        self.id = ''
        self.state = ''
        self.scheduled_auctions = ''
        self.results_available = ''
        self.clearance_rate = ''
        self.sold_prior_to_auction = ''
        self.sold_at_auction = ''
        self.sold_after_auction = ''
        self.withdrawn = ''
        self.passed_in = ''
        self.auction_date = ''
        self.update_date = ''

    def __init__(self):
        self.data_clean()

    def auction2dict(self):
        return {'id': self.id,
                'state': self.state,
                'scheduled_auctions': self.scheduled_auctions,
                'results_available': self.results_available,
                'clearance_rate': self.clearance_rate,
                'sold_prior_to_auction': self.sold_prior_to_auction,
                'sold_at_auction': self.sold_at_auction,
                'sold_after_auction': self.sold_after_auction,
                'withdrawn': self.withdrawn,
                'passed_in': self.passed_in,
                'auction_date': self.auction_date,
                'update_date': self.update_date}

    def dict2auction(self, d):
        self.id = d['id']
        self.state = d['state']
        self.scheduled_auctions = d['scheduled_auctions']
        self.results_available = d['results_available']
        self.clearance_rate = d['clearance_rate']
        self.sold_prior_to_auction = d['sold_prior_to_auction']
        self.sold_at_auction = d['sold_at_auction']
        self.sold_after_auction = d['sold_after_auction']
        self.withdrawn = d['withdrawn']
        self.passed_in = d['passed_in']
        self.auction_date = d['auction_date']
        self.update_date = d['update_date']
        return self


class Switch(object):
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

    def __init__(self, task_id, max_tries, max_tasks, check_exceptions, timeout, queue_size):
        global url_root, headers_base, headers_referer_base, headers_useragent_pool
        self.task_id = task_id
        self.max_tries = max_tries
        self.max_tasks = max_tasks
        self.check_exceptions = check_exceptions
        self.timeout = timeout
        self.task_queue = asyncio.Queue(queue_size)
        self.result_queue = asyncio.Queue(queue_size)
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
        self.list_buffer = []   # it's a buffer in list, u can store anything temporarily here
        self.dict_buffer = {}   # it's a dict in list, u can store anything temporarily here

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

    def wait_cancelled(self):
        print('[*]Wait be cancelled...')
        for w in self.workers:
            with suppress(asyncio.CancelledError):
                self.loop.run_until_complete(w)

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

            # crawl sold history of properties
            if case(3):
                def gen_url(url_root, **kw):
                    if len(kw['url_togo']) > 0:
                        url = url_root % (kw['location'].suburb.replace(' ', '+').lower(), kw['location'].state.lower(),
                                          kw['location'].postcode, kw['url_togo'][0]['current_list'])
                    else:
                        url = url_root % (kw['location'].suburb.replace(' ', '+').lower(), kw['location'].state.lower(),
                                          kw['location'].postcode, 1)
                    return url
                def gen_url_filter_price(url_root_filter_price, **kw):
                    url = url_root_filter_price % (kw['url_togo'][0]['price_from'], kw['url_togo'][0]['price_to'],
                                             kw['location'].suburb.replace(' ', '+').lower(),
                                             kw['location'].state.lower(), kw['location'].postcode,
                                             kw['url_togo'][0]['current_list'])
                    return url
                # when you need to use gen_url_filter_bedrooms(), it indicates that there are so many sold records that only applying price filter is not enough
                def gen_url_filter_bedrooms(url_root_filter_bedrooms, **kw):
                    url = url_root_filter_bedrooms % (kw['url_togo'][0]['bedroom_from'], kw['url_togo'][0]['price_from'], kw['url_togo'][0]['price_to'],
                                                      kw['location'].suburb.replace(' ', '+').lower(), kw['location'].state.lower(),
                                                      kw['location'].postcode, kw['url_togo'][0]['current_list'], kw['url_togo'][0]['bedroom_to'])
                    return url
                url_root = u'https://www.realestate.com.au/sold/in-%s%%2c+%s+%s%%3b/list-%s?includeSurrounding=false'
                url_root_filter_price = u'https://www.realestate.com.au/sold/between-%s-%s-in-%s%%2c+%s+%s%%3b/list-%s?includeSurrounding=false'
                url_root_filter_bedrooms = u'https://www.realestate.com.au/sold/with-%s-between-%s-%s-in-%s%%2c+%s+%s/list-%s?maxBeds=%s&includeSurrounding=false&source=refinement'
                Bedrooms = Enum('Bedrooms', ('studio', '1-bedroom', '2-bedrooms', '3-bedrooms', '4-bedrooms', '5-bedrooms'))
                MaxBeds = Enum('MaxBeds', ('studio', '1', '2', '3', '4', '5'))
                headers_base = {'accept': '*/*',
                                'referer': 'https://www.realestate.com.au/sold'}
                headers_base.update(self.headers_useragent_pool[random.randint(0, len(self.headers_useragent_pool) - 1)])
                hds = headers_base
                # it's not my first time to fetch pages -.-
                if len(kw['url_togo']) > 0:
                    # don't need to apply extra filters
                    if kw['url_togo'][0]['list_max'] <= 50:
                        # it's not generated by applying filters
                        if kw['url_togo'][0]['list_max'] != 0:
                            kw['url_togo'][0]['current_list'] += 1
                        if not 'filter' in kw['url_togo'][0]:
                            url = gen_url(url_root, **kw)
                        elif len(kw['url_togo'][0]['filter']) == 1:
                            url = gen_url_filter_price(url_root_filter_price, **kw)
                        elif len(kw['url_togo'][0]['filter']) == 2:
                            url = gen_url_filter_bedrooms(url_root_filter_bedrooms, **kw)
                        else:
                            print ('[*]Error: Unable to generate URL')
                    else:
                        # it's the first time to apply a filter
                        if not 'filter' in kw['url_togo'][0]:
                            # apply price filter
                            kw['url_togo'][0]['price_from'] = 0
                            kw['url_togo'][0]['price_to'] = 350000
                            kw['url_togo'][0]['filter'] = ['price', ]
                            url = gen_url_filter_price(url_root_filter_price, **kw)
                            kw['url_togo'].append({})
                            kw['url_togo'][-1]['price_from'] = 350001
                            kw['url_togo'][-1]['price_to'] = 25000000
                            kw['url_togo'][-1]['current_list'] = 1
                            kw['url_togo'][-1]['list_max'] = 0
                            kw['url_togo'][-1]['filter'] = ['price', ]
                        elif len(kw['url_togo'][0]['filter']) == 1:
                            # need change params of the price filter
                            if kw['url_togo'][0]['price_from'] !=kw['url_togo'][0]['price_to']:
                                price_median = int((kw['url_togo'][0]['price_from'] + kw['url_togo'][0]['price_to']) / 2)
                                kw['url_togo'].append({})
                                if kw['url_togo'][0]['price_to'] - kw['url_togo'][0]['price_from'] == 1:
                                    kw['url_togo'][-1]['price_from'] = kw['url_togo'][0]['price_to']
                                    kw['url_togo'][-1]['price_to'] = kw['url_togo'][0]['price_to']
                                    kw['url_togo'][0]['price_to'] = kw['url_togo'][0]['price_from']
                                else:
                                    kw['url_togo'][-1]['price_to'] = kw['url_togo'][0]['price_to']
                                    kw['url_togo'][0]['price_to'] = price_median
                                    kw['url_togo'][-1]['price_from'] = price_median + 1
                                kw['url_togo'][-1]['current_list'] = 1
                                kw['url_togo'][-1]['list_max'] = 0
                                kw['url_togo'][-1]['filter'] = ['price', ]
                                url = gen_url_filter_price(url_root_filter_price, **kw)
                            # apply bedroom filter
                            else:
                                for i in range(len(Bedrooms)):
                                    if i == 0:
                                        kw['url_togo'][0]['bedroom_from'] = Bedrooms(i + 1).name
                                        kw['url_togo'][0]['bedroom_to'] = MaxBeds(i + 1).name
                                        if 'bedroom' not in kw['url_togo'][0]['filter']:
                                            kw['url_togo'][0]['filter'].append('bedroom')
                                        url = gen_url_filter_bedrooms(url_root_filter_bedrooms, **kw)
                                    else:
                                        kw['url_togo'].append({})
                                        kw['url_togo'][-1] = kw['url_togo'][0].copy()
                                        kw['url_togo'][-1]['bedroom_from'] = Bedrooms(i + 1).name
                                        kw['url_togo'][-1]['bedroom_to'] = MaxBeds(i + 1).name
                                        kw['url_togo'][-1]['list_max'] = 0
                                        if 'bedroom' not in kw['url_togo'][-1]['filter']:
                                            kw['url_togo'][-1]['filter'].append('bedroom')
                        elif len(kw['url_togo'][0]['filter']) == 2:
                            print ('[*]It seems that you need extra filter to decrease the amount of list.')
                            print ('[*]To simplify the fetch process, only fetch the first 50 pages.')
                            kw['url_togo'][0]['list_max'] = 50
                            url = gen_url_filter_bedrooms(url_root_filter_bedrooms, **kw)

                # it's the initial page
                else:
                    url = gen_url(url_root, **kw)
                break
            if case(4):
                url_root = u'https://www.realestate.com.au/sold/list-%s?includeSurrounding=false'
                headers_base = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                                'accept-encoding': 'gzip, deflate, br',
                                'upgrade-insecure-requests': '1',
                                'accept-Language': 'zh-CN,zh;q=0.9',
                                'referer': 'https://www.realestate.com.au/sold'}
                # generate url
                url = url_root % kw['list_id']
                # generate header
                headers_base.update(self.headers_useragent_pool[random.randint(0, len(self.headers_useragent_pool) - 1)])
                hds = headers_base  # just assign the pointer
                break
            if case(5):
                url_root = u'https://www.realestate.com.au/auction-results/'
                headers_base = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                                'accept-encoding': 'gzip, deflate, br',
                                'accept-Language': 'zh-CN,zh;q=0.9'}
                # generate url
                url = url_root + kw['state']
                # generate header
                headers_base.update(self.headers_useragent_pool[random.randint(0, len(self.headers_useragent_pool) - 1)])
                hds = headers_base  # just assign the pointer
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
                medi_price_sub.update_date = datetime.datetime.now().strftime('%Y-%m-%d')
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


    async def sold_history_parse(self, response, **kw):
        # mainly for dealing with the date format to meet the requirements of MySQL
        Month = Enum('Month', ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'))
        def str2date(string):
            string = string.replace('Sold on ','')
            strformat = re.compile(r'^(\d+)\s(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s(\d{4})$')
            if strformat.match(string) != None:
                date_origin = strformat.match(string).groups()
                if len(date_origin) == 3:
                    dd = date_origin[0].zfill(2)
                    mm = str(getattr(Month, date_origin[1]).value).zfill(2)
                    date = date_origin[2] + '-' + mm + '-' + dd
                else:
                    print ('[!]Error: function \'str2date\' receive invalid params')
                    raise ValueError
            else:
                date = None
            return date

        def property_detail_parse(all_info):
            # date parse
            price = all_info.find('div', {'class': 'residential-card__price rui-truncate'}).text
            if price == '':
                price_src = all_info.find('div', {'class': 'residential-card__price rui-truncate'}).img['src']
                price_src_format = re.compile(r'^https://text-to-image.realestate.com.au/convert/([\w+/=]+)')
                price_base64 = price_src_format.match(price_src).groups()[0]
                if price_base64 != '':
                    price = bytes.decode(base64.urlsafe_b64decode(str.encode(price_base64)))
                else:
                    price = None
            property_info = all_info.find('div', {'class': 'residential-card__info'})
            property_url = property_info.a['href']
            m = 0
            for info in property_info.a.span.strings:
                if m == 0:
                    property_address = info.title().replace('\x80','').replace('\x91','').replace('\x92','').replace('\x93','').replace('\x94','').replace('，',' ').replace('‘','').replace('’','').replace('â','').capitalize()
                if m == 2:
                    property_suburb = info.title()
                m += 1
            property_type = property_info.p.find_all('span')[0].text
            property_sold_on = property_info.p.find_all('span')[1].text
            property_features = all_info.find('ul', {'class': 'general-features rui-clearfix residential-card__general-features'})
            if property_features == None:
                property_beds = None
                property_baths = None
                property_cars = None
            else:
                property_beds = property_features.find('span', {'class': 'general-features__icon general-features__beds'}).text.replace(' ', '')
                property_baths = property_features.find('span', {'class': 'general-features__icon general-features__baths'}).text.replace(' ', '')
                property_cars = property_features.find('span', {'class': 'general-features__icon general-features__cars'}).text.replace(' ', '')
            # date clean
            price = price.replace('$', '').replace(',', '')
            if price.isnumeric() == False:
                price = None
            elif int(price) > 99999999:
                price = None
            property_sold_on = str2date(property_sold_on)
            if property_sold_on != None:
                if property_sold_on.replace('-', '').isnumeric() == False:
                    property_sold_on = None
            return {'full_address':property_address,
                    'suburb': property_suburb,
                    'property_type': property_type,
                    'price': price,
                    'bedrooms':property_beds,
                    'bathrooms': property_baths,
                    'carspaces': property_cars,
                    'sold_date': property_sold_on,
                    'url': property_url}

        source_code = await response.read()
        soup = BeautifulSoup(source_code, 'lxml')
        propt_sold_history = Propt_sold_history()

        # list information
        if soup.find('div', {'class': 'no-search-results'}) != None:
            if len(kw['url_togo']) == 0:
                print ('[*]No recent sales: id=%s, postcode=%s, suburb=%s, state=%s, price from $0 to any' % (kw['location'].id,
                                                                                           kw['location'].postcode,
                                                                                           kw['location'].suburb,
                                                                                           kw['location'].state))
            elif 'price_from' in kw['url_togo'][0] and 'bedroom_from' not in kw['url_togo'][0]:
                print ('[*]No recent sales: id=%s, postcode=%s, suburb=%s, state=%s, price from $%s to $%s' % (kw['location'].id, kw['location'].postcode,
                                                                                           kw['location'].suburb, kw['location'].state,
                                                                                           kw['url_togo'][0]['price_from'], kw['url_togo'][0]['price_to']))
            elif 'bedroom_from' in kw['url_togo'][0]:
                print ('[*]No recent sales: id=%s, postcode=%s, suburb=%s, state=%s, price from $%s to $%s, bedroom from %s to %s' % (kw['location'].id, kw['location'].postcode,
                                                                                           kw['location'].suburb, kw['location'].state,
                                                                                           kw['url_togo'][0]['price_from'], kw['url_togo'][0]['price_to'],
                                                                                           kw['url_togo'][0]['bedroom_from'], kw['url_togo'][0]['bedroom_to']))

            # delete the item in url_togo
            if len(kw['url_togo']) > 0:
                kw['url_togo'].pop(0)
            with open(EXCEPTION_PATH, 'a') as f:
                self.report.write_exception('TASK %s: NO_VALID_INFORMATION' % self.task_id,
                                            'id=%s, postcode=%s, suburb=%s, state=%s'
                                                % (kw['location'].id, kw['location'].postcode, kw['location'].suburb, kw['location'].state),
                                            f)
            return
        property_list = soup.find('div',{'class':'results-set-header__summary'}).text
        list_format = re.compile(r'([\d]+)-([\d]+)\sof\s([\d]+)\sresults$')
        list_format_lessinfo = re.compile(r'([\d]+)\sresult[s]?$')
        if list_format.match(property_list) != None:
            list = list_format.match(property_list).groups()
            if len(list) == 3:
                try:
                    list_from = int(list[0])
                    list_to = int(list[1])
                    list_overall = int(list[2])
                except ValueError as e:
                    print ('[!]Error: list number can\'t be transformed to int.', e)
                # if it's the initial (first) page, we will generate the detailed list info
                if list_from == 1:
                    if len(kw['url_togo']) == 0:
                        print('[*]Suburb %s in %s has %s property sold records, price from $0 to any' % (kw['location'].suburb, kw['location'].state, list_overall))
                    elif 'price_from' in kw['url_togo'][0] and 'bedroom_from' not in kw['url_togo'][0]:
                        print('[*]Suburb %s in %s has %s property sold records, price from $%s to $%s' %
                                        (kw['location'].suburb, kw['location'].state, list_overall, kw['url_togo'][0]['price_from'], kw['url_togo'][0]['price_to']))
                    elif 'bedroom_from' in kw['url_togo'][0]:
                        print('[*]Suburb %s in %s has %s property sold records, price from $%s to $%s, bedroom from %s to %s' %
                                        (kw['location'].suburb, kw['location'].state, list_overall, kw['url_togo'][0]['price_from'], kw['url_togo'][0]['price_to'],
                                         kw['url_togo'][0]['bedroom_from'], kw['url_togo'][0]['bedroom_to']))
                    list_max = math.ceil(list_overall/20)
                    # record list information when there are more than one list
                    if list_max > 1:
                        if len(kw['url_togo']) == 0:
                            kw['url_togo'].append({})
                        kw['url_togo'][0]['list_overall'] = list_overall
                        # assign only once as req_gen() would assign it if two filters (price+bedroom) have been applied
                        if 'list_max' not in kw['url_togo'][0]:
                            kw['url_togo'][0]['list_max'] = list_max
                        elif not (kw['url_togo'][0]['list_max'] == 50 and list_max > 50):
                            kw['url_togo'][0]['list_max'] = list_max
                        #elif int(list_max) < kw['url_togo'][0]['list_max']:
                        #    kw['url_togo'][0]['list_max'] = list_max
                        kw['url_togo'][0]['current_list'] = 1
                        kw['url_togo'][0]['list_from'] = list_from
                        kw['url_togo'][0]['list_to'] = list_to
                    if kw['url_togo'][0]['list_max'] > 50:
                        return 'need filter'
                else:
                    if len(kw['url_togo']) > 0:
                        kw['url_togo'][0]['list_from'] = list_from
                        kw['url_togo'][0]['list_to'] = list_to
            else:
                print('[*]Error: Can\'t parse the length of the property list: id=%s, postcode=%s, suburb=%s, state=%s'
                      % (kw['location'].id, kw['location'].postcode, kw['location'].suburb, kw['location'].state))
                # write to exception
                # something to do

        elif list_format_lessinfo.match(property_list) != None:
            # add code to print the amount of results
            list_from = 1
            list_to = int(list_format_lessinfo.match(property_list).groups()[0])
            list_overall = list_to
            if len(kw['url_togo']) == 0:
                print('[*]Suburb %s in %s has %s property sold records, price from $0 to any' % (kw['location'].suburb, kw['location'].state, list_overall))
            elif 'price_from' in kw['url_togo'][0]:
                print('[*]Suburb %s in %s has %s property sold records, price from $%s to $%s' %
                      (kw['location'].suburb, kw['location'].state, list_overall, kw['url_togo'][0]['price_from'], kw['url_togo'][0]['price_to']))
        else:
            print ('[*]Error: Can\'t parse the property list: id=%s, postcode=%s, suburb=%s, state=%s'
                        % (kw['location'].id, kw['location'].postcode, kw['location'].suburb, kw['location'].state))
            # write to exception
            # something to do

        # parse other information
        # property information
        full_view_properties = soup.find_all('article', {'class': 'results-card residential-card '})
        compressed_view_properties = soup.find_all('article', {'class': 'results-card residential-card residential-card--compressed-view'})
        record_count = 0
        if len(full_view_properties) > 0:
            if len(full_view_properties) <= 20:
                for i in range(len(full_view_properties)):
                    property_detail = full_view_properties[i].find('div', {'class': 'residential-card__content'})
                    # price, address and features
                    property_detail_info = property_detail_parse(property_detail)
                    # agent
                    property_agent = full_view_properties[i].find('div', {'class':'branding branding--medium'})
                    if property_agent != None:
                        property_agent = property_agent.img['alt'].replace(' - Wagga Wagga', '')
                    propt_sold_history.suburb_id = kw['location'].id
                    propt_sold_history.full_address = property_detail_info['full_address']
                    propt_sold_history.property_type = property_detail_info['property_type']
                    propt_sold_history.price = property_detail_info['price']
                    propt_sold_history.bedrooms = property_detail_info['bedrooms']
                    propt_sold_history.bathrooms = property_detail_info['bathrooms']
                    propt_sold_history.carspaces = property_detail_info['carspaces']
                    propt_sold_history.sold_date = property_detail_info['sold_date']
                    propt_sold_history.url = property_detail_info['url']
                    propt_sold_history.agent = property_agent
                    propt_sold_history.update_date = datetime.datetime.now().strftime('%Y-%m-%d')
                    # save these information into dict_buffer
                    if (kw['location'].id in self.dict_buffer) == False:
                        self.dict_buffer[kw['location'].id] = []
                    self.dict_buffer[kw['location'].id].append(json.dumps(propt_sold_history, default=Propt_sold_history.soldhistory2dict))
                    propt_sold_history.data_clean()
                    record_count += 1
            else:
                print('[*]Error: Too much info on the page.')
        if len(compressed_view_properties) > 0:
            if len(compressed_view_properties) <= 20:
                for i in range(len(compressed_view_properties)):
                    property_detail = compressed_view_properties[i].find('div', {'class': 'residential-card__content'})
                    # price, address and features
                    property_detail_info = property_detail_parse(property_detail)
                    # agent
                    property_agent = compressed_view_properties[i].find('div', {'class': 'branding branding--extra-small'})
                    if property_agent != None:
                        property_agent = property_agent.img['alt'].replace(' - Wagga Wagga', '')
                    propt_sold_history.suburb_id = kw['location'].id
                    propt_sold_history.full_address = property_detail_info['full_address']
                    propt_sold_history.property_type = property_detail_info['property_type']
                    propt_sold_history.price = property_detail_info['price']
                    propt_sold_history.bedrooms = property_detail_info['bedrooms']
                    propt_sold_history.bathrooms = property_detail_info['bathrooms']
                    propt_sold_history.carspaces = property_detail_info['carspaces']
                    propt_sold_history.sold_date = property_detail_info['sold_date']
                    propt_sold_history.url = property_detail_info['url']
                    propt_sold_history.agent = property_agent
                    propt_sold_history.update_date = datetime.datetime.now().strftime('%Y-%m-%d')
                    # save these information into dict_buffer
                    if (kw['location'].id in self.dict_buffer) == False:
                        self.dict_buffer[kw['location'].id] = []
                    self.dict_buffer[kw['location'].id].append(json.dumps(propt_sold_history, default=Propt_sold_history.soldhistory2dict))
                    propt_sold_history.data_clean()
                    record_count += 1
            else:
                print('[*]Error: Too much info on the page.')

        if len(kw['url_togo']) > 0:
            print('[*]%s records are fetched in list %s, suburb_id=%s, postcode=%s, suburb=%s, state=%s'
                  % ((list_to - list_from + 1), kw['url_togo'][0]['current_list'],
                     kw['location'].id, kw['location'].postcode, kw['location'].suburb, kw['location'].state.upper()))
        else:
            print('[*]%s records are fetched in list 1, suburb_id=%s, postcode=%s, suburb=%s, state=%s'
                  % ((list_to - list_from + 1),
                     kw['location'].id, kw['location'].postcode, kw['location'].suburb, kw['location'].state.upper()))

        # Check if this list is the last one. If the url_togo is empty, the loop in work() will break
        if len(kw['url_togo']) > 0:
            if 'list_max' in kw['url_togo'][0]:
                if kw['url_togo'][0]['current_list'] + 1 > kw['url_togo'][0]['list_max']:
                    kw['url_togo'].pop(0)
            else:
                kw['url_togo'].pop(0)

        # save all the infomation in dict_buffer into result queue
        if len(kw['url_togo']) == 0:
            try:
                # extract property info from the dict_buffer and put them into result queue
                self.result_queue.put_nowait(json.dumps({kw['location'].id: self.dict_buffer.get(kw['location'].id)}))
                print('[*]Crawl a suburb successfully: id=%s, postcode=%s, suburb=%s, state=%s' % (kw['location'].id,
                                                                                                   kw['location'].postcode,
                                                                                                   kw['location'].suburb,
                                                                                                   kw['location'].state))
                self.dict_buffer.pop(kw['location'].id)
                self.done += 1
            except asyncio.queues.QueueFull as e:
                print('[!]Error: ', e)
                self.failed += 1

    async def latest_sold_parse(self, response, **kw):
        # mainly for dealing with the date format to meet the requirements of MySQL
        Month = Enum('Month', ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'))

        def str2date(string):
            string = string.replace('Sold on ', '')
            strformat = re.compile(r'^(\d+)\s(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s(\d{4})$')
            if strformat.match(string) != None:
                date_origin = strformat.match(string).groups()
                if len(date_origin) == 3:
                    dd = date_origin[0].zfill(2)
                    mm = str(getattr(Month, date_origin[1]).value).zfill(2)
                    date = date_origin[2] + '-' + mm + '-' + dd
                else:
                    print('[!]Error: function \'str2date\' receive invalid params')
                    raise ValueError
            else:
                date = None
            return date

        def property_detail_parse(all_info):
            # date parse
            price = all_info.find('div', {'class': 'residential-card__price rui-truncate'}).text
            if price == '':
                price_src = all_info.find('div', {'class': 'residential-card__price rui-truncate'}).img['src']
                price_src_format = re.compile(r'^https://text-to-image.realestate.com.au/convert/([\w+/=]+)')
                price_base64 = price_src_format.match(price_src).groups()[0]
                if price_base64 != '':
                    price = bytes.decode(base64.urlsafe_b64decode(str.encode(price_base64)))
                else:
                    price = None
            property_info = all_info.find('div', {'class': 'residential-card__info'})
            #property_url = property_info.a['href']

            # info = property_info.a.span.text.split(',')
            # property_address = ','.join(info[:-1]).replace('\x91','').replace('\u200b','').replace('\x92','').replace('\x93','').strip()
            # property_suburb = info[-1].strip()
            info = property_info.find('div', {'class': 'residential-card__info-text'}).text.split(',')
            property_address = info[0].strip().title().replace('\x91','').replace('\x92','').replace('\x93','').replace('\u200b','')
            property_suburb = info[-1].strip().title()

            property_type = property_info.p.find_all('span')[0].text
            property_sold_on = property_info.p.find_all('span')[1].text
            property_features = all_info.find('ul', {
                'class': 'general-features rui-clearfix residential-card__general-features'})
            if property_features == None:
                property_beds = None
                property_baths = None
                property_cars = None
            else:
                property_beds = property_features.find('span', {'class': 'general-features__icon general-features__beds'})
                if property_beds != None:
                    property_beds = property_beds.text.replace(' ', '')
                property_baths = property_features.find('span', {'class': 'general-features__icon general-features__baths'})
                if property_baths != None:
                    property_baths = property_baths.text.replace(' ', '')
                property_cars = property_features.find('span', {'class': 'general-features__icon general-features__cars'})
                if property_cars != None:
                    property_cars = property_cars.text.replace(' ', '')
            # date clean
            price = price.replace('$', '').replace(',', '')
            if price.isnumeric() == False:
                price = None
            elif int(price) > 99999999:
                price = None
            property_sold_on = str2date(property_sold_on)
            if property_sold_on != None:
                if property_sold_on.replace('-', '').isnumeric() == False:
                    property_sold_on = None
            return {'full_address': property_address,
                    'suburb': property_suburb,
                    'property_type': property_type,
                    'price': price,
                    'bedrooms': property_beds,
                    'bathrooms': property_baths,
                    'carspaces': property_cars,
                    'sold_date': property_sold_on}
                    #'url': property_url}

        source_code = await response.read()
        soup = BeautifulSoup(source_code, 'lxml')
        propt_sold_history = Propt_sold_history()

        full_view_properties = soup.find_all('article', {'class': 'results-card residential-card '})
        record_count = 0
        self.dict_buffer[kw['list_id']] = []
        if len(full_view_properties) > 0:
            if len(full_view_properties) <= 25:
                for i in range(len(full_view_properties)):
                    property_detail = full_view_properties[i].find('div', {'class': 'residential-card__content'})
                    # price, address and features
                    property_detail_info = property_detail_parse(property_detail)
                    # agent
                    property_agent = full_view_properties[i].find('div', {'class': 'branding branding--large '})
                    if property_agent == None:
                        property_agent = full_view_properties[i].find('div', {'class': 'branding branding--medium '})
                    if property_agent != None:
                        # property_agent = property_agent.img['alt'].replace(' - Wagga Wagga', '').replace('\xE2\x80\x8B','').replace('\u200b','').strip()
                        property_agent = property_agent.img['alt'].replace('\xE2\x80\x8B','').replace('\u200b','').split(' - ')[0].strip()
                    # state and postcode
                    postcode_info = full_view_properties[i].find('div', {'class': 'property-image'}).img['alt']
                    if postcode_info != None:
                        postcode_info = postcode_info.split(',')[-1].replace('/s+', ' ')
                        postcode_format = re.compile(r'\s?([a-zA-Z]{2,3})\s([\d]{4})$')
                        r = postcode_format.match(postcode_info).groups()
                    if len(r) == 2:
                        propt_sold_history.state = r[0].upper()
                        propt_sold_history.postcode = r[1]
                    else:
                        propt_sold_history.state = None
                        propt_sold_history.postcode = None
                    # property URL
                    property_url = full_view_properties[i].a['href']

                    propt_sold_history.suburb = property_detail_info['suburb']
                    propt_sold_history.full_address = property_detail_info['full_address']
                    propt_sold_history.property_type = property_detail_info['property_type']
                    propt_sold_history.price = property_detail_info['price']
                    propt_sold_history.bedrooms = property_detail_info['bedrooms']
                    propt_sold_history.bathrooms = property_detail_info['bathrooms']
                    propt_sold_history.carspaces = property_detail_info['carspaces']
                    propt_sold_history.sold_date = property_detail_info['sold_date']
                    propt_sold_history.url = property_url
                    propt_sold_history.agent = property_agent
                    propt_sold_history.update_date = datetime.datetime.now().strftime('%Y-%m-%d')
                    # save these information into dict_buffer
                    self.dict_buffer[kw['list_id']].append(
                        json.dumps(propt_sold_history, default=Propt_sold_history.soldhistory2dict))
                    propt_sold_history.data_clean()
                    record_count += 1
                # save all the infomation in dict_buffer into result queue
                try:
                    # extract property info from the dict_buffer and put them into result queue
                    self.result_queue.put_nowait(
                        json.dumps({kw['list_id']: self.dict_buffer.get(kw['list_id'])}))
                    print('[*]%s records are fetched in list %s' % (record_count, kw['list_id']))
                    self.dict_buffer.pop(kw['list_id'])
                    self.done += 1
                except asyncio.queues.QueueFull as e:
                    print('[!]Error: ', e)
                    self.failed += 1
            else:
                print('[*]Error: Too much info on the page.')
        else:
            print('[*]No any valid information is found for list_id=%s' % kw['list_id'])
            with open(EXCEPTION_PATH, 'a') as f:
                self.report.write_exception('TASK %s: NO_VALID_INFORMATION' % self.task_id,
                                            'list_id=%s' % kw['list_id'],
                                            f)

    async def corelogic_auction_parse(self, response, **kw):
        source_code = await response.read()
        soup = BeautifulSoup(source_code, 'lxml')
        auction_result = Auction_result()

        key_stats = soup.find('section', {'class': 'auction-results-key-stats'})
        if key_stats == None:
            print('[*]No any valid information is found for state=%s' % kw['state'])
            with open(EXCEPTION_PATH, 'a') as f:
                self.report.write_exception('TASK %s: NO_VALID_INFORMATION' % self.task_id, 'state=%s'
                                            % kw['state'], f)
            self.failed += 1
        else:
            if len(key_stats) > 10:
                auction_result.scheduled_auctions = key_stats.find('div', {'class': 'key-stats reported-auctions'}).find('div', {'class': 'num'}).text.replace(',','')
                auction_result.results_available = key_stats.find('div', {'class': 'key-stats result-available'}).find('div', {'class': 'num'}).text.replace(',','')
                auction_result.clearance_rate = key_stats.find('div', {'class': 'key-stats clearance-rate'}).find('div', {'class': 'num'}).span.text.replace(',', '')
                sold_type_stats = key_stats.find('ul', {'class': 'auction-results-sold-type-stats'})
                auction_result.sold_prior_to_auction = sold_type_stats.find('li', {'class':'sold-prior-auction'}).text.split(' ')[0]
                auction_result.sold_at_auction = sold_type_stats.find('li', {'class': 'sold-at-auction'}).text.split(' ')[0]
                auction_result.sold_after_auction = sold_type_stats.find('li', {'class': 'sold-after-auction'}).text.split(' ')[0]
                auction_result.withdrawn = sold_type_stats.find('li', {'class': 'withdrawn'}).text.split(' ')[0]
                auction_result.passed_in = sold_type_stats.find('li', {'class': 'passed-in'}).text.split(' ')[0]
                auction_result.state = kw['state'].upper()
                auction_result.auction_date = soup.find('p', {'class': 'auction-date'}).time['datetime']
                auction_result.update_date = datetime.datetime.now().strftime('%Y-%m-%d')
                try:
                    self.result_queue.put_nowait(json.dumps(auction_result, default=Auction_result.auction2dict))
                    print('[*]Crawl a page successfully: state=%s, clearance_rate=%s, sold_prior_to_auction=%s' % (auction_result.state, auction_result.clearance_rate, auction_result.sold_prior_to_auction))
                    self.done += 1
                except asyncio.queues.QueueFull as e:
                    print('[!]Error: ', e)
                    self.failed += 1
            # the page don't contain valid information
            else:
                print('[*]No any valid information is found for state=%s' % kw['state'])
                with open(EXCEPTION_PATH, 'a') as f:
                    self.report.write_exception('TASK %s: NO_VALID_INFORMATION' % self.task_id, 'state=%s'
                                                % kw['state'], f)
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
            if case(3):
                await self.sold_history_parse(response, **kw)
                break
            if case(4):
                await self.latest_sold_parse(response, **kw)
                break
            if case(5):
                await self.corelogic_auction_parse(response, **kw)
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
                            if resp.status != 200:
                                print ('[*]The page is not exist: Try %s time(s) for url: %s' % (tries, url))
                                continue
                            await self.page_parse(resp, **kw)
                            break
            except aiohttp.ClientError as e:
                # to deal with exceptions like timeout and urlerror
                if tries > 1:
                    print('[*]Try %s time(s) for url: %s' % (tries, url))
            except asyncio.TimeoutError as e:
                if tries > 1:
                    print('[*]Try %s time(s) for url: %s' % (tries, url))

        else:
            # write information to a txt
            print('[*]Can\'t crawl the page: %s' % url)
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
                    if case(3):
                        self.report.write_exception('TASK %s: CANNOT_CRAWL' % self.task_id,
                                                    'suburb_id=%s, postcode=%s, suburb=%s, state=%s, url=%s'
                                                        % (kw['location'].id, kw['location'].postcode, kw['location'].suburb, kw['location'].state, url),
                                                        f)
                        break
                    if case(4):
                        self.report.write_exception('TASK %s: CANNOT_CRAWL' % self.task_id,
                                                    'list_id=%s' % kw['list_id'], f)
                        break
                    if case(5):
                        self.report.write_exception('TASK %s: CANNOT_CRAWL' % self.task_id,
                                                    'state=%s' % kw['state'], f)
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
                            print('[!]Error: ', e)
                        except ValueError as e:
                            print('[!]Error: ', e)
                        url, header = self.req_gen(location = location)
                        await self.fetch(url, header, location = location, medi_price_sub=medi_price_sub)
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
                if case(3):
                    location = Location()
                    while True:
                        try:
                            lo = await self.task_queue.get()
                            location = json.loads(lo, object_hook=location.dict2location)
                        except asyncio.queues.QueueEmpty as e:
                            print ('[!]Error: ', e)
                        except ValueError as e:
                            print ('[!]Error: ', e)
                        url_togo = []
                        # generate the first page's url and header
                        #url, header = self.req_gen(location = location, url = url_togo)
                        # get the first page of sold history list of the suburb
                        #await self.fetch(url, header, location = location)
                        #url_togo.append(url)
                        # get other pages of sold history of the suburb
                        while True:
                            # generate the initial page and from then on read from url_togo
                            url, header = self.req_gen(location=location, url_togo=url_togo)
                            await self.fetch(url, header, location=location, url_togo=url_togo)
                            if len(url_togo) == 0:
                                break
                        # must after result_queue.put()
                        location.data_clean()
                        self.task_queue.task_done()
                    break
                if case(4):
                    while True:
                        try:
                            list_id = await self.task_queue.get()
                        except asyncio.queues.QueueEmpty as e:
                            print('[!]Error: ', e)
                        except ValueError as e:
                            print('[!]Error: ', e)
                        url, header = self.req_gen(list_id=list_id)
                        await self.fetch(url, header, list_id=list_id)
                        # must after result_queue.put()
                        self.task_queue.task_done()
                    break
                if case(5):
                    while True:
                        try:
                            state = await self.task_queue.get()
                        except asyncio.queues.QueueEmpty as e:
                            print('[!]Error: ', e)
                        except ValueError as e:
                            print('[!]Error: ', e)
                        url, header = self.req_gen(state=state)
                        await self.fetch(url, header, state=state)
                        # must after result_queue.put()
                        self.task_queue.task_done()
                    break
                if case():
                    print ('[!]Error: invalid task id')
        except asyncio.CancelledError as e:
            print ('[*]An coroutine has been cancelled.', e)
            raise asyncio.CancelledError


    # the main of the coroutine, you can allocate different tasks to crawl() by giving different task_id
    async def crawl(self):
        """Run the crawler until all finished."""
        #self.workers = [asyncio.ensure_future(self.work(), loop = self.loop)
         #          for _ in range(self.max_tasks)]
        self.workers = [self.loop.create_task(self.work()) for _ in range(self.max_tasks)]
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


