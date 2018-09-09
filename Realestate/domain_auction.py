#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
This .py is mainly for fetch auction result from domain.
'''

import requests
import urllib
import os
import PyPDF2
import re
from enum import Enum
import datetime
from db import DBWrapper

DOWNLOAD_FOLDER = './_temp_download'
TB_AUCTION = 'domain_auction_results'

class Domain_auction(object):
    def __init__(self, path):
        self.path = path
        self.dbwrapper = DBWrapper()

    def has_dir(self,):
        if os.path.exists(self.path):
            if not os.path.isdir(self.path):
                print('[!]Error: Invalid path: %s' % self.path)
                raise ValueError
            print('[*]Directory \'%s\' has exist' % self.path)
        else:
            os.mkdir(self.path)
            print('[*]Directory \'%s\' is created' % self.path)
        return True

    def fetch_result(self):
        cities = ['Sydney', 'Melbourne', 'Brisbane', 'Adelaide', 'Canberra']
        url = u'https://auction-results.domain.com.au/Proofed/PDF/%s_Domain.pdf'
        headers= {  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'zh-CN,zh;q=0.9',
                    'Cache-Control': 'max-age=0',
                    'Connection': 'keep-alive',
                    'Host': 'auction-results.domain.com.au',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36'}
        print('[*]Downloading pdf...')
        for city in cities:
            req = urllib.request.Request(url % city, headers=headers)
            filename = os.path.basename(url % city)
            resp = urllib.request.urlopen(req)
            with open(os.path.join(self.path,filename), 'wb') as f:
                f.write(resp.read())
            print('[*]Auction results of %s is fetched' % city)

    def del_dir(self):
        for filename in os.listdir(self.path):
            os.remove(os.path.join(self.path,filename))
        os.rmdir(self.path)
        print('[*]Temporary directory \'%s\' is deleted' % self.path)

    def save2db(self, result):
        if len(result) != 10:
            print('[!]Error: There isn\'t enough information that is parsed')
        sql = 'INSERT INTO %s (city, listed_auctions, reported_auctions, sold, withdrawn, clearance_rate, ' \
                                'total_sales, median, auction_date, update_date) ' \
                                'VALUES (\'%s\', %s, %s, %s, %s, %s, %s, %s, \'%s\', \'%s\')'
        self.dbwrapper.db_execute(sql % (TB_AUCTION, result['city'], result['listed_auctions'], result['reported_auctions'],
                                         result['sold'], result['withdrawn'], result['clearance_rate'],
                                         result['total_sales'], result['median'], result['auction_date'],
                                         result['update_date']))

    def str2date(self, date):
        Month = Enum('Month', ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August',
                               'September', 'October', 'November', 'December'))
        date = date.split(' ')
        if len(date) == 4:
            date = date[1:]
        dd_format = re.compile(r'(\d{1,2})(st|nd|rd|th)')
        dd = dd_format.match(date[0]).groups()[0].zfill(2)
        mm = str(getattr(Month, date[1]).value).zfill(2)
        yy = date[2]
        date = yy + '-' + mm + '-' + dd
        return date

    def parse(self, text):
        text = text.split('\n')
        for i in range(len(text)):
            line = text[i].replace('%','').replace('$','').replace(',','').strip()
            if line == 'Property Snapshot':
                break
        f1 = lambda x: x.replace('%','').replace('$','').replace(',','').strip()
        f2 = lambda x: x if x.isdigit() else 'NULL'
        key_lines = list(map(f1, text[i+2:i+15:2]))
        key_lines = list(map(f2, key_lines))
        print('Auction date: %s' % self.str2date(text[i-1]))
        print('Number Listed Auctions: %s' % key_lines[0])
        print('Number Reported Auctions: %s' % key_lines[1])
        print('Sold: %s' % key_lines[2])
        print('Withdrawn: %s' % key_lines[3])
        print('Cleared: %s%%' % key_lines[4])
        print('Total Sales: $%s' % key_lines[5])
        print('Median: $%s' % key_lines[6])
        return {'auction_date': self.str2date(text[i-1]),
                'listed_auctions': key_lines[0],
                'reported_auctions': key_lines[1],
                'sold': key_lines[2],
                'withdrawn': key_lines[3],
                'clearance_rate': key_lines[4],
                'total_sales': key_lines[5],
                'median': key_lines[6]}

    def readpdf(self):
        for filename in os.listdir(self.path):
            with open(os.path.join(self.path, filename), 'rb') as f:
                reader = PyPDF2.PdfFileReader(f)
                content = reader.getPage(0).extractText()
                city = filename.replace('_Domain.pdf','')
                print('---------------------------')
                print('CITY: %s' % city)
                r = self.parse(content)
                r['city'] = city
                r['update_date'] = datetime.datetime.now().strftime('%Y-%m-%d')
                self.save2db(r)

if __name__ == '__main__':
    domain_auction = Domain_auction(DOWNLOAD_FOLDER)
    domain_auction.has_dir()
    domain_auction.fetch_result()
    domain_auction.readpdf()
    domain_auction.del_dir()

