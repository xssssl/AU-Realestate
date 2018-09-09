#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Class Schedular is mainly for work assignment. Its responsibilities are below:
  * read data from database and put in a task queue
  * read data from result queue in which all the coroutines put data
  * read data from exceptions and put them in task queue again
  * cancel all the coroutines when all the task are done
'''

import asyncio
from db import DBWrapper
import json
from spidering import Location
from spidering import Medi_price_sub
from spidering import Propt_sold_history
from spidering import Auction_result
from spidering import Switch
import re
import mysql.connector

DB_SAVE_FAIL_PATH = './DB_fails.txt'

class Scheduler(object):
    def __init__(self, task_id, task_queue, result_queue, start_point = 1):
        self.task_id = task_id
        self.location = Location()
        self.median_price_sub = Medi_price_sub()
        self.propt_sold_history = Propt_sold_history()
        self.task_queue = task_queue
        self.result_queue = result_queue
        #self.start_id = start_id
        self.current_point = start_point
        self.excp_point = 0
        self.fetch_step = self.task_queue.maxsize
        self.dbwrapper = DBWrapper()
        self.result_queue_maxsize = self.result_queue.maxsize
        self.result_queue_qsize = 0
        if self.task_queue.maxsize > self.result_queue.maxsize:
            raise ValueError ('The size of task queue is smaller than result queue.')

    # transform some attributes of Class Scheduler into dict
    def scheduler2dict(self):
        return {'current_point': self.current_point,
                'excp_point': self.excp_point}

    def dict2scheduler(self, d):
        self.current_point = d['current_point']
        self.excp_point = d['excp_point']
        return self

    # assign works specifically for grabbing information about suburbs' median price
    def work_median_price(self):
        sql = 'SELECT * FROM postcodes WHERE id>=%s and id<%s'
        # fetch all valid information from database and put them in task_queue
        results = self.dbwrapper.db_fetchall(sql, [self.current_point, self.current_point+self.fetch_step])

        # there are many items remain
        if len(results) > 0:
            for r in results:
                self.location.id = r[0]
                self.location.postcode = r[1]
                self.location.state = r[2]
                self.location.region = r[3]
                self.location.suburb = r[5]

                # deal with items with NULL informaion
                if self.location.region == None:
                    print ('[*]An item fetched from database is abondoned: id:%s, postcode:%s, state:%s, suburb:%s' %
                                                                                               (self.location.id,
                                                                                                self.location.postcode,
                                                                                                self.location.state,
                                                                                                self.location.suburb))
                else:
                    # put without blocking, transform instance into json to make it more general
                    try:
                        self.task_queue.put_nowait(json.dumps(self.location, default=Location.location2dict))
                    except asyncio.queues.QueueFull as e:
                        print('[!]Error: ', e)
                self.location.data_clean()

            self.current_point += self.fetch_step
            print ('[*]Successfully fetch %s items from database and put %s items in task queue.' % (self.fetch_step, self.task_queue.qsize()))
        # finish fetching all the items
        else:
            # should let the crawler know that :)
            return 'done'

    # assign works for grabbing suburb's position which are probably ploygon
    def work_suburb_position(self):
        sql = 'SELECT * FROM postcodes_copy_sample WHERE id>=%s and id<%s'
        # fetch all valid information from database and put them in task_queue
        results = self.dbwrapper.db_fetchall(sql, [self.current_point, self.current_point+self.fetch_step])

        # there are many items remain
        if len(results) > 0:
            for r in results:
                self.location.id = r[0]
                self.location.postcode = r[1]
                self.location.state = r[2]
                self.location.region = r[3]
                self.location.suburb = r[5]

                # deal with items with NULL informaion
                if self.location.region == None:
                    print ('[*]An item fetched from database is abondoned: id:%s, postcode:%s, state:%s, suburb:%s' %
                                                                                               (self.location.id,
                                                                                                self.location.postcode,
                                                                                                self.location.state,
                                                                                                self.location.suburb))
                else:
                    # put without blocking, transform instance into json to make it more general
                    try:
                        self.task_queue.put_nowait(json.dumps(self.location, default=Location.location2dict))
                    except asyncio.queues.QueueFull as e:
                        print('[!]Error: ', e)
                self.location.data_clean()

            self.current_point += self.fetch_step
            print ('[*]Successfully fetch %s items from database and put %s items in task queue.' % (self.fetch_step, self.task_queue.qsize()))
        # finish fetching all the items
        else:
            # should let the crawler know that :)
            return 'done'

    def work_sold_history(self):
        sql = 'SELECT * FROM postcodes_copy_sample WHERE id>=%s and id<%s'
        # fetch all valid information from database and put them in task_queue
        results = self.dbwrapper.db_fetchall(sql, [self.current_point, self.current_point+self.fetch_step])

        # there are many items remain
        if len(results) > 0:
            for r in results:
                self.location.id = r[0]
                self.location.postcode = r[1]
                self.location.state = r[2]
                self.location.region = r[3]
                self.location.suburb = r[5]
                # deal with items with NULL informaion
                if self.location.region == None:
                    print ('[*]An item fetched from database is abondoned: id:%s, postcode:%s, state:%s, suburb:%s' %
                                                                                               (self.location.id,
                                                                                                self.location.postcode,
                                                                                                self.location.state,
                                                                                                self.location.suburb))
                else:
                    # put without blocking, transform instance into json to make it more general
                    try:
                        self.task_queue.put_nowait(json.dumps(self.location, default=Location.location2dict))
                    except asyncio.queues.QueueFull as e:
                        print('[!]Error: ', e)
                self.location.data_clean()
            self.current_point += self.fetch_step
            print ('[*]Successfully fetch %s items from database and put %s items in task queue.' % (self.fetch_step, self.task_queue.qsize()))
        # finish fetching all the items
        else:
            # should let the crawler know that :)
            return 'done'

    def work_latest_history(self):
        toplist = 50
        if self.current_point < toplist:
            try:
                toplimit = min((self.current_point+self.fetch_step-1), toplist)
                for i in range(self.current_point, toplimit+1):
                    self.task_queue.put_nowait(i)
                self.current_point = toplimit + 1
            except asyncio.queues.QueueFull as e:
                print('[!]Error: ', e)
        else:
            return 'done'

    def work_corelogic_auction(self):
        states = ['VIC', 'NSW', 'QLD', 'SA', 'WA', 'NT', 'ACT', 'TAS']
        if self.current_point < len(states)-1:
            try:
                for _ in range(min(len(states),self.task_queue.maxsize)):
                    self.task_queue.put_nowait(states[self.current_point-1])
                    self.current_point += 1
                print('[*]Put %s items in task queue' % min(len(states), self.task_queue.maxsize))
            except asyncio.queues.QueueFull as e:
                print('[!]Error: ', e)
        else:
            return 'done'

    # receive results of suburbs' median price
    def result_median_price(self):
        sql_insert = 'INSERT INTO median_price_suburb (postcodes_id, house_buy, house_buy_update_date, house_buy_2br, house_buy_3br, house_buy_4br, ' \
                                                                    'house_rent, house_rent_update_date, house_rent_2br, house_rent_3br, house_rent_4br, ' \
                                                                    'unit_buy, unit_buy_update_date, unit_buy_2br, unit_buy_3br, unit_buy_4br,' \
                                                                    'unit_rent, unit_rent_update_date, unit_rent_2br, unit_rent_3br, unit_rent_4br,' \
                                                                    'trend, update_date) ' \
                                                'VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        self.result_queue_qsize = self.result_queue.qsize()
        for _ in range(self.result_queue_qsize):
            # get result with blocking,
            # because if not blocking for queue.get(), there may be a case that the latest results may beyond the upper size limit of queue
            try:
                self.median_price_sub = json.loads(self.result_queue.get_nowait(), object_hook=self.median_price_sub.dict2mediprice)
                self.result_queue.task_done()
            except asyncio.queues.QueueEmpty as e:
                print('[!]Error: ', e)
            except ValueError as e:
                print('[!]Error: ', e)

            self.dbwrapper.db_execute(sql_insert, [self.median_price_sub.postcodes_id,
                                                    self.median_price_sub.house_buy, self.median_price_sub.house_buy_update_date, self.median_price_sub.house_buy_2br, self.median_price_sub.house_buy_3br, self.median_price_sub.house_buy_4br,
                                                    self.median_price_sub.house_rent, self.median_price_sub.house_rent_update_date, self.median_price_sub.house_rent_2br, self.median_price_sub.house_rent_3br, self.median_price_sub.house_rent_4br,
                                                    self.median_price_sub.unit_buy, self.median_price_sub.unit_buy_update_date, self.median_price_sub.unit_buy_2br, self.median_price_sub.unit_buy_3br, self.median_price_sub.unit_buy_4br,
                                                    self.median_price_sub.unit_rent, self.median_price_sub.unit_rent_update_date, self.median_price_sub.unit_rent_2br, self.median_price_sub.unit_rent_3br, self.median_price_sub.unit_rent_4br,
                                                    self.median_price_sub.trend, self.median_price_sub.update_date])

        print('[*]Successfully get %s items from result queue and save them in database.' % self.result_queue_qsize)

    # receive results of suburbs' position
    def result_suburb_position(self):
        sql_insert = 'INSERT INTO median_price_suburb_copy_sample (postcodes_id, position) VALUE (%s, %s)'
        sql_update = 'UPDATE median_price_suburb_copy_sample SET position=%s WHERE postcodes_id=%s'

        self.result_queue_qsize = self.result_queue.qsize()
        for _ in range(self.result_queue_qsize):
            try:
                self.median_price_sub = json.loads(self.result_queue.get_nowait(), object_hook=self.median_price_sub.dict2mediprice)
                self.result_queue.task_done()
            except asyncio.queues.QueueEmpty as e:
                print('[!]Error: ', e)
            except ValueError as e:
                print('[!]Error: ', e)

            r = self.dbwrapper.db_execute(sql_update, [self.median_price_sub.position, self.median_price_sub.postcodes_id])
            # examine if this suburb had a record
            if r == 0:
                self.dbwrapper.db_execute(sql_insert, [self.median_price_sub.postcodes_id, self.median_price_sub.position])
        print('[*]Successfully get %s items from result queue and save them in database.' % self.result_queue_qsize)


    # receive results of properties' sold history
    def result_sold_history(self):
        sql_insert = 'INSERT INTO overall_sold_history_sample (suburb_id, full_address, property_type, price,'  \
                                                        'bedrooms, bathrooms, carspaces, sold_date, agent, url, update_date) ' \
                                                        'VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        sql_update = 'UPDATE overall_sold_history_sample SET suburb_id=%s, full_address=%s, property_type=%s, price=%s,' \
                                            'bedrooms=%s, bathrooms=%s, carspaces=%s, sold_date=%s, agent=%s, update_date=%s ' \
                                            'WHERE url=%s'
        self.result_queue_qsize = self.result_queue.qsize()
        for _ in range(self.result_queue_qsize):
            try:
                suburb_sold_history = json.loads(self.result_queue.get_nowait())
                for key, value in suburb_sold_history.items():
                    for i in range(len(value)):
                        self.propt_sold_history = json.loads(value[i], object_hook=self.propt_sold_history.dict2soldhistory)
                        self.dbwrapper.db_execute(sql_insert, [self.propt_sold_history.suburb_id, self.propt_sold_history.full_address,
                                                                   self.propt_sold_history.property_type, self.propt_sold_history.price,
                                                                   self.propt_sold_history.bedrooms, self.propt_sold_history.bathrooms,
                                                                   self.propt_sold_history.carspaces, self.propt_sold_history.sold_date,
                                                                   self.propt_sold_history.agent, self.propt_sold_history.url, self.propt_sold_history.update_date])
                self.result_queue.task_done()
            except asyncio.queues.QueueEmpty as e:
                print('[!]Error: ', e)
            except ValueError as e:
                print('[!]Error: ', e)
            except mysql.connector.Error as e:
                with open(DB_SAVE_FAIL_PATH, 'a') as f:
                    f.write('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n' %
                               (self.propt_sold_history.suburb_id, self.propt_sold_history.full_address,
                                   self.propt_sold_history.property_type, self.propt_sold_history.price,
                                   self.propt_sold_history.bedrooms, self.propt_sold_history.bathrooms,
                                   self.propt_sold_history.carspaces, self.propt_sold_history.sold_date,
                                   self.propt_sold_history.agent, self.propt_sold_history.url, self.propt_sold_history.update_date))
        print('[*]Successfully get %s items from result queue and save them in database.' % self.result_queue_qsize)

    def result_latest_history(self):
        sql_insert = 'INSERT INTO overall_sold_history_increment (state, postcode, suburb, full_address, property_type, price,'  \
                                                        'bedrooms, bathrooms, carspaces, sold_date, agent, url, update_date) ' \
                                                        'VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

        self.result_queue_qsize = self.result_queue.qsize()
        for _ in range(self.result_queue_qsize):
            try:
                suburb_sold_history = json.loads(self.result_queue.get_nowait())
                for key, value in suburb_sold_history.items():
                    for i in range(len(value)):
                        self.propt_sold_history = json.loads(value[i], object_hook=self.propt_sold_history.dict2soldhistory)
                        # examine if this property has had the record
                        # if r == 0:
                        self.dbwrapper.db_execute(sql_insert, [self.propt_sold_history.state,
                                                               self.propt_sold_history.postcode,
                                                               self.propt_sold_history.suburb,
                                                               self.propt_sold_history.full_address,
                                                               self.propt_sold_history.property_type,
                                                               self.propt_sold_history.price,
                                                               self.propt_sold_history.bedrooms,
                                                               self.propt_sold_history.bathrooms,
                                                               self.propt_sold_history.carspaces,
                                                               self.propt_sold_history.sold_date,
                                                               self.propt_sold_history.agent,
                                                               self.propt_sold_history.url,
                                                               self.propt_sold_history.update_date])
                self.result_queue.task_done()
            except asyncio.queues.QueueEmpty as e:
                print('[!]Error: ', e)
            except ValueError as e:
                print('[!]Error: ', e)
            except mysql.connector.Error as e:
                with open(DB_SAVE_FAIL_PATH, 'a') as f:
                    f.write('%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s\n' %
                               (self.propt_sold_history.state, self.propt_sold_history.postcode,
                                self.propt_sold_history.suburb, self.propt_sold_history.full_address,
                                self.propt_sold_history.property_type, self.propt_sold_history.price,
                                self.propt_sold_history.bedrooms, self.propt_sold_history.bathrooms,
                                self.propt_sold_history.carspaces, self.propt_sold_history.sold_date,
                                self.propt_sold_history.agent, self.propt_sold_history.url,
                                self.propt_sold_history.update_date))
        print('[*]Successfully get %s items from result queue and save them in database.' % self.result_queue_qsize)

    def result_corelogic_auction(self):
        sql_insert = 'INSERT INTO corelogic_auction_results (state,scheduled_auctions,results_available,clearance_rate,sold_prior_to_auction,' \
                     'sold_at_auction,sold_after_auction,withdrawn,passed_in,auction_date,update_date) ' \
                     'VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        self.result_queue_qsize = self.result_queue.qsize()
        auction_result = Auction_result()

        for _ in range(self.result_queue_qsize):
            try:
                auction_result = json.loads(self.result_queue.get_nowait(), object_hook=auction_result.dict2auction)
                self.dbwrapper.db_execute(sql_insert, [auction_result.state,
                                                       auction_result.scheduled_auctions,
                                                       auction_result.results_available,
                                                       auction_result.clearance_rate,
                                                       auction_result.sold_prior_to_auction,
                                                       auction_result.sold_at_auction,
                                                       auction_result.sold_after_auction,
                                                       auction_result.withdrawn,
                                                       auction_result.passed_in,
                                                       auction_result.auction_date,
                                                       auction_result.update_date])
                self.result_queue.task_done()
            except asyncio.queues.QueueEmpty as e:
                print('[!]Error: ', e)
            except ValueError as e:
                print('[!]Error: ', e)
            except mysql.connector.Error as e:
                with open(DB_SAVE_FAIL_PATH, 'a') as f:
                    f.write('state=%s\n' % acution_result.state)
        print('[*]Successfully get %s items from result queue and save them in database.' % self.result_queue_qsize)

    # re-fetch the pages which are not crawled in the first place
    def exceptions_median_price(self, lines):
        valid_line = re.compile(r'^TASK [%s]: CANNOT_CRAWL: id=(\d+), postcode=(\d+), suburb=([A-Za-z0-9\s]+), state=(VIC|NSW|QLD|TAS|SA|WA|ACT|NT)' % self.task_id)
        #for line in lines:
        while self.excp_point < len(lines):
            if valid_line.match(lines[self.excp_point]) != None:
                info = valid_line.match(lines[self.excp_point]).groups()
                self.location.id = info[0]
                self.location.postcode = info[1]
                self.location.suburb = info[2]
                self.location.state = info[3]
                # put without blocking, transform instance into json to make it more general
                try:
                    self.task_queue.put_nowait(json.dumps(self.location, default=Location.location2dict))
                except asyncio.queues.QueueFull as e:
                    print('[!]Error: ', e)
                self.location.data_clean()
            self.excp_point +=1
            if self.task_queue.full() == True:
                print ('[*]Successfully extract %s items from exceptions and put them in task queue.' % self.task_queue.qsize())
                break
        # all exceptions has been read and extract
        else:
            if self.task_queue.empty() == False:
                print ('[*]Successfully extract %s items from exceptions and put them in task queue.' % self.task_queue.qsize())
            # should let the crawler know that :)
            if self.task_queue.empty() == True:
                return 'done'

    # re-fetch the pages which are not crawled in the first place
    def exceptions_suburb_position(self, lines):
        valid_line = re.compile(r'^TASK [%s]: CANNOT_CRAWL: id=(\d+), postcode=(\d+), suburb=([A-Za-z0-9\s]+), state=(VIC|NSW|QLD|TAS|SA|WA|ACT|NT)' % self.task_id)
        #for line in lines:
        while self.excp_point < len(lines):
            if valid_line.match(lines[self.excp_point]) != None:
                info = valid_line.match(lines[self.excp_point]).groups()
                self.location.id = info[0]
                self.location.postcode = info[1]
                self.location.suburb = info[2]
                self.location.state = info[3]
                # put without blocking, transform instance into json to make it more general
                try:
                    self.task_queue.put_nowait(json.dumps(self.location, default=Location.location2dict))
                except asyncio.queues.QueueFull as e:
                    print('[!]Error: ', e)
                self.location.data_clean()
            self.excp_point +=1
            if self.task_queue.full() == True:
                print ('[*]Successfully extract %s items from exceptions and put them in task queue.' % self.task_queue.qsize())
                break
        # all exceptions has been read and extract
        else:
            if self.task_queue.empty() == False:
                print ('[*]Successfully extract %s items from exceptions and put them in task queue.' % self.task_queue.qsize())
            # should let the crawler know that :)
            if self.task_queue.empty() == True:
                return 'done'

    def exceptions_sold_history(self, lines):
        valid_line = re.compile(r'^TASK [%s]: CANNOT_CRAWL: suburb_id=(\d+), postcode=(\d+), suburb=([A-Za-z0-9\s]+), '
                                r'state=(VIC|NSW|QLD|TAS|SA|WA|ACT|NT), url=([A-Za-z0-9+-=&%?.]+)' % self.task_id)
        #for line in lines:
        while self.excp_point < len(lines):
            if valid_line.match(lines[self.excp_point]) != None:
                info = valid_line.match(lines[self.excp_point]).groups()
                self.location.id = info[0]
                self.location.postcode = info[1]
                self.location.suburb = info[2]
                self.location.state = info[3]
                # put without blocking, transform instance into json to make it more general
                try:
                    self.task_queue.put_nowait(json.dumps(self.location, default=Location.location2dict))
                except asyncio.queues.QueueFull as e:
                    print('[!]Error: ', e)
                self.location.data_clean()
            self.excp_point +=1
            if self.task_queue.full() == True:
                print ('[*]Successfully extract %s items from exceptions and put them in task queue.' % self.task_queue.qsize())
                break
        # all exceptions has been read and extract
        else:
            if self.task_queue.empty() == False:
                print ('[*]Successfully extract %s items from exceptions and put them in task queue.' % self.task_queue.qsize())
            # should let the crawler know that :)
            if self.task_queue.empty() == True:
                return 'done'

    def exceptions_latest_history(self, lines):
        valid_line = re.compile(r'^TASK [%s]: CANNOT_CRAWL: list_id=(\d+)' % self.task_id)
        #for line in lines:
        while self.excp_point < len(lines):
            if valid_line.match(lines[self.excp_point]) != None:
                info = valid_line.match(lines[self.excp_point]).groups()
                list_id = info[0]
                # put without blocking, transform instance into json to make it more general
                try:
                    self.task_queue.put_nowait(list_id)
                except asyncio.queues.QueueFull as e:
                    print('[!]Error: ', e)
            self.excp_point +=1
            if self.task_queue.full() == True:
                print ('[*]Successfully extract %s items from exceptions and put them in task queue.' % self.task_queue.qsize())
                break
        # all exceptions has been read and extract
        else:
            if self.task_queue.empty() == False:
                print ('[*]Successfully extract %s items from exceptions and put them in task queue.' % self.task_queue.qsize())
            # should let the crawler know that :)
            if self.task_queue.empty() == True:
                return 'done'

    # assign works for coroutines through queue
    def assign_work(self):
        # ensure all the tasks have been done
        if self.task_queue.empty() != True:
            print ('[!]Error: task queue is not empty, cannot put other tasks in')
            raise AttributeError
        for case in Switch(self.task_id):
            if case(1):
                r = self.work_median_price()
                break
            if case(2):
                r = self.work_suburb_position()
                break
            if case(3):
                r = self.work_sold_history()
                break
            if case(4):
                r = self.work_latest_history()
                break
            if case(5):
                r = self.work_corelogic_auction()
                break
            if case():
                print('[!]Error: invalid task id')
        if r == 'done':
            return 'done'

    # receive results from coroutines through queue
    def rev_result(self):
        for case in Switch(self.task_id):
            if case(1):
                self.result_median_price()
                break
            if case(2):
                self.result_suburb_position()
                break
            if case(3):
                self.result_sold_history()
                break
            if case(4):
                self.result_latest_history()
                break
            if case(5):
                self.result_corelogic_auction()
                break
            if case():
                print('[!]Error: invalid task id')

        # Abondon the tasks when we have triggered the anti-spider mechanism of the web server.
        if self.result_queue_maxsize >= 64 and self.result_queue_qsize < round(0.38 * self.result_queue_maxsize):
            return 'abondon'
        else:
            return 'done'

    # deal with exceptions, re-fetch the pages that are not be crawled in the normal work
    def manage_exceptions(self, exceptions):
        if isinstance(exceptions, list) != True:
            print ('[!]Error: read exceptions error')
            raise TypeError
        if exceptions == []:
            print('[*] Terrific! All the pages are crawled successfully')
            return 'done'

        for case in Switch(self.task_id):
            if case(1):
                r = self.exceptions_median_price(exceptions)
                break
            if case(2):
                r = self.exceptions_suburb_position(exceptions)
                break
            if case(3):
                r = self.exceptions_sold_history(exceptions)
                break
            if case(4):
                r = self.exceptions_latest_history(exceptions)
                break
            if case(5):
                print('[!]EXCEPTION: No action temporarily')
                r = 'done'
                break
            if case():
                print('[!]Error: invalid task id')
        if r == 'done':
            return 'done'
        else:
            return 'continue'



