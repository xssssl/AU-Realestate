#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Class Schedular is mainly for work assignment. Its responsibilities are below:
  * read data from database and put in a task queue
  * read data from result queue in which all the coroutines put data
  * read data from exceptions and put them in task queue again
  * cancel all the coroutines when all the task are done
'''

__author__ = 'zht'


import asyncio
from db import DBWrapper
import json
from spidering import Location


class Scheduler(object):
    def __init__(self, task_queue, result_queue, start_point = 1):
        self.location = Location()
        self.task_queue = task_queue
        self.result_queue = result_queue
        #self.start_id = start_id
        self.current_point = start_point
        self.fetch_step = self.task_queue.maxsize
        self.dbwrapper = DBWrapper()
        if self.task_queue.maxsize > self.result_queue.maxsize:
            raise ValueError ('The size of task queue is smaller than result queue.')

    # assign works for coroutines through queue
    def assign_work(self):
        sql = 'SELECT * FROM postcodes_copy WHERE id>=%s and id<%s'
        # fetch all valid information from database and put them in task_queue
        results = self.dbwrapper.db_fetchall(sql, [self.current_point, self.current_point+self.fetch_step])

        # there are many items remain
        if len(results) > 0:
            for r in results:
                self.location.id = r[0]
                self.location.postcode = r[1]
                self.location.state = r[2]
                self.location.suburb = r[5]
                # put without blocking, transform instance into json to make it more general
                try:
                    self.task_queue.put_nowait(json.dumps(self.location, default=Location.location2dict))
                except asyncio.queues.QueueFull as e:
                    print('[!]Error: ', e)
                self.location.data_clean()
            self.current_point += self.fetch_step
            print ('[*]Successfully fetch %s items from database and put them in task queue.' % self.fetch_step)
        # finish fetching all the items
        else:
            # should let the crawler know that :)
            return 'done'

    # receive results from coroutines through queue
    def rev_result(self):
        sql = 'UPDATE postcodes_copy SET region=%s, sub_region=%s, phone_area=%s, latitude=%s, longitude=%s WHERE id=%s'
        result_qsize = self.result_queue.qsize()
        for _ in range(result_qsize):
            # get result without blocking,
            # because if blocking for queue.get(), u can't ensure that the number of results could smaller than the upper size limit of queue
            try:
                self.location = json.loads(self.result_queue.get_nowait(), object_hook=self.location.dict2location)
                self.result_queue.task_done()
            except asyncio.queues.QueueEmpty as e:
                print('[!]Error: ', e)
            except ValueError as e:
                print('[!]Error: ', e)

            # save the result in database
            self.dbwrapper.db_execute(sql, [self.location.region, self.location.sub_region, self.location.phone_area,
                                            self.location.latitude, self.location.longitude, self.location.id])
        print('[*]Successfully get %s items from result queue and save them in database.' % result_qsize)

        # Abondon the tasks when we have triggered the anti-spider mechanism of the web server.
        if (self.result_queue.maxsize > 64):
            if (result_qsize < round(0.4 * self.result_queue.maxsize != result_qsize)):
                return 'abondon'
            else:
                return 'continue'



