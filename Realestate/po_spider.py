#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Get postcode relevant information from websites
Postcode, state and suburb have involved in the table, other information such as region and phone area need to be deal with in this step.
Tested in python 3.5.2 in ubuntu 16.0.4. Some functions may not run successfully in other python and linux versions.
'''

#import threading
import asyncio
import spidering
import time
import resuming
from contextlib import suppress
import argparse

def readargs():
    parser = argparse.ArgumentParser(description='Spider is crawling')
    parser.add_argument('-t', '--task_id', required=True,
                        help='task id')
    parser.add_argument('-a', '--max_tasks', default=8,
                        help='max tasks, default=8')
    parser.add_argument('-r', '--max_tries', default=2,
                        help='max tries, default=2')
    parser.add_argument('-i', '--timeout', default=30,
                        help='timeout of aiohttp, default=30')
    parser.add_argument('-q', '--queue_size', default=128,
                        help='queue size, default=128, highly recommend setting it to 64 if running task 4')
    parser.add_argument('-E', '--check_exception', action='store_true', default=1,
                        help='check exception, default=1 (0=False, 1=True)')
    args = parser.parse_args()
    print('TASK ID = %s' % args.task_id)
    print('MAX TASKS = %s' % args.max_tasks)
    print('MAX TRIES = %s' % args.max_tries)
    print('TIMEOUT = %s' % args.timeout)
    print('QUEUE SIZE = %s' % args.queue_size)
    print('CHECK EXCEPTION = %s' % args.check_exception)
    args.task_id = int(args.task_id)
    args.max_tasks = int(args.max_tasks)
    args.max_tries = int(args.max_tries)
    args.timeout = int(args.timeout)
    args.queue_size = int(args.queue_size)
    args.check_exception = int(args.check_exception)
    return args

def main():
    MAX_TASKS = 8
    MAX_TRIES = 2
    CHECK_EXCEPTIONS = 1        #ENABLE=1  DISABLE=0
    TIMEOUT = 30
    TASK_ID = 1

    args = readargs()
    print ('[*]Spider Begin')
    start_time = time.time()

    # Create a loop. If run in windows and mac OS, you may need to use set_event_loop().
    loop = asyncio.get_event_loop()
    spider = spidering.Spider(args.task_id, args.max_tries, args.max_tasks, args.check_exception, args.timeout, args.queue_size)

    try:
        # spider gonna crawl
        loop.run_until_complete(spider.crawl())

    # interrupt from keyboard by Ctrl+C
    except KeyboardInterrupt:
        print ('Interrupt from keyboard')

        spider.close()
        spider.wait_cancelled()
        pending = asyncio.Task.all_tasks()

        for w in pending:
            w.cancel()
            with suppress(asyncio.CancelledError):
                loop.run_until_complete(w)

    finally:
        # next two lines are required for actual aiohttp resource cleanup
        loop.stop()
        loop.run_forever()

        loop.close()
        print ('[*]Spider End')
        end_time = time.time()
        print ('Time consumed: %s s' % (end_time-start_time))

if __name__ == '__main__':
    main()
