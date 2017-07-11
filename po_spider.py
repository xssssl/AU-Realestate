#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Get postcode relevant information from websites
Postcode, state and suburb have involved in the table, other information such as region and phone area need to be deal with in this step.
Tested in python 3.5.2 in ubuntu 16.0.4. Some functions may not run successfully in other python and linux versions.
'''
__author__ = 'zht'

#import threading
import asyncio
import spidering
import time
import resuming

def main():
    MAX_TASKS = 1
    MAX_TRIES = 2
    CHECK_EXCEPTIONS = 1        #ENABLE=1  DISABLE=0
    TIMEOUT = 30
    TASK_ID = 1

    print ('[*]Spider Begin')
    start_time = time.time()

    # Create a loop. If run in windows and mac OS, you may need to use set_event_loop().
    loop = asyncio.get_event_loop()
    spider = spidering.Spider(TASK_ID, MAX_TRIES, MAX_TASKS, CHECK_EXCEPTIONS, TIMEOUT)

    # debug
    #resume = resuming.Resume()
    #with open(RESUMING_PATH, 'rb') as f:
    #    spider = resume.verify(f, object_hook=spider.dict2spider, **spider.spider2dict())


    try:
        # spider gonna crawl
        loop.run_until_complete(spider.crawl())

    # interrupt from keyboard by Ctrl+C
    except KeyboardInterrupt:
        print ('Interrupt from keyboard')
        ### store all the stack in a file
        ### to do someing
        user_input = input ('[*]Do you want to stop and quit? [Y/N]: ')
        spider.close()

        # use report to print some results
        pass

    finally:
        # make a report
        #reporting.report(spider)

        # next two lines are required for actual aiohttp resource cleanup
        loop.stop()
        loop.run_forever()

        loop.close()
        print ('[*]Spider End')
        end_time = time.time()
        print ('Time consumed: %s s' % (end_time-start_time))

if __name__ == '__main__':
    main()