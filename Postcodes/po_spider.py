#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Get postcodes relevant information from websites
The info including the postcode, state and suburb has acquired and saved into MySQL. This program is to crawl the corresponding info including region and phone area, etc.
Tested in python 3.5.2 in ubuntu 16.0.4. Some functions may not run successfully in other python and linux versions.
'''

#import threading
import asyncio
import spidering
import time

def main():
    MAX_TASKS = 8
    MAX_TRIES = 2
    TIMEOUT = 30

    print ('[*]Spider Begin')
    start_time = time.time()

    # Create a loop. If run in windows and mac OS, you may need to use set_event_loop().
    loop = asyncio.get_event_loop()
    spider = spidering.Spider(MAX_TRIES, MAX_TASKS, TIMEOUT)

    try:
        # spider gonna crawl
        loop.run_until_complete(spider.crawl())

    # interrupt from keyboard by Ctrl+C
    except KeyboardInterrupt:
        spider.close()

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
