#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
This .py is for the needs that recording crawl result, exception, status, and so on.
'''
__author__ = 'zht'

import io
import time

class Report(object):
    def __init__(self):
        pass

    def write_exception(self, exception_type, item, file=None):
        if file != None:
            if isinstance(exception_type, str) and isinstance(item, str):
                try:
                    file.write('%s: %s\n' % (exception_type, item))
                except AttributeError as e:
                    print('[!]Error: unable to write to the file for exception', e)
                except io.UnsupportedOperation as e:
                    print('[!]Error: unable to write to the file for exception', e)
        else:
            print('[!]Error: unable to write to the file for exception, because no handler')

    def summary(self, spider, file=None):
        if file != None:
            try:
                file.write('-------------------------  SUMMARY  ---------------------------\n')
                file.write('START@ %s\n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(spider.start_time)))
                file.write('END@ %s\n' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(spider.end_time)))
                file.write('TIME CONSUMPTION: %s s\n' % (spider.end_time-spider.start_time))
                file.write('MAX TASKS: %s\n' % spider.max_tasks)
                file.write('MAX TRIES: %s\n' % spider.max_tries)
                file.write('TIMEOUT: %s s\n' % spider.timeout)
                file.write('DONE: %s\n' % spider.done)
                file.write('FAILED: %s\n' % spider.failed)
                file.write('SPEED: %s item/s\n' % ((spider.done+spider.failed)/(spider.end_time-spider.start_time)))
                file.write('TOTAL COUNT: %s\n\n' % (spider.done+spider.failed))
            except AttributeError as e:
                print('[!]Error: unable to write to the file for exception', e)
            except io.UnsupportedOperation as e:
                print('[!]Error: unable to write to the file for exception', e)
        else:
                print('[!]Error: unable to write to the file for exception, because no handler')


