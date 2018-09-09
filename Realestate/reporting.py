#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
This .py is for the needs that recording crawl result, exception, status, and so on.
'''

import io
import time
import re
from functools import reduce

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
                file.write('TIME CONSUMPTION (OVERALL): %s s\n' % spider.time_consumption)
                file.write('TASK ID: %s\n' % spider.task_id)
                file.write('MAX TASKS: %s\n' % spider.max_tasks)
                file.write('MAX TRIES: %s\n' % spider.max_tries)
                if spider.check_exceptions == 1:
                    file.write('CHECK EXCEPTIONS: ENABLE\n')
                elif spider.check_exceptions == 0:
                    file.write('CHECK EXCEPTIONS: DISABLE\n')
                file.write('TIMEOUT: %s s\n' % spider.timeout)
                file.write('DONE: %s\n' % spider.done)
                file.write('FAILED: %s\n' % spider.failed)
                file.write('SPEED: %s item/s\n' % ((spider.done+spider.failed)/spider.time_consumption))
                file.write('TOTAL COUNT: %s\n\n' % (spider.done+spider.failed))
            except AttributeError as e:
                print('[!]Error: unable to write to the file for exception', e)
            except io.UnsupportedOperation as e:
                print('[!]Error: unable to write to the file for exception', e)
        else:
                print('[!]Error: unable to write to the file for exception, because no handler')

    # read the last block of exception
    def read_exception(self, file=None):
        if not isinstance(file, io.TextIOWrapper):
            print ('[!]Error: Report.read_exception \'s parameter invalid')
            raise TypeError
        lines = []
        # get the size of the file and set the cursor at the end
        cursor_last = file.seek(0,2)
        cursor = cursor_last
        step = 1024
        topofblock = re.compile(r'^\*\*\*\*\*[\*]+$')
        # read the last block which may contain information that is not belong to it
        integrated = 0
        while integrated == 0:
            if cursor >= step:
                cursor = cursor - step
                file.seek(cursor, 0)
                # set the cursor to the begining of a line
                file.readline()
            else:
                cursor = 0
                file.seek(cursor, 0)
            i = 0
            while cursor_last > file.tell():
                # if f.readline() is valid, put it in lines
                current_line = file.readline()
                if topofblock.match(current_line) != None:
                    integrated = 1
                lines.insert(i, current_line)
                i += 1
            else:
                cursor_last = cursor
        # deal with the invalid information which must be at the top
        line_num = len(lines)
        first_line = 0
        for i in range(line_num):
            if topofblock.match(lines[i]) != None:
                first_line = i
        for _ in range(first_line + 1):
            lines.pop(0)
        # delete those same lines
        clean_lines = lambda x,y: x if y in x else x + [y]
        lines = reduce(clean_lines, [[], ] + lines)
        return lines


