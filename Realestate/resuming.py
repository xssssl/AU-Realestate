#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
This .py is for resumption. Resumption is an important function specifically for those complicated and time-consuming tasks.
'''

import io
import pickle
import json

class Resume(object):
    def __init__(self):
        pass

    # save a dict which is serialized into a file
    def save2file(self, file, **kw):
        if not isinstance(file, io.BufferedWriter) and not isinstance(file, io.BufferedRandom):
            print ('[!]Error: Resume.save2file \'s parameters are invalid')
            raise TypeError
        try:
            kw = json.dumps(kw)
            pickle.dump(kw, file)
        except AttributeError as e:
            print('[!]Error: unable to write to the file for resumption', e)
        except io.UnsupportedOperation as e:
            print('[!]Error: unable to write to the file for resumption', e)

    # read a dict which is serialized from a file, in most cases it actually read a line
    def readfromfile(self, file):
        if not isinstance(file, io.BufferedReader) and not isinstance(file, io.BufferedRandom):
            print ('[!]Error: Resume.readfromfile \'s parameters are invalid')
            raise TypeError
        try:
            r = pickle.load(file)
        except AttributeError as e:
            print('[!]Error: unable to write to the file for resumption', e)
        except io.UnsupportedOperation as e:
            print('[!]Error: unable to write to the file for resumption', e)
        return r

    # resume, actually it apply json.loads() to deserialize the dict
    def recover(self, *args, **kwargs):
        return json.loads(*args,**kwargs)

    # compare the values of two dict that generate by function spider2dict() with the same length
    def cmp_dict(self, d1, d2):
        core_keys = ['task_id', 'max_tries', 'max_tasks', 'check_exceptions', 'timeout']
        if isinstance(d1, dict) and isinstance(d2, dict):
            if len(d1) == len(d2):
                diff = []
                for k in d1:
                    # only concern about the parameters in core_keys
                    if k in core_keys:
                        r = d2.get(k)
                        if r != None:
                            if r != d1.get(k):
                                diff.append(k)
                        else:
                            print ('[!]Error: two dict have different keys.')
                            raise AttributeError
                return diff
            else:
                print ('[!]Error: two dict have different length.')
                raise AttributeError
        else:
            print ('[!]Error: the two arguments must be dict.')
            raise TypeError

    # to verify the information stored for resuming and ask the user to check if resuming or not
    def verify(self, file, object_hook=None, **kw):
        # read the first line of the file, so u need to corresponding dict in the file
        try:
            res_spider = self.readfromfile(file)
            res_spider_dict = json.loads(res_spider)
        except EOFError as e:
            print ('[*]No information found for resuming: ', e)
            return None

        diff_params = self.cmp_dict(res_spider_dict, kw)
        while True:
            if 'task_id' in diff_params:
                print('[*]An unfinished task is found. But it\'s different from the current task.')
                user_input = input('[*]Do you want to resume from the last task and abondon the current one? [Y/N]: ')
                if user_input == 'Y' or user_input == 'y':
                    print('[*]Resume from the unfinished task.')
                    # begin to resume
                    # re = self.recover(res_spider, object_hook=object_hook)
                    return self.recover(res_spider, object_hook=object_hook)
                elif user_input == 'N' or user_input == 'n':
                    print('[*]Start a new task.')
                    # don't resume, start a new spider
                    return None
            else:
                print('[*]An unfinished task is found. It\'s the same task with the one you want to launch.')
                user_input = input('[*]Do you want to resume? [Y/N]: ')
                if user_input == 'Y' or user_input == 'y':
                    print('[*]Resume from the unfinished task.')
                    if len(diff_params) != 0:
                        for info in diff_params:
                            while True:
                                print ('[*]Argument \'%s\' is different in the unfinished task and the new task.' % info)
                                print ('    Unfinished task: %s = %s' % (info, res_spider_dict[info]))
                                print ('    New task: %s = %s' % (info, kw[info]))
                                user_input = input('[*]Do you want to apply the augument in the unfinished task? [Y/N]:')
                                if user_input == 'Y' or user_input == 'y':
                                    break
                                elif user_input == 'N' or user_input == 'n':
                                    res_spider_dict[info] = kw[info]
                                    break
                                else:
                                    print('[*]Invalid Input.')
                        res_spider = json.dumps(res_spider_dict)
                    return self.recover(res_spider, object_hook=object_hook)
                elif user_input == 'N' or user_input == 'n':
                    print('[*]Start a new task.')
                    # don't resume, start a new spider
                    return None
            print('[*]Invalid Input.')

    # to clear the file recording the information about resuming, u need to open the file with 'wb' or 'wb+'
    def cleanfile(self, file):
        if not isinstance(file, io.BufferedWriter) and not isinstance(file, io.BufferedRandom):
            print ('[!]Error: Resume.clearfile \'s parameters are invalid')
            raise TypeError
        file.truncate()
