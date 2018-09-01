#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Class DBWrapper wrap some commands that related to the interaction of MySQL.
You can save date with 'execute' and fetch information with 'fetchall'
without worrying about problems like how to connect the database and how to safely interact with the database.

Class DBConn is mainly for the scenario that need to execute or fetch frequently.
The Class DBWrapper would be inefficient in this scenario, as it would establish and close the connection
once it run 'execute' or 'fetch'.
'''

import mysql.connector
import threading

# Information that you need when log in MySQL. Need to change to your configuration.
DB_USER = 'root'
DB_PASSWORD = '1234'
DB_DATABASE = 'au_realestate'
CLIENT_FLAGS = [mysql.connector.constants.ClientFlag.FOUND_ROWS, ]

class DBWrapper():
    def __init__(self, multithread = False):
        self.__multithread = multithread
        # when use multithread, we need lock()
        if self.__multithread:
            self.__lock = threading.RLock()

    def db_conn(self):
        # seting client_flags to FOUND_ROWS option makes cursor.rowcount return the number of matched rows instead
        # of returning the changed rows by default. So we could know if there are matched rows when using 'UPDATE'.
        self.__conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE, client_flags=CLIENT_FLAGS)
        self.__cursor = self.__conn.cursor()

    # commit to mysql
    def db_commit(self):
        self.__conn.commit()

    # close cursor and connection
    def db_close(self):
        self.__cursor.close()
        self.__conn.close()

    # safely use database
    def conn_safe(func):
        def connection(self,*args,**kwargs):
            if self.__multithread:
                self.__lock.acquire()
            self.db_conn()
            #kwargs['conn'] = conn
            rs = func(self,*args,**kwargs)
            self.db_close()
            if self.__multithread:
                self.__lock.release()
            return rs
        return connection

    @conn_safe
    def db_execute(self, sql, args=[]):
        # check args type
        if isinstance(args, list) == False:
            raise TypeError ('Invalid type in %s: args' % __name__)
            return
        # execute
        try:
            self.__cursor.execute(sql, args)
        except SyntaxError as e:
            print ('[!]Error: ', e)
            raise
        except mysql.connector.Error as e:
            print ('[!]Error: ', e, args)
            raise
        self.db_commit()
        return self.__cursor.rowcount

    @conn_safe
    def db_fetchall(self, sql, args = []):
        # check args type
        if isinstance(args, list) == False:
            raise TypeError('Invalid type in %s: args' % __name__)
            return
        # execute
        try:
            self.__cursor.execute(sql, args)
        except SyntaxError as e:
            print ('[!]Error: ', e)
            raise
        except mysql.connector.Error as e:
            print ('[!]Error: ', e)
            raise
        # fetchall
        values = []
        try:
            values = self.__cursor.fetchall()
        except mysql.connector.Error as e:
            print('[!]Error: ', e)
            raise
        return values

class DBConn(object):
    '''
    Sample:
        with DBConn(user='root', password='1234', database='sample_database') as db:
            rs = db.fetchall_noconn('select * from sample_table')
            print(rs)
    '''
    def __init__(self, multithread = False):
        self.__multithread = multithread
        # when use multithread, we need lock()
        if self.__multithread:
            self.__lock = threading.RLock()

    def db_conn(self):
        # seting client_flags to FOUND_ROWS option makes cursor.rowcount return the number of matched rows instead
        # of returning the changed rows by default. So we could know if there are matched rows when using 'UPDATE'.
        self.__conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE, client_flags=CLIENT_FLAGS)
        self.__cursor = self.__conn.cursor()

    # commit to mysql
    def db_commit(self):
        self.__conn.commit()

    # close cursor and connection
    def db_close(self):
        self.__cursor.close()
        self.__conn.close()

    def __enter__(self):
        self.db_conn()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db_close()
        if exc_type != None:
            print(exc_tb, exc_type, exc_val)

    def execute(self, sql, args=[]):
        # check args type
        if isinstance(args, list) == False:
            raise TypeError ('Invalid type of args, it should be a list')
            return
        # execute
        try:
            self.__cursor.execute(sql, args)
        except SyntaxError as e:
            print('[!]Error: ', e)
            raise
        except mysql.connector.Error as e:
            print('[!]Error: ', e, args)
            raise
        self.db_commit()
        return self.__cursor.rowcount

    def fetchall(self, sql, args=[]):
        # check args type
        if isinstance(args, list) == False:
            raise TypeError('Invalid type of args, it should be a list')
            return
        # execute
        try:
            self.__cursor.execute(sql, args)
        except SyntaxError as e:
            print('[!]Error: ', e)
            raise
        except mysql.connector.Error as e:
            print('[!]Error: ', e)
            raise
        # fetchall
        values = []
        try:
            values = self.__cursor.fetchall()
        except mysql.connector.Error as e:
            print('[!]Error: ', e)
            raise
        return values
