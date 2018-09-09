#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
add_subid() is the function that could get suburb id from database (table 'postcodes') according to 'state', 'postcode'
and 'suburb' in table 'overall_sold_history_increment', then add it into table 'overall_sold_history_increment'.
'''

from db import DBWrapper
from db import DBConn
import datetime
import re

TB_POSTCODES = 'postcodes'
TB_INCREM = 'overall_sold_history_increment'

class INCRM_PROS(object):
    def __init__(self):
        pass

    def add_subid(self, *, date=None):
        dbwrapper = DBWrapper()
        if date != None:
            date_format = re.compile(r'(\d{4})\-(\d{1,2})\-(\d{1,2})')
            date_YMD = date_format.match(date).groups()
            date_YMD = list(map(int,date_YMD))
            if len(date_YMD) != 3:
                print('[!]Eror: Optional param of add_subid requires a string like \'2017-1-1\'')
                raise TypeError
            if date_YMD[0]>1900 and date_YMD[1]<=12 and date_YMD[2]<=31 and date_YMD[1]>0 and date_YMD[2]>0:
                pass
            else:
                print('[!]Eror: Optional param of add_subid requires a string like \'2017-1-1\'')
                raise ValueError
        else:
            sql_latestdate = 'SELECT MAX(update_date) FROM %s' % TB_INCREM
            latest_date = dbwrapper.db_fetchall(sql_latestdate)
            date = latest_date[0][0].strftime('%Y-%m-%d')
        sql_gethistory = 'SELECT id,state,postcode,suburb FROM %s WHERE suburb_id is NULL AND update_date=\'%s\''
        latest_history = dbwrapper.db_fetchall(sql_gethistory % (TB_INCREM, date))
        sql_getsubid = 'SELECT id FROM %s WHERE state=\'%s\' AND postcode=\'%s\' AND suburb=\'%s\''
        sql_updatesubid = 'UPDATE %s SET suburb_id=%s WHERE id=%s'

        with DBConn() as db:
            for item in latest_history:
                suburb_id = db.fetchall(sql_getsubid % (TB_POSTCODES, item[1], item[2], item[3]))
                print('[*]id=%s, state=%s, postcode=%s, suburb=%s' % (item[0], item[1], item[2], item[3]))
                if len(suburb_id) != 1 or len(suburb_id[0]) !=1:
                    print('[*]Error: Fetch suburb_id failed: id=%s, state=%s, postcode=%s, suburb=%s' % (item[0], item[1], item[2], item[3]))
                else:
                    suburb_id = suburb_id[0][0]
                    db.execute(sql_updatesubid % (TB_INCREM, suburb_id, item[0]))

    def upper(self):
        sql_get = 'SELECT id,suburb FROM overall_sold_history_increment WHERE id<=1000'
        sql_update = 'UPDATE overall_sold_history_increment SET suburb=\'%s\' WHERE id=%s'
        with DBConn() as db:
            history = db.fetchall(sql_get)
            for item in history:
                db.execute(sql_update % (item[1].upper(), item[0]))

    def distct_history(self):
        sql_startid = 'SELECT MIN(id) FROM %s WHERE processed_date IS NULL'

        sql_getid = 'SELECT id FROM %s WHERE id NOT IN (SELECT MIN(id) FROM overall_sold_history_increment GROUP BY url) AND id>=%s'
        sql_delete = 'DELETE FROM %s WHERE id=%s'
        sql_date = 'UPDATE %s SET processed_date=\'%s\' WHERE processed_date IS NULL'
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        with DBConn() as db:
            startid = db.fetchall(sql_startid % TB_INCREM)[0][0]
            if startid != None:
                id_set = db.fetchall(sql_getid % (TB_INCREM, startid))
                for id in id_set:
                    db.execute(sql_delete % (TB_INCREM, id[0]))
                    print('[*]Deleting id=%s' % id[0])
                db.execute(sql_date % (TB_INCREM, current_date))
            else:
                print('[*]No new record is found.')


if __name__ == '__main__':
    incrm_pros = INCRM_PROS()
    incrm_pros.distct_history()
    incrm_pros.add_subid()



