# -*- coding:utf-8 -*-
import MySQLdb
import time
import datetime
import random
import logging
import copy
from pyhive import presto  # or import hive
from dateutil.relativedelta import relativedelta
from MysqlUtility import MySQL, paddstr, paddnum
import sys
import calendar

logger = logging.getLogger('userInfolog')
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler('userInfolog.log')
fileHandler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fileHandler.setFormatter(formatter)
# logger.addHandler(fileHandler)
cnslhandler = logging.StreamHandler()
cnslhandler.setLevel(logging.DEBUG)
cnslhandler.setFormatter(formatter)
logger.addHandler(cnslhandler)

g_pro_aduit_biz_dbcfg = {'host': '192.168.5.25', 'port': 3310, 'user': 'biz_device_detail', 'passwd': 'M6khz3CQ', 'db': 'device_detail', 'charset': 'utf8'}


def calc_user_info():
    '''创建用户'''
    bizConn = MySQL(g_pro_aduit_biz_dbcfg)

    sql = "select channel,sn,device_type,delivery_time,delivery_merchant,active_time," \
          "active_merchant,pk_name,pk_period,id from dj_device_details where active_time is not null " \
          "order by active_time "
    if not bizConn.query(sql):
        return
    result = bizConn.fetchAllRows()
    startUserId = 50000000
    arrValues = []
    logger.info(f"calc_user_info total={len(result)}")
    progress = 0
    for v in result:
        channel = paddstr(v[0])
        sn = paddstr(v[1])
        devType = paddnum(v[2])
        deliTime = paddstr(v[3])
        deliMerchant = paddstr(v[4])
        activeTime = paddstr(v[5])
        activeMer = paddstr(v[6])
        pkName = paddstr(v[7])
        pkPeriod = paddnum(v[8])
        startUserId += 1
        strValue = f"({channel},{sn},{devType},{deliTime},{deliMerchant},{activeTime}," \
                   f"{activeMer},{pkName},{pkPeriod},{paddnum(startUserId)})"
        arrValues.append(strValue)
        progress += 1
        if len(arrValues) >= 1000:
            insertSql = "insert into dj_user_info (channel,sn,device_type,delivery_time,delivery_merchant," \
                        "active_time,active_merchant,pk_name,pk_period,user_id) values " + ','.join(arrValues)
            if not bizConn.insert(insertSql):
                logger.error("calc_user_info fail, sql=%s", insertSql)
                return
            arrValues = []
            logger.info(f"calc_user_info progress={progress}")

    if len(arrValues) > 0:
        insertSql = "insert into dj_user_info (channel,sn,device_type,delivery_time,delivery_merchant," \
                    "active_time,active_merchant,pk_name,pk_period,user_id) values " + ','.join(arrValues)
        if not bizConn.insert(insertSql):
            logger.error("calc_user_info fail, sql=%s", insertSql)
            return
    logger.info(f"calc_user_info done={progress}")


if __name__ == '__main__':
    '''制造'''
    calc_user_info()