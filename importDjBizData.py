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

g_pro_biz_dbcfg = {'host': '192.168.5.20', 'port': 3306, 'user': 'biz_glsx_data', 'passwd': 'unEKczDc',
                   'db': 'glsx_biz_data',
                   'charset': 'utf8'}

g_pro_aduit_biz_dbcfg = {'host': '192.168.5.25', 'port': 3310, 'user': 'biz_device_detail', 'passwd': 'M6khz3CQ',
                         'db': 'device_detail', 'charset': 'utf8'}

logger = logging.getLogger('import')
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler('import.log')
fileHandler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fileHandler.setFormatter(formatter)
# logger.addHandler(fileHandler)
cnslhandler = logging.StreamHandler()
cnslhandler.setLevel(logging.DEBUG)
cnslhandler.setFormatter(formatter)
logger.addHandler(cnslhandler)


def calc_device_info_gps():
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "select sn,device_type,active_time,active_merchant,pk_name,pk_period,settle_merchant " \
          "from dj_2021_settle_details where active_time is not null and `return` = 0"
    if not bizConn.query(sql):
        logger.error("calc_device_info_gps fail,sql=%s", sql)
        return
    result = bizConn.fetchAllRows()
    valuesArr = []
    total = 0
    for row in result:
        sn = paddstr(row[0])
        devtype = paddnum(row[1])
        activetime = row[2]
        activemer = paddstr(row[3])
        pkname = paddstr(row[4])
        pkperiod = paddnum(row[5])
        settlemer = paddstr(row[6])
        effectDate = activetime + relativedelta(days=1)
        effectDate = effectDate.replace(hour=0, minute=0, second=0, microsecond=0)
        endDate = effectDate + relativedelta(months=row[5])
        activetime = paddstr(str(activetime))
        effectDate = paddstr(str(effectDate))
        endDate = paddstr(str(endDate))
        devstatus = paddnum(1)
        createDate = activetime
        createBy = paddstr('admin')
        updateDate = activetime
        updateBy = paddstr('admin')
        # device_sn,active_time,pkname,period,device_type,effect_date,end_date,device_status,merchant,
        # settle_merchant,created_by,created_date,updated_by,updated_date
        strValue = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (sn, activetime, pkname, pkperiod, devtype,
                                                                    effectDate, endDate, devstatus, activemer,
                                                                    settlemer, createBy, createDate, updateBy,
                                                                    updateDate)
        valuesArr.append(strValue)
        total += 1
        if len(valuesArr) >= 1000:
            insertSql = "insert into dj_device_info (device_sn,active_time,pkname,period,device_type," \
                        "effect_date,end_date,device_status,merchant, settle_merchant," \
                        "created_by,created_date,updated_by,updated_date) VALUES " + ','.join(valuesArr)
            if not bizConn.insert(insertSql):
                logger.error("calc_device_info_gps fail,sql=%s", insertSql)
                return
            valuesArr = []
            logger.info("calc_device_info_gps progress count=%d", total)
    if len(valuesArr) > 0:
        insertSql = "insert into dj_device_info (device_sn,active_time,pkname,period,device_type," \
                    "effect_date,end_date,device_status,merchant, settle_merchant," \
                    "created_by,created_date,updated_by,updated_date) VALUES " + ','.join(valuesArr)
        if not bizConn.insert(insertSql):
            logger.error("calc_device_info_gps fail,sql=%s", insertSql)
            return
    logger.info("calc_device_info_gps progress done=%d", total)


if __name__ == "__main__":
    calc_device_info_gps()
