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

logger = logging.getLogger('dj2021log')
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler('dj2021log.log')
fileHandler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fileHandler.setFormatter(formatter)
# logger.addHandler(fileHandler)
cnslhandler = logging.StreamHandler()
cnslhandler.setLevel(logging.DEBUG)
cnslhandler.setFormatter(formatter)
logger.addHandler(cnslhandler)

g_supply_dbcfg = {'host': '192.168.5.23', 'port': 3306, 'user': 'data_house_user', 'passwd': 'ZhaiMiAxIg',
                  'db': 'glsx_supplychain', 'charset': 'utf8'}
g_pro_biz_dbcfg = {'host': '192.168.5.20', 'port': 3306, 'user': 'biz_glsx_data', 'passwd': 'unEKczDc',
                   'db': 'glsx_biz_data', 'charset': 'utf8'}
g_presto_host = '192.168.5.35'
g_presto_port = 9090

monthDict = {
    '1月':'2021-01',
    '2月':'2021-02',
    '3月':'2021-03',
    '4月':'2021-04',
    '5月':'2021-05',
    '6月':'2021-06',
    '7月':'2021-07',
    '8月':'2021-08',
    '9月':'2021-09',
    '10月':'2021-10',
    '11月':'2021-11',
    '12月':'2021-12',
}


def calc_hardware_cost():
    '''计算硬件成本'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    #t_hardware_cost_bill_2021_10_12
    sql = "select finmonth,settle_merchant,total,price,amount from t_hardware_cost_bill_2021_10_12 "
    if not bizConn.query(sql):
        logger.error("calc_hardware_cost fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    monthFormat = "'%Y-%m'"
    for v in res:
        month = v[0]
        settleMonth = monthDict.get(month, None)
        if settleMonth is None:
            continue
        settleMerchant = v[1]
        if settleMerchant in ('广汇','上海宝信实嘉汽车销售有限公司天津武清分公司','东台宝通汽车服务有限责任公司','东台宝通汽车服务有限责任公司'):
            settleMerchant = '广汇汽车'
        total = v[2]
        if total is None:
            continue
        price = v[3]
        # amount = v[4]
        # logger.info("calc_hardware_cost month=%s settlemer=%s total=%d price=%f",
        #            settleMonth, settleMerchant, total, price)
        if total > 0:
            tblName = 'dj_2021_settle_details'
        else:
            tblName = 'dj_2021_settle_details_return'
            price = 0.0 - price
            total = abs(total)
        querySql = "select channel,sn,settle_time,active_time,pk_period from {} where " \
                   "settle_merchant like '%{}%' " \
                   "and date_format(settle_time, {})='{}' limit {}".format(tblName, settleMerchant, monthFormat, settleMonth, total)
        logger.info(querySql)
        if not bizConn.query(querySql):
            logger.error("calc_hardware_cost fail, sql=%s", querySql)
            return
        result = bizConn.fetchAllRows()
        if len(result) != total:
            logger.info("calc_hardware_cost month=%s settlemer=%s total=%d price=%f find=%d",
                        settleMonth, settleMerchant, total, price, len(result))

        arrValues = []
        for row in result:
            channel = row[0]
            sn = row[1]
            settleTime = row[2]
            activeTime = row[3]
            period = row[4]
            strValue = "(%s,%s,%s,%s,%s,%s,%s)" % (paddstr(channel), paddstr(sn), paddstr(settleTime),
                                                   paddstr(activeTime), paddnum(period),
                                                   paddstr(settleMerchant), paddnum(price))
            arrValues.append(strValue)
        if len(arrValues) > 0:
            insertSql = "insert into t_hardware_cost (mer_type,sn,settle_time,active_time," \
                        "period,settle_merchant,price) values " + ",".join(arrValues)
            if not bizConn.insert(insertSql):
                logger.error("calc_hardware_cost fail, sql=%s ", insertSql)
                return

    logger.info("calc_hardware_cost done")


def calc_install_cost():
    '''计算安装成本'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    # t_install_cost_bill_2021_10_12
    sql = "select settle_merchant,total,price,amount from t_install_cost_bill_2021_10_12 where price is not null"
    if not bizConn.query(sql):
        logger.error("calc_install_cost fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    for v in res:
        settleMerchant = v[0]
        total = v[1]
        price = v[2]
        amount = v[3]

        if settleMerchant in ('广汇','上海宝信实嘉汽车销售有限公司天津武清分公司','东台宝通汽车服务有限责任公司','东台宝通汽车服务有限责任公司'):
            settleMerchant = '广汇汽车'

        if total > 0:
            tblName = 'dj_2021_settle_details'
        else:
            tblName = 'dj_2021_settle_details_return'
            price = price
            total = abs(total)

        #settle_time>='2021-01-01' and settle_time < '2021-10-01'
        querySql = "select channel,sn,settle_time,active_time,pk_period from {} where " \
                   "settle_time>='2021-10-01' and " \
                   "settle_merchant like '%{}%' limit {}".format(tblName, settleMerchant, total)
        logger.info(querySql)
        if not bizConn.query(querySql):
            logger.error("calc_install_cost fail, sql=%s", querySql)
            return
        result = bizConn.fetchAllRows()
        if len(result) != total:
            logger.info("calc_install_cost settlemer=%s total=%d price=%f find=%d",
                         settleMerchant, total, price, len(result))

        arrValues = []
        for row in result:
            channel = row[0]
            sn = row[1]
            settleTime = row[2]
            activeTime = row[3]
            period = row[4]
            strValue = "(%s,%s,%s,%s,%s,%s,%s)" % (paddstr(channel), paddstr(sn), paddstr(settleTime),
                                                   paddstr(activeTime), paddnum(period),
                                                   paddstr(settleMerchant), paddnum(price))
            arrValues.append(strValue)
        if len(arrValues) > 0:
            insertSql = "insert into t_install_cost (mer_type,sn,settle_time,active_time," \
                        "period,settle_merchant,price) values " + ",".join(arrValues)
            if not bizConn.insert(insertSql):
                logger.error("calc_install_cost fail, sql=%s ", insertSql)
                return


def update_hwcost_device_active_time():
    '''更新激活时间'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "select sn,active_time from glsx_biz_data.dj_2021_settle_details where settle_time < '2022-01-01' " \
          "and active_time >= '2021-10-01' and device_type =8 " \
          "and sn in (select sn from  glsx_biz_data.t_hardware_cost where active_time is null)"

    if not bizConn.query(sql):
        logger.error("update_cost_device_active_time fail, sql=%s", sql)
        return
    result = bizConn.fetchAllRows()
    logger.info("update_cost_device_active_time total=%d", len(result))
    count = 0
    for v in result:
        sn = v[0]
        active_time = v[1]
        if len(sn) > 10:
            sn = sn[0:10]
        updateSql = "update t_hardware_cost set active_time='%s' where sn='%s'" % (active_time, sn)
        if not bizConn.update(updateSql):
            logger.error("update_cost_device_active_time fail, sql=%s", updateSql)
            return
        count += 1
        if count % 1000 == 0:
            logger.info("update_cost_device_active_time progress=%d", count)
    logger.info("update_cost_device_active_time total=%d", count)


def update_install_cost_device_active_time():
    '''更新激活时间'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "select sn from  glsx_biz_data.t_install_cost where " \
          "settle_time  < '2022-01-01' and (active_time ='null' or active_time is null)"
    if not bizConn.query(sql):
        logger.error("update_install_cost_device_active_time fail, sql=%s", sql)
        return
    result = bizConn.fetchAllRows()
    logger.info("update_install_cost_device_active_time total=%d", len(result))
    count = 0
    for v in result:
        sn = v[0]
        sql = "select active_time from dj_2021_settle_details where sn = '%s'" % sn
        bizConn.query(sql)
        actRes = bizConn.fetchOneRow()
        if actRes is None:
            continue
        activeTime = actRes[0]
        updateSql = "update t_install_cost set active_time='%s' where sn='%s'" % (activeTime, sn)
        if not bizConn.update(updateSql):
            logger.error("update_install_cost_device_active_time fail, sql=%s", updateSql)
            return
        count += 1
        if count % 10 == 0:
            logger.info("update_install_cost_device_active_time progress=%d", count)
    logger.info("update_install_cost_device_active_time total=%d", count)


def update_gh_hwcost_settletime():
    '''更新广汇汽车结算时间，往后推要一个月'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "select id, date_format(settle_time,'%Y-%m-%d %H:%i:%s') from t_hardware_cost " \
          "where settle_merchant = '广汇汽车' and settle_time < '2021-01-01'"
    if not bizConn.query(sql):
        logger.error("update_gh_hwcost_settletime fail, sql=%s", sql)
        return
    result = bizConn.fetchAllRows()
    count = 0
    logger.info("update_gh_hwcost_settletime total=%d", len(result))
    for v in result:
        idx = v[0]
        settleTime = v[1]
        settleTime = datetime.datetime.strptime(settleTime, '%Y-%m-%d %H:%M:%S')
        nextsettleTime = settleTime + relativedelta(months=1)
        updateSql = "update t_hardware_cost set settle_time='%s' where id=%d" % (nextsettleTime, idx)
        # logger.info("settletime=%s nextmonth=%s",settleTime,nextsettleTime)
        count += 1
        if not bizConn.update(updateSql):
            logger.error("update_gh_hwcost_settletime fail, sql=%s", updateSql)
            return
        if count % 1000 == 0:
            logger.info("update_gh_hwcost_settletime progress=%d", count)
    logger.info("update_gh_hwcost_settletime done")


def update_gh_installcost_settletime():
    '''更新广汇汽车结算时间，往后推要一个月'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "select id,date_format(settle_time,'%Y-%m-%d %H:%i:%s') from t_install_cost " \
          "where settle_merchant = '广汇汽车' and settle_time < '2021-01-01'"
    if not bizConn.query(sql):
        logger.error("update_gh_installcost_settletime fail, sql=%s", sql)
        return
    result = bizConn.fetchAllRows()
    count = 0
    logger.info("update_gh_installcost_settletime total=%d", len(result))
    for v in result:
        idx = v[0]
        settleTime = v[1]
        settleTime = datetime.datetime.strptime(settleTime, "%Y-%m-%d %H:%M:%S")
        nextsettleTime = settleTime + relativedelta(months=1)
        updateSql = "update t_install_cost set settle_time='%s' where id=%d" % (nextsettleTime, idx)
        # logger.info("settletime=%s nextmonth=%s",settleTime,nextsettleTime)
        count += 1
        if not bizConn.update(updateSql):
            logger.error("update_gh_installcost_settletime fail, sql=%s", updateSql)
            return
        if count % 1000 == 0:
            logger.info("update_gh_installcost_settletime progress=%d", count)
    logger.info("update_gh_installcost_settletime done")


def calc_cost_duplicate_device(type = 1):
    '''统计嘀加重复的设备'''
    if type == 1:
        tblName = 't_hardware_cost'
    else:
        tblName = 't_install_cost'
    sql = "SELECT sn,COUNT(1) from %s where price > 0.0 GROUP BY sn HAVING COUNT(1) > 1" % tblName
    bizDbConn = MySQL(g_pro_biz_dbcfg)
    if not bizDbConn.query(sql):
        logger.error("calc_cost_duplicate_device fail, sql=%s", sql)
        return
    res = bizDbConn.fetchAllRows()
    total = 0
    logger.info("calc_cost_duplicate_device total=%d", len(res))
    for v in res:
        sn = v[0]
        querySql = "select id,sn from %s " \
                   "where sn='%s' order by settle_time limit 1" % (tblName, sn)
        if not bizDbConn.query(querySql):
            logger.error("calc_cost_duplicate_device fail, sql=%s", querySql)
            return
        result = bizDbConn.fetchAllRows()
        for row in result:
            idx = row[0]
            newSn = row[1]
            newSn += '1'
            updateSql = "update %s set sn='%s' where id=%d" % (tblName, newSn, idx)
            if not bizDbConn.update(updateSql):
                logger.error("calc_cost_duplicate_device fail, sql=%s", updateSql)
                return
            total += 1
        if total % 10 == 0:
            logger.info("calc_cost_duplicate_device progress=%d", total)
    logger.info("calc_cost_duplicate_device done, total=%d", total)


if __name__ == '__main__':
    '''计算'''
    calc_hardware_cost()

    #calc_install_cost()

    #update_hwcost_device_active_time()

    #update_install_cost_device_active_time()

    #calc_cost_duplicate_device(type=1)





