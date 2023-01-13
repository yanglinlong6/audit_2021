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


class ShopMerSales:
    merId = 0
    merName = ''
    settName = ''
    channel = ''
    sales = 0


class SettleMerSales:
    settleName = ''
    sales = 0
    shopList = []
    merIdList = []


def match_direct_settlement_delivery_order(startTime, devType=8, findmode=0):
    """根据未匹配的订单生成设备
    findmode 0:完全匹配月份 ,1:前发货匹配，2:后发货匹配 3:仅匹配月份，不匹配发货作为调拨
    """
    bizConn = MySQL(g_pro_biz_dbcfg)
    deliveryTblName = ''
    resultTblName = ''
    if devType == 8:
        deliveryTblName = 'dj_2021_delivery_device'
        strType = "('GPS','OBD')"
        matchTypeCon = "device_type in (8,1)"
        resultTblName = 'dj_2022_settle_details'
        tblName = 'dj_2022_settlment_order_06plus'
    elif devType == 6:
        deliveryTblName = 'dj_2021_delivery_device_record'
        matchTypeCon = "device_type = 6"
        strType = "('记录仪')"
        resultTblName = 'dj_2022_settle_details_record'
        tblName = 'dj_2022_settlment_order_06plus'
    elif devType == 12501:
        deliveryTblName = 'dj_2021_delivery_device_record'
        strType = "('车充')"
        matchTypeCon = "device_type = 12501"
        resultTblName = 'dj_2022_settle_details_record'
        tblName = 'dj_2022_settlment_order_06plus'
    #and business_type = '智慧门店SAAS服务'
    sql = f"SELECT id,finance_time,trim(cus_name),product_code,total,hw_unit_price_tax,hw_unit_price,pk_unit_price_tax," \
          f"pk_unit_price,install_unit_price_tax,install_unit_price,product_name,pk_term,pk_name,cost,match_total," \
          f"sale_type from {tblName} where sale_type = '渠道'  and device_type in {strType} and product_type = '硬件' " \
          f"and finance_time >= '{startTime}' and total > 0 and status <> 1 " \
          f"and business_type = 'SAAS系统订阅服务—旧版本' order by finance_time"

    if not bizConn.query(sql):
        logger.error("match_direct_settlement_delivery_order fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    for v in res:
        rowId = v[0]
        finDate = v[1]
        #finDate = datetime.datetime.strptime(finTime, '%Y/%m/%d')
        #finDate = datetime.datetime.strptime(finTime, '%Y-%m-%d %H:%M:%S')
        strFinDate = finDate.strftime('%Y-%m')
        settleTime = finDate.strftime('%Y-%m-%d')
        cusName = v[2]
        matchCusName = cusName
        matchCondition2 = ''
        if cusName == '零售/电商客户':
            matchCusName = '零星销售（销管）'
        elif cusName == '其他':
            # 内蒙古威耀商贸有限公司，隆尧县跃驿汽车销售有限公司
            matchCusName = '内蒙古威耀商贸有限公司'
            matchCusName2 = '隆尧县跃驿汽车销售有限公司'
            matchCondition2 = " or send_merchant_name like '%{}%' ".format(matchCusName2)
        productCode = v[3]
        total = v[4]
        if total <= 0:
            logger.error("match_direct_settlement_delivery_order cusName=%s month=%s total=%d ",
                         cusName, strFinDate, total)
            continue
        hwPriceTax = v[5]
        hwPrice = v[6]
        pkPriceTax = v[7]
        pkPrice = v[8]
        installPriceTax = v[9]
        installPrice = v[10]
        productName = v[11]
        if devType == 8:
            pkTerm = v[12]
            pkName = getPkName(v[13], pkTerm)
        else:
            pkName = v[13]
            pkTerm = getPkTerm(pkName)
        cost = v[14]
        if cost is None:
            cost = 0
        matchTotal = v[15]
        saleType = v[16]
        if matchTotal is None:
            matchTotal = 0
        unitCost = cost * 1.0 / total
        total = total - matchTotal
        if findmode == 0:
            findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from {} " \
                      "where alloc = 0 and {} and (send_merchant_name like '%{}%' {}) and " \
                      "date_format(delivery_time, '%Y-%m') = '{}' order by delivery_time limit {}".format(
                deliveryTblName, matchTypeCon, matchCusName, matchCondition2, strFinDate, total)
        elif findmode == 1:
            finDate = datetime.datetime.strptime(finTime, '%Y/%m/%d')
            findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from {} " \
                      "where alloc = 0 and {}  and (send_merchant_name like '%{}%' {}) and " \
                      "delivery_time < '{}' order by delivery_time limit {}".format(
                deliveryTblName, matchTypeCon, matchCusName, matchCondition2, finDate, total)
        elif findmode == 2:
            finDate = datetime.datetime.strptime(finTime, '%Y/%m/%d')
            nextFinDate = finDate + relativedelta(months=1)
            strNextFinDate = nextFinDate.strftime('%Y-%m')
            findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from {} " \
                      "where alloc = 0 and {} and (send_merchant_name like '%{}%' {}) and " \
                      "date_format(delivery_time, '%Y-%m') = '{}' order by delivery_time limit {}".format(
                deliveryTblName, matchTypeCon, matchCusName, matchCondition2, strNextFinDate, total)
        elif findmode == 3:
            findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from {} " \
                      "where alloc = 0  and {} and channel not in ('金融渠道','金融风控渠道') and " \
                      "date_format(delivery_time, '%Y-%m') = '{}' order by delivery_time limit {}".format(
                deliveryTblName, matchTypeCon, strFinDate, total)
        elif findmode == 5:
            #finDate = datetime.datetime.strptime(finTime, '%Y-%m-%d %H:%M:%S')
            findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from {} " \
                      "where alloc = 0 and {} and " \
                      "delivery_time < '{}' order by delivery_time limit {}".format(
                deliveryTblName, matchTypeCon, finDate, total)
        elif findmode == 4:
            findSql = "select id,sn,NULL as delivery_order,settle_time as delivery_time," \
                      "null as send_merchant_name from dj_2021_settle_details_return " \
                      "where flag = 0  and " \
                      "date_format(settle_time, '%Y-%m') = '{}' order by settle_time limit {}".format(
                strFinDate, total)
        elif findmode == 6:
            findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from {} " \
                      "where alloc = 0 and {} and " \
                      "date_format(delivery_time, '%Y-%m') = '{}' order by delivery_time limit {}".format(
                deliveryTblName, matchTypeCon, strFinDate, total)

        if not bizConn.query(findSql):
            logger.error("match_direct_settlement_delivery_order fail, sql=%s", findSql)
            return
        result = bizConn.fetchAllRows()
        idsArr = []
        values = []
        for row in result:
            idx = row[0]
            idsArr.append(str(idx))
            sn = row[1]
            delivery_order = row[2]
            delivery_time = row[3]
            send_merchant_name = row[4]
            # (sn,device_type,material_code,delivery_order,delivery_time,delivery_merchant,pk_name,pk_period,
            # settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax,install_price,
            # install_price_tax,cost,flag,channel)
            strValue = "(%s,%s,%s,%s,%s,%s,%s,%s," \
                       "%s,%s,%s,%s,%s,%s,%s,%s," \
                       "%s,%s,%s,%s)" % (
                           paddstr(sn), paddnum(devType), paddstr(productCode), paddstr(delivery_order),
                           paddstr(delivery_time),
                           paddstr(send_merchant_name), paddstr(pkName), paddnum(pkTerm),
                           paddstr(cusName), paddstr(str(rowId)), paddstr(settleTime), paddnum(pkPrice),
                           paddnum(pkPriceTax), paddnum(hwPrice), paddnum(hwPriceTax), paddnum(installPrice),
                           paddnum(installPriceTax), paddnum(unitCost), paddnum(findmode), paddstr(saleType))
            values.append(strValue)
        newNatchTotal = len(result)
        if newNatchTotal > 0:
            if findmode == 4:
                ''''''
                updateSql = "update dj_2021_settle_details_return set flag= 4 where id in (%s)" % ','.join(idsArr)
            else:
                updateSql = "update " + deliveryTblName + " set alloc= 1 where id in (%s)" % ','.join(idsArr)
            if not bizConn.update(updateSql):
                logger.error("match_direct_settlement_delivery_order fail, sql=%s", updateSql)
                return
            insertSql = "insert into " + resultTblName + " (sn,device_type,material_code,delivery_order," \
                        "delivery_time,delivery_merchant,pk_name,pk_period," \
                        "settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax," \
                        "install_price,install_price_tax,cost,flag,channel) values %s" % ','.join(values)
            if not bizConn.insert(insertSql):
                logger.error("match_direct_settlement_delivery_order fail, sql=%s", insertSql)
                return
            status = 2
            if total == newNatchTotal:
                status = 1
            else:
                logger.error("match_direct_settlement_delivery_order cusName=%s month=%s need=%d find=%d",
                             cusName, strFinDate, total, len(result))
            matchTotal += newNatchTotal
            updateSql = "update %s set status = %d,match_total=%d where id=%d" % (tblName, status, matchTotal, rowId)
            if not bizConn.update(updateSql):
                logger.error("match_direct_settlement_delivery_order fail, sql=%s", updateSql)
                return
        else:
            logger.error("match_direct_settlement_delivery_order cusName=%s month=%s need=%d find=%d",
                         cusName, strFinDate, total, len(result))
    logger.info("match_direct_settlement_delivery_order devtype=%d done", devType)


def genRandomTime(activeDate):
    '''根据日期生产随机时间'''
    monthRange = calendar.monthrange(activeDate.year, activeDate.month)
    dayx = random.randint(1, monthRange[1])
    activeDate = activeDate.replace(day=dayx)
    hour = random.randint(0, 199) % 13 + 8  # 8点开始
    munite = random.randint(0, 199) % 60
    sencond = random.randint(0, 199) % 60
    strTime = "%04d-%02d-%02d %02d:%02d:%02d" % (
        activeDate.year, activeDate.month, activeDate.day, hour, munite, sencond)
    return strTime


def calc_not_direct_device_active_time(deliveryTime, settleTime):
    '''计算发货时间到结算时间之间的时间'''
    deliveryDate = datetime.datetime.strptime(str(deliveryTime), '%Y-%m-%d %H:%M:%S')
    settleDate = datetime.datetime.strptime(str(settleTime), '%Y-%m-%d %H:%M:%S')
    if deliveryDate.month == settleDate.month:
        lastMonthOfSettle = settleDate
    else:
        lastMonthOfSettle = settleDate + relativedelta(months=-1)
    monthRange = calendar.monthrange(lastMonthOfSettle.year, lastMonthOfSettle.month)
    startDay = 1
    if deliveryDate.month == lastMonthOfSettle.month:
        startDay = deliveryDate.day
    dayx = random.randint(startDay, monthRange[1])
    activeDate = lastMonthOfSettle.replace(day=dayx)
    hour = random.randint(0, 199) % 13 + 8  # 8点开始
    munite = random.randint(0, 199) % 60
    sencond = random.randint(0, 199) % 60
    strTime = "%04d-%02d-%02d %02d:%02d:%02d" % (
        activeDate.year, activeDate.month, activeDate.day, hour, munite, sencond)
    return strTime


def getPkName(pkName, period):
    if pkName is None or pkName == '':
        if period == 12:
            pkName = '驾宝无忧-基础版一年期'
        elif period == 24:
            pkName = '驾宝无忧-基础版二年期'
        elif period == 36:
            pkName = '驾宝无忧-基础版三年期'
        elif period == 48:
            pkName = '驾宝无忧-基础版四年期'
        elif period == 60:
            pkName = '驾宝无忧-基础版五年期'
        else:
            pkName = '驾宝无忧-基础版'
    return pkName


def getPkTerm(pkName):
    pkTerm = 0
    if pkName is not None and pkName != '':
        if pkName == '位置服务三年期':
            pkTerm = 36
        elif pkName == '记录仪运营服务一年版':
            pkTerm = 12
        elif pkName == '记录仪运营服务三年版':
            pkTerm = 36
        elif pkName == '驾宝无忧-基础版一年期':
            pkTerm = 12
        elif pkName == '驾宝无忧-基础版三年期':
            pkTerm = 36
        elif pkName == '驾宝无忧-基础版三年期、驾宝无忧-基础版一年期':
            pkTerm = 36
        elif pkName == '驾宝无忧-畅享版三年期':
            pkTerm = 36
    return pkTerm


def match_not_direct_settlement_delivery_order(startTime, devType=8, mode=0):
    """根据未匹配的订单生成设备"""
    bizConn = MySQL(g_pro_biz_dbcfg)
    settMerDict = {}

    query_settlement_merchant(bizConn, settMerDict)
    if devType == 8:
        deliverytTblName = 'dj_2021_delivery_device'
        resultTblName = 'dj_2022_settle_details'
        strType = "('GPS','OBD')"
        tblName = 'dj_2022_settlment_order_06plus'
    elif devType == 6:
        deliverytTblName = 'dj_2021_delivery_device_record'
        resultTblName = 'dj_2022_settle_details_record'
        strType = "('记录仪')"
        tblName = 'dj_2022_settlment_order_06plus'
    elif devType == 12501:
        deliverytTblName = 'dj_2021_delivery_device_record'
        resultTblName = 'dj_2022_settle_details_record'
        strType = "('车充')"
        tblName = 'dj_2022_settlment_order_06plus'

    # and business_type = '智慧门店SAAS服务'
    sql = "SELECT id,finance_time,trim(cus_name),product_code,total,hw_unit_price_tax,hw_unit_price,pk_unit_price_tax," \
          "pk_unit_price,install_unit_price_tax,install_unit_price,product_name,pk_term,pk_name,cost,match_total," \
          "sale_type from %s where sale_type = '直销'  and device_type in %s and product_type = '硬件' and status <> 1 " \
          "and finance_time >= '%s' and business_type = 'SAAS系统订阅服务—智慧车管线索订阅' order by finance_time " % (tblName, strType, startTime)

    if not bizConn.query(sql):
        logger.error("match_not_direct_settlement_delivery_order fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    for v in res:
        rowId = v[0]
        finDate = v[1]
        #finDate = datetime.datetime.strptime(finTime, '%Y/%m/%d')
        #finDate = datetime.datetime.strptime(finTime, '%Y-%m-%d %H:%M:%S')
        strFinDate = finDate.strftime('%Y-%m')
        settleTime = finDate.strftime('%Y-%m-%d')
        cusName = v[2]
        matchCusName = cusName
        saleType = v[16]
        if cusName == '绵阳鑫驰骋商贸有限公司' or cusName == '成都市艾潇商贸有限公司':
            matchCusName = '四川新艾潇商贸有限公司'
        elif cusName in ('广汇汽车服务有限责任公司天津武清分公司', '东台宝通汽车服务有限责任公司',
                         '广汇汽车服务有限责任公司东台分公司', '上海宝信实嘉汽车销售有限公司天津武清分公司'):
            matchCusName = '广汇汽车'
            cusName = '广汇汽车'
            saleType = '广汇'

        productCode = v[3]
        total = v[4]
        if total <= 0:
            logger.error("match_direct_settlement_delivery_order cusName=%s month=%s total=%d ",
                         cusName, strFinDate, total)
            continue
        hwPriceTax = v[5]
        hwPrice = v[6]
        pkPriceTax = v[7]
        pkPrice = v[8]
        installPriceTax = v[9]
        installPrice = v[10]
        productName = v[11]
        if devType == 8:
            pkTerm = v[12]
            pkName = getPkName(v[13], pkTerm)
        else:
            pkTerm = getPkTerm(v[13])
            pkName = v[13]

        cost = v[14]
        preMatchTotal = v[15]
        #saleType = v[16]
        if preMatchTotal is None:
            preMatchTotal = 0
        if cost is None:
            cost = 0
        unitCost = cost * 1.0 / total
        total = total - preMatchTotal
        settMerInfo = settMerDict.get(matchCusName, None)
        shopList = []
        if settMerInfo is not None:
            shopList = settMerInfo.shopList
        if mode == 0:
            findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from {} " \
                      "where device_type = {} and alloc = 0  and customer_name like '%{}%' and delivery_time  <= '{}' " \
                      "order by delivery_time limit {}".format(deliverytTblName, devType, matchCusName, settleTime, total)
        elif mode == 1:
            findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from {} " \
                      "where device_type = {} and alloc = 0  and customer_name like '%{}%' and " \
                      "date_format(delivery_time, '%Y-%m') = '{}' " \
                      "order by delivery_time limit {}".format(deliverytTblName, devType, matchCusName, strFinDate, total)
        elif mode == 5:
            findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from {} " \
                      "where device_type = {} and alloc = 0  and delivery_time  < '{}' " \
                      "order by delivery_time limit {}".format(deliverytTblName, devType, settleTime, total)
        elif mode == 4:
            findSql = "select id,sn,NULL as delivery_order,settle_time as delivery_time," \
                      "null as send_merchant_name from dj_2021_settle_details_return " \
                      "where flag = 0  and date_format(settle_time, '%Y-%m') = '{}' " \
                      "order by settle_time limit {}".format(strFinDate, total)
        else:
            findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from {} " \
                      "where device_type = {} and alloc = 0  and channel='广汇' and delivery_time > '2021-01-01' and " \
                      "delivery_time  < '{}' order by delivery_time limit {}".format(deliverytTblName, devType,
                                                                                     settleTime, total)

        if not bizConn.query(findSql):
            logger.error("match_not_direct_settlement_delivery_order fail, sql=%s", sql)
            return
        result = bizConn.fetchAllRows()
        idsArr = []
        values = []

        for row in result:
            idx = row[0]
            idsArr.append(str(idx))
            sn = row[1]
            delivery_order = row[2]
            delivery_time = row[3]
            send_merchant_name = row[4]
            activeTime = calc_not_direct_device_active_time(delivery_time, finDate)
            if len(shopList) == 0:
                activeMerchant = send_merchant_name
            else:
                activeMerchant = random.choice(shopList).merName  # send_merchant_name
            # (sn,device_type,material_code,delivery_order,delivery_time,delivery_merchant,pk_name,pk_period,
            # settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax,install_price,
            # install_price_tax,cost,flag)
            strValue = "(%s,%s,%s,%s,%s,%s,%s,%s," \
                       "%s,%s,%s,%s,%s,%s,%s,%s," \
                       "%s,%s,%s,%s,%s,%s)" % (
                           paddstr(sn), paddnum(devType), paddstr(productCode), paddstr(delivery_order),
                           paddstr(delivery_time),
                           paddstr(send_merchant_name), paddstr(pkName), paddnum(pkTerm),
                           paddstr(cusName), paddstr(str(rowId)), paddstr(settleTime), paddnum(pkPrice),
                           paddnum(pkPriceTax), paddnum(hwPrice), paddnum(hwPriceTax), paddnum(installPrice),
                           paddnum(installPriceTax), paddnum(unitCost), paddstr(activeTime), paddstr(activeMerchant),
                           paddnum(mode), paddstr(saleType))
            values.append(strValue)
        matchTotal = len(result)
        if matchTotal > 0:
            if mode == 4:
                ''''''
                updateSql = "update dj_2021_settle_details_return set flag= 4 where id in (%s)" % ','.join(idsArr)
            else:

                updateSql = "update " + deliverytTblName + " set alloc= 1 where id in (%s)" % ','.join(idsArr)
            if not bizConn.update(updateSql):
                logger.error("match_not_direct_settlement_delivery_order fail, sql=%s", updateSql)
                return
            insertSql = "insert into " + resultTblName + " (sn,device_type,material_code,delivery_order," \
                                                         "delivery_time,delivery_merchant,pk_name,pk_period," \
                                                         "settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax," \
                                                         "install_price,install_price_tax,cost,active_time,active_merchant,flag,channel) values %s" % \
                        ','.join(values)
            if not bizConn.insert(insertSql):
                logger.error("match_direct_settlement_delivery_order fail, sql=%s", insertSql)
                return
            status = 2
            if total == matchTotal:
                status = 1
            matchTotal += preMatchTotal
            updateSql = "update %s set status = %d,match_total=%d where id=%d" % (tblName, status, matchTotal, rowId)
            if not bizConn.update(updateSql):
                logger.error("match_not_direct_settlement_delivery_order fail, sql=%s", updateSql)
                return
            # logger.info("match_direct_settlement_delivery_order cusName=%s month=%s need=%d find=%d",
            #            cusName, strFinDate, total, len(result))
        else:
            logger.error("match_not_direct_settlement_delivery_order cusName=%s month=%s devtype=%d need=%d find=%d",
                         cusName, strFinDate, devType, total, len(result))


def query_gh_svrmer_shop(bizConn, settMerDict):
    '''查询广汇服务商门店'''
    sql = "SELECT server, b.merchant_id, merchant FROM gh_server_merchant_jj a " \
          " LEFT JOIN dj_system_merchant b on (a.merchant = b.merchant_name) where merchant_id is not null"
    if not bizConn.query(sql):
        logger.error("query_gh_svrmer_shop fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    for v in res:
        shopInfo = ShopMerSales()
        shopInfo.merId = int(v[1])
        shopInfo.merName = str(v[2])
        shopInfo.settName = str(v[0]).strip()
        settMerInfo = settMerDict.get(shopInfo.settName, None)
        if settMerInfo is None:
            settMerInfo = SettleMerSales()
            settMerInfo.settleName = shopInfo.settName
            settMerInfo.shopList = []
            settMerInfo.shopList.append(shopInfo)
        else:
            settMerInfo.shopList.append(shopInfo)
        settMerDict[shopInfo.settName] = settMerInfo
    logger.info("query_gh_svrmer_shop total=%d", len(settMerDict.keys()))


def match_gh_settlement_delivery_order():
    """根据未匹配的订单生成设备"""
    bizConn = MySQL(g_pro_biz_dbcfg)
    settMerDict = {}
    query_gh_svrmer_shop(bizConn, settMerDict)

    sql = "SELECT id,finance_time,cus_name,product_code,total,hw_unit_price_tax,hw_unit_price,pk_unit_price_tax," \
          "pk_unit_price,install_unit_price_tax,install_unit_price,product_name,pk_term,pk_name,cost,sale_type from " \
          "dj_2021_settlment_order_gh where sale_type = '代销' and device_type in ('GPS','OBD') " \
          "and product_type = '硬件'  and status = 0 " \
          "and finance_time >= '2021/2/1'"

    if not bizConn.query(sql):
        logger.error("match_gh_settlement_delivery_order fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    for v in res:
        rowId = v[0]
        finTime = v[1]
        finDate = datetime.datetime.strptime(finTime, '%Y/%m/%d')
        strFinDate = finDate.strftime('%Y-%m')
        settleTime = finDate.strftime('%Y-%m-%d')
        cusName = v[2]
        matchCusName = '广汇汽车'
        productCode = v[3]
        total = v[4]
        if total <= 0:
            logger.error("match_gh_settlement_delivery_order cusName=%s month=%s total=%d ",
                         cusName, strFinDate, total)
            continue
        hwPriceTax = v[5]
        hwPrice = v[6]
        pkPriceTax = v[7]
        pkPrice = v[8]
        installPriceTax = v[9]
        installPrice = v[10]
        productName = v[11]
        pkTerm = v[12]
        pkName = getPkName(v[13], pkTerm)
        cost = v[14]
        saleType = '广汇'
        unitCost = cost * 1.0 / total

        findSql = "select id,sn,delivery_order,delivery_time,send_merchant_name from dj_2021_delivery_device " \
                  "where alloc = 0  and customer_name like '%{}%' and " \
                  "delivery_time  < '{}' and (delivery_time <'2021-03-01' or delivery_time >= '2021-04-01 00:00:00') " \
                  "order by delivery_time limit {}".format(matchCusName, settleTime, total)

        if not bizConn.query(findSql):
            logger.error("match_gh_settlement_delivery_order fail, sql=%s", sql)
            return
        result = bizConn.fetchAllRows()
        idsArr = []
        values = []

        for row in result:
            idx = row[0]
            idsArr.append(str(idx))
            sn = row[1]
            delivery_order = row[2]
            delivery_time = row[3]
            send_merchant_name = row[4]
            activeTime = calc_not_direct_device_active_time(delivery_time, finDate)

            settMerInfo = settMerDict.get(send_merchant_name, None)
            shopList = []
            if settMerInfo is not None:
                shopList = settMerInfo.shopList
            else:
                shopInfo = ShopMerSales()
                shopInfo.merName = send_merchant_name
                shopList.append(shopInfo)

            activeMerchant = random.choice(shopList).merName  # send_merchant_name
            # (sn,device_type,material_code,delivery_order,delivery_time,delivery_merchant,pk_name,pk_period,
            # settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax,install_price,
            # install_price_tax,cost,channel)
            strValue = "(%s,%s,%s,%s,%s,%s,%s,%s," \
                       "%s,%s,%s,%s,%s,%s,%s,%s," \
                       "%s,%s,%s,%s,%s)" % (
                           paddstr(sn), paddnum(8), paddstr(productCode), paddstr(delivery_order),
                           paddstr(delivery_time), paddstr(send_merchant_name), paddstr(pkName), paddnum(pkTerm),
                           paddstr(matchCusName), paddstr(str(rowId)), paddstr(settleTime), paddnum(pkPrice),
                           paddnum(pkPriceTax), paddnum(hwPrice), paddnum(hwPriceTax), paddnum(installPrice),
                           paddnum(installPriceTax), paddnum(unitCost), paddstr(activeTime), paddstr(activeMerchant),
                           paddstr(saleType))
            values.append(strValue)
        matchTotal = len(result)
        if matchTotal > 0:
            updateSql = "update dj_2021_delivery_device set alloc= 1 where id in (%s)" % ','.join(idsArr)
            if not bizConn.update(updateSql):
                logger.error("match_gh_settlement_delivery_order fail, sql=%s", updateSql)
                return
            insertSql = "insert into dj_2021_settle_details (sn,device_type,material_code,delivery_order," \
                        "delivery_time,delivery_merchant,pk_name,pk_period," \
                        "settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax," \
                        "install_price,install_price_tax,cost,active_time,active_merchant,channel) values %s" % \
                        ','.join(values)
            if not bizConn.insert(insertSql):
                logger.error("match_gh_settlement_delivery_order fail, sql=%s", insertSql)
                return
            status = 2
            if total == matchTotal:
                status = 1
            updateSql = "update dj_2021_settlment_order_gh set status = %d,match_total=%d where id=%d" % (
                status, matchTotal, rowId)
            if not bizConn.update(updateSql):
                logger.error("match_gh_settlement_delivery_order fail, sql=%s", updateSql)
                return
            # logger.info("match_direct_settlement_delivery_order cusName=%s month=%s need=%d find=%d",
            #           cusName, strFinDate, total, len(result))
        else:
            logger.error("match_gh_settlement_delivery_order cusName=%s month=%s need=%d find=%d",
                         cusName, strFinDate, total, len(result))
    logger.info("match_direct_settlement_delivery_order done")


def update_delivery_device_settle_merchant(devType=8, year=2021):
    """根据未匹配的订单生成设备"""
    bizConn = MySQL(g_pro_biz_dbcfg)
    if devType == 8:
        deliverytTblName = 'dj_2021_delivery_device'
    elif devType == 6:
        deliverytTblName = 'dj_2021_delivery_device_record'

    tblName = "dj_%d_settlemer_relation" % year
    merDict = {}  # 服务商-结算客户
    sql = "select settle_mer,svrmerchant from %s" % tblName
    if not bizConn.query(sql):
        return
    result = bizConn.fetchAllRows()
    for v in result:
        settleMer = str(v[0]).strip()
        svrMer = str(v[1]).strip()
        merDict[svrMer] = settleMer
    logger.info("match_nodirect_settlement_delivery_order year=%d total svrmer = %d", year, len(merDict.keys()))

    if year == 2020:
        for key in merDict.keys():
            updateSql = "update " + deliverytTblName + " set customer_name='%s' " \
                                                       "where delivery_time < '2021-01-01 00:00:00' and send_merchant_name='%s' and customer_name = ''" % (
                            merDict.get(key), key)
            if not bizConn.update(updateSql):
                logger.error("match_nodirect_settlement_delivery_order fail, sql=%s", updateSql)
                return
            logger.info("match_nodirect_settlement_delivery_order update svrmer=%s  settlemer=%s", key,
                        merDict.get(key))
    else:
        for key in merDict.keys():
            updateSql = "update " + deliverytTblName + " set customer_name='%s' " \
                                                       "where delivery_time >= '2022-09-01 00:00:00' and send_merchant_name='%s' " \
                                                       "and customer_name = ''" % (merDict.get(key), key)
            if not bizConn.update(updateSql):
                logger.error("match_nodirect_settlement_delivery_order fail, sql=%s", updateSql)
                return
            logger.info("match_nodirect_settlement_delivery_order update svrmer=%s  settlemer=%s", key,
                        merDict.get(key))


def query_supplychain_delivery_device(startTime, endTime, devType=8):
    """
    从供应链导入设备
    :param devType: 8 追踪器设备 6：记录仪&车充
    :return:
    """
    if devType == 8:
        strType = "(1,8)"
        resTblName = "dj_2021_delivery_device"
    elif devType == 6:
        strType = "(6,12501)"
        #strType = "(12501)"
        resTblName = "dj_2021_delivery_device_record"
    sql = "select (case when bdui.CHANNEL = 1 then '广汇' when bdui.CHANNEL = 2 then '金融风控渠道' " \
          "when bdui.CHANNEL = 3 then '同盟会渠道' " \
          "when bdui.CHANNEL = 4 then '金融渠道' " \
          "when bdui.CHANNEL = 5 then '亿咖通' " \
          "when bdui.CHANNEL = 6 then '同盟会渠道制定品' " \
          "when bdui.CHANNEL = 7 then '安吉租赁' " \
          "when bdui.CHANNEL = 8 then '广汇直营' " \
          "when bdui.CHANNEL = 9 then '园区店' " \
          "else '其他' end ) as '渠道'," \
          "(case when bdui.SALE_MODE = 1 then '经销' " \
          "when bdui.SALE_MODE = 2 then '代销' " \
          "else '未知' end) as '销售', " \
          "oid.sn,oi.order_code,oi.SEND_MERCHANT_NO,bdui.name,bl.send_time, " \
          "oi.attrib_code,ami.material_name," \
          "ami.device_type_id,oi.DEVICE_ID,oi.DEVICE_NAME " \
          "from order_info_detail as oid " \
          "LEFT JOIN order_info as oi on(oid.ORDER_CODE=oi.ORDER_CODE) " \
          "left join bs_logistics as bl on(bl.ID=oid.LOGISTICS_ID) " \
          "left join am_material_info as ami on(ami.material_code=oi.ATTRIB_CODE) " \
          "left join bs_merchant_order_vehicle as bmov on(bmov.dispatch_order_code=oid.ORDER_CODE)" \
          "left join bs_merchant_order as bmo on(bmo.ORDER_NUMBER=bmov.merchant_order) " \
          "left JOIN bs_dealer_user_info as bdui on(bdui.MERCHANT_CODE=bmo.MERCHANT_CODE) " \
          "left join device_code as dc on(dc.ID = oi.DEVICE_ID)" \
          "where dc.TYPE_ID in %s and oid.UPDATED_DATE >= '%s' and oid.UPDATED_DATE < '%s' " \
          "and oi.SEND_MERCHANT_NO =bmo.merchant_code " % (strType, startTime, endTime)
    # and bmo.merchant_code ='44201279'
    supplyDbCfg = MySQL(g_supply_dbcfg)
    bizConn = MySQL(g_pro_biz_dbcfg)
    if not supplyDbCfg.query(sql):
        logger.error("query_supplychain_delivery_device fail, sql=%s", sql)
        return
    result = supplyDbCfg.fetchAllRows()
    arrValues = []
    count = 0
    for v in result:
        channel = v[0]
        saleMode = v[1]
        sn = v[2]
        orderCode = v[3]
        sendMerCode = v[4]
        sendMerName = v[5]
        sendTime = v[6]
        materialCode = v[7]
        materailName = v[8]
        devTypeId = v[9]
        devId = v[10]
        devName = v[11]
        strValue = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())" % (
            paddstr(channel), paddstr(saleMode), paddstr(sn), paddstr(orderCode), paddstr(sendMerCode),
            paddstr(sendMerName), paddstr(sendTime), paddstr(materialCode), paddstr(materailName), paddnum(devTypeId),
            paddnum(devId), paddstr(devName))
        arrValues.append(strValue)
        count += 1
        if len(arrValues) >= 1000:
            insertSql = "insert into " + resTblName + " (channel,sale_mode,sn,delivery_order,send_merchant_no," \
                                                      "send_merchant_name,delivery_time,material_code,material_name,device_type,device_code," \
                                                      "device_name,create_time,update_time) values " + ",".join(
                arrValues) + \
                        " on duplicate key update update_time=values(update_time)"
            if not bizConn.insert(insertSql):
                logger.error("query_supplychain_delivery_device fail, sql=%s", insertSql)
                return
            arrValues = []
            logger.info("query_supplychain_delivery_device count=%d", count)

    if len(arrValues) > 0:
        insertSql = "insert into " + resTblName + " (channel,sale_mode,sn,delivery_order,send_merchant_no," \
                                                  "send_merchant_name,delivery_time,material_code,material_name,device_type,device_code," \
                                                  "device_name,create_time,update_time) values " + ",".join(arrValues) + \
                    " on duplicate key update update_time=values(update_time)"
        if not bizConn.insert(insertSql):
            logger.error("query_supplychain_delivery_device fail, sql=%s", insertSql)
            return
    logger.info("query_supplychain_delivery_device %s done total=%d", strType, count)


def textToNumber(text):
    if text == '' or text == '-':
        return 0
    else:
        return int(float(text))


def calc_direct_device_active_time(deliveryTime, activeMonth):
    '''计算发货时间到结算时间之间的时间'''
    deliveryDate = datetime.datetime.strptime(str(deliveryTime), '%Y-%m-%d %H:%M:%S')
    activeDate = datetime.datetime.strptime(str(activeMonth), '%Y-%m-%d %H:%M:%S')
    monthRange = calendar.monthrange(activeDate.year, activeDate.month)
    startDay = 1
    if deliveryDate.month == activeDate.month:
        startDay = deliveryDate.day
    dayx = random.randint(startDay, monthRange[1])
    activeDate = activeDate.replace(day=dayx)
    hour = random.randint(0, 199) % 13 + 8  # 8点开始
    munite = random.randint(0, 199) % 60
    sencond = random.randint(0, 199) % 60
    strTime = "%04d-%02d-%02d %02d:%02d:%02d" % (
        activeDate.year, activeDate.month, activeDate.day, hour, munite, sencond)
    return strTime


def query_settlement_merchant(auditConn, settMerDict={}):
    '''查询每个结算客户下属门店'''
    sql = "select merchant_id,merchant_name,settle_merchant_name,svr_merchant_name from dj_system_merchant " \
          "where  settle_merchant_name <>'' and svr_merchant_name <> '二次转化客户'"

    if False == auditConn.query(sql):
        logger.error("query_settlement_merchant query fail, sql=%s", sql)
        return
    res = auditConn.fetchAllRows()
    for v in res:
        shopInfo = ShopMerSales()
        shopInfo.merId = int(v[0])
        shopInfo.merName = str(v[1])
        if v[2] is None:
            continue

        shopInfo.settName = str(v[2]).strip()
        channel = str(v[3]).strip()
        if channel == '广汇门店':
            channel = '广汇直销'
        elif channel == '渠道客户':
            channel = '渠道'
        elif channel == '非广汇直销' or channel == '非广汇直销客户':
            channel = '非广汇直销'
        shopInfo.channel = channel

        settMerInfo = settMerDict.get(shopInfo.settName, None)
        if settMerInfo is None:
            settMerInfo = SettleMerSales()
            settMerInfo.settleName = shopInfo.settName
            settMerInfo.shopList = []
            settMerInfo.shopList.append(shopInfo)
        else:
            settMerInfo.shopList.append(shopInfo)
        settMerDict[shopInfo.settName] = settMerInfo
    logger.info("query_settlement_merchant total settlemer = %d", len(settMerDict.keys()))


def active_direct_merchant_device():
    '''计算直销激活'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    settMerDict = {}
    query_settlement_merchant(bizConn, settMerDict)
    sql = "select customer,active_202101,active_202102,active_202103,active_202104,active_202105,active_202106," \
          "active_202107,active_202108,active_202109,active_202110,active_202111,active_202112 from dj_2022_direct_cus_active "

    sql = "select customer,active_202103,active_202104,active_202105,active_202106,active_202107,active_202108 from dj_2022_direct_cus_active_new"

    if not bizConn.query(sql):
        logger.error("active_direct_merchant_device fail, sql=%s", sql)
        return
    year = 2022
    result = bizConn.fetchAllRows()
    merMonthActive = {}
    for v in result:
        # logger.info("active_direct_merchant_device %s", v)
        cusName = v[0]
        monthActive = []
        month3_active = textToNumber(v[1])
        monthActive.append(month3_active)
        month2_active = textToNumber(v[2])
        monthActive.append(month2_active)

        month4_active = textToNumber(v[3])
        monthActive.append(month4_active)
        month5_active = textToNumber(v[4])
        monthActive.append(month5_active)
        month6_active = textToNumber(v[5])
        monthActive.append(month6_active)
        month7_active = textToNumber(v[6])
        monthActive.append(month7_active)

        '''
        month8_active = textToNumber(v[7])
        monthActive.append(month8_active)     
        month8_active = textToNumber(v[8])
        monthActive.append(month8_active)
        month9_active = textToNumber(v[9])
        monthActive.append(month9_active)
        month10_active = textToNumber(v[10])
        monthActive.append(month10_active)
        month11_active = textToNumber(v[11])
        monthActive.append(month11_active)
        month12_active = textToNumber(v[12])
        monthActive.append(month12_active)
        '''
        merMonthActive[cusName] = monthActive

    for key in merMonthActive.keys():
        logger.info("active_direct_merchant_device mer=%s %s", key, merMonthActive.get(key))
        monthActives = merMonthActive.get(key)
        settleMer = key
        month = 2
        for activeTotal in monthActives:
            month += 1
            if activeTotal == 0:
                continue
            activeMonth = datetime.datetime.now().replace(year=year, month=month, day=1,
                                                          hour=0, minute=0, second=0, microsecond=0)
            nextMonth = activeMonth + relativedelta(months=1)
            sql = "select id,delivery_time,settle_time from dj_2021_settle_details where device_type = 8  " \
                  "and active_time is null and " \
                  "delivery_time < '%s' and settle_merchant = '%s' order by delivery_time limit %d" % \
                  (nextMonth, settleMer, activeTotal)
            if not bizConn.query(sql):
                logging.error("active_direct_merchant_device fail, sql=%s", sql)
                return
            res = bizConn.fetchAllRows()
            settMerInfo = settMerDict.get(settleMer, None)
            shopList = []
            if settMerInfo is not None:
                shopList = settMerInfo.shopList
            else:
                shopInfo = ShopMerSales()
                shopInfo.merName = settleMer
                shopList.append(shopInfo)

            if activeTotal != len(res):
                logger.error("active_direct_merchant_device settlemer=%s month=%s need=%d found=%d",
                             settleMer, activeMonth, activeTotal, len(res))
            for row in res:
                idx = row[0]
                deliveryTime = row[1]
                settleTime = row[2]
                activeTime = calc_direct_device_active_time(deliveryTime, activeMonth)
                activeMerchant = random.choice(shopList)
                shopName = activeMerchant.merName
                updateSql = "update dj_2021_settle_details set active_time='%s',active_merchant='%s' where id=%d" % \
                            (activeTime, shopName, idx)
                if not bizConn.update(updateSql):
                    logger.error("active_direct_merchant_device fail, sql=%s", updateSql)
                    return
                bizConn.commit()
    logger.info("active_direct_merchant_device done")


class DevInfo:
    pass

def active_direct_merchant_device_new():
    '''计算直销激活'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    settMerDict = {}
    query_settlement_merchant(bizConn, settMerDict)
    monthActiveDict = {'2021-10': 6702, '2021-11': 4728, '2021-12': 5500}
    for monthx in range(10, 13):
        endMonth = "2021-%d-01 00:00:00" % (monthx+1)
        activeMonth = "2021-%d"%monthx
        activeMonthDate = datetime.datetime.now().replace(month=monthx, day=1, hour=0, minute=0, second=0, microsecond=0)
        if monthx == 12:
            endMonth = '2022-01-01 00:00:00'
        sql = "select id,delivery_time,settle_merchant from dj_2021_settle_details where device_type =8 " \
              "and `return` = 0 " \
              "and channel = '直销'  and delivery_time < '%s' and active_time  is null " % endMonth
        if not bizConn.query(sql):
            logger.error("active_direct_merchant_device fail, sql=%s", sql)
            return
        result = bizConn.fetchAllRows()
        allDevice = []
        for v in result:
            devInfo = DevInfo()
            devInfo.idx = v[0]
            devInfo.deliTime = v[1]
            devInfo.settleMer = v[2]
            allDevice.append(devInfo)
        activeTotal = monthActiveDict.get(activeMonth)
        logger.info("active_direct_merchant_device_new month=%s totaldev=%d activeDEv=%d", activeMonth, len(allDevice), activeTotal)
        activeDevs = random.sample(allDevice, activeTotal)
        progress = 0
        for dev in activeDevs:
            shopList = []
            settMerInfo = settMerDict.get(dev.settleMer, None)
            if settMerInfo is not None:
                shopList = settMerInfo.shopList
            else:
                shopInfo = ShopMerSales()
                shopInfo.merName = dev.settleMer
                shopList.append(shopInfo)
            activeTime = calc_direct_device_active_time(dev.deliTime, activeMonthDate)
            activeMerchant = random.choice(shopList)
            shopName = activeMerchant.merName
            updateSql = "update dj_2021_settle_details set active_time='%s',active_merchant='%s' where id=%d" % \
                        (activeTime, shopName, dev.idx)
            if not bizConn.update(updateSql):
                logger.error("active_direct_merchant_device_new fail, sql=%s", updateSql)
                return
            progress += 1
            if(progress % 10 ==0):
                logger.info("active_direct_merchant_device_new month=%s totaldev=%d activeDEv=%d progress=%d",
                            activeMonth, len(allDevice), activeTotal, progress)

        logger.info("active_direct_merchant_device_new Done month=%s totaldev=%d activeDEv=%d", activeMonth, len(allDevice), activeTotal)

    logger.info("active_direct_merchant_device_new done")


def handle_return_device(channel, data, devType, matchPrice=0):
    '''处理退货的情况
    直销 代销
    '''
    if devType == 8:
        srcTblName = 'dj_2022_settlment_order_06plus'
        orderColumn = ' NULL as delivery_order'
        if data == 2021:
            tblName = 'dj_2021_settle_details'
            orderColumn = 'delivery_order'
            dstTblName = 'dj_2022_settle_details_return'
        else:
            tblName = 'dj_2022_settle_details'
            dstTblName = 'dj_2022_settle_details_return'

        #"and business_type = '智慧门店SAAS服务' " \
        sql = "SELECT id,finance_time,cus_name,product_code,total,hw_unit_price_tax,hw_unit_price,pk_unit_price_tax," \
              "pk_unit_price,install_unit_price_tax,install_unit_price,product_name,pk_term,pk_name,cost,match_total," \
              "sale_type from %s where sale_type = '%s' and device_type in ('GPS','OBD') and product_type = '硬件' " \
              "and business_type ='SAAS系统订阅服务—旧版本' " \
              "and product_name<>'销售折扣' and status <> 1 and total < 0 order by finance_time desc" % (srcTblName, channel)
    else:
        tblName = 'dj_2022_settle_details_record'
        srcTblName = 'dj_2022_settlment_order_06plus'
        dstTblName = 'dj_2022_settle_details_return'
        orderColumn = 'delivery_order'

        tblName = 'dj_2021_settle_details'
        orderColumn = 'delivery_order'
        dstTblName = 'dj_2022_settle_details_return'

        if devType == 6:
            strType = '记录仪'
        elif devType == 12501:
            strType = '车充'
        sql = "SELECT id,finance_time,cus_name,product_code,total,hw_unit_price_tax,hw_unit_price,pk_unit_price_tax," \
              "pk_unit_price,install_unit_price_tax,install_unit_price,product_name,pk_term,pk_name,cost,match_total," \
              "sale_type,device_type from " \
              "%s where device_type = '%s' and product_type = '硬件'  and product_name<>'销售折扣' " \
              "and business_type ='SAAS系统订阅服务—旧版本' " \
              "and status <> 1 and total < 0 order by finance_time desc" % (srcTblName, strType)
        # and product_name <> '销售折扣'
        if data == 2020:
            if devType == 6:
                strType = '驾宝记录仪'
            elif devType == 12501:
                strType = '蓝牙车充'
            srcTblName = 'dj_record_settlment_order'
            sql = "SELECT id,business_time,cus_name,product_code,cus_num,hw_unit_price_tax,hw_unit_price," \
                  "NULL as pk_unit_price_tax,pk_unit_price, null as install_unit_price_tax,null as install_unit_price," \
                  "product_name,null as pk_term,pk_name,cost,`match`,sale_type,device_type from dj_record_settlment_order " \
                  "where device_type = '%s' and business_type = '智慧门店SAAS服务' and status <> 1 and business_type ='SAAS系统订阅服务—旧版本' " \
                  "and cus_num < 0 order by business_time desc" % strType

    bizConn = MySQL(g_pro_biz_dbcfg)

    if not bizConn.query(sql):
        logger.error("handle_return_device fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    for v in res:
        rowId = v[0]
        finDate = v[1]
        #finDate = datetime.datetime.strptime(finTime, '%Y/%m/%d')
        #finDate = datetime.datetime.strptime(finTime, '%Y-%m-%d %H:%M:%S')

        strFinDate = finDate.strftime('%Y-%m')
        settleTime = finDate.strftime('%Y-%m-%d')
        cusName = v[2]
        saleType = v[16]
        matchCusName = cusName
        if cusName in ('广汇汽车服务有限责任公司天津武清分公司', '东台宝通汽车服务有限责任公司',
                         '广汇汽车服务有限责任公司东台分公司', '上海宝信实嘉汽车销售有限公司天津武清分公司'):
            matchCusName = '广汇汽车'
            saleType = '广汇'

        productCode = v[3]
        total = abs(v[4])
        hwPriceTax = v[5]
        hwPrice = v[6]
        pkPriceTax = v[7]
        pkPrice = v[8]

        if data == 2020:
            if hwPrice is not None:
                hwPrice = hwPrice/1.13
            if pkPrice is not None:
                pkPrice = pkPrice/1.13

        installPriceTax = v[9]
        installPrice = v[10]
        productName = v[11]
        if devType == 8:
            pkTerm = v[12]
            pkName = getPkName(v[13], pkTerm)
        else:
            pkTerm = getPkTerm(v[13])
            pkName = v[13]
        cost = v[14]
        if cost is None:
            cost = 0
        matchTotal = v[15]

        if matchTotal is None:
            matchTotal = 0
        unitCost = cost * 1.0 / total
        total = total - matchTotal
        if matchPrice == 1:
            findSql = f"select id,sn,{orderColumn},delivery_time,settle_merchant from {tblName} " \
                      f"where `return` = 0  and device_type = {devType} and settle_merchant = '{matchCusName}' and active_time is null and " \
                      f"delivery_time < '{finDate}' and abs(hw_price-{hwPrice}) < 0.1 order by delivery_time desc limit {total}"
        else:
            findSql = f"select id,sn,{orderColumn},delivery_time,settle_merchant from {tblName} " \
                      f"where `return` = 0  and device_type = {devType} and settle_merchant = '{matchCusName}' and " \
                      f"delivery_time < '{finDate}' order by delivery_time desc limit {total}"

        if not bizConn.query(findSql):
            logger.error("handle_return_device fail, sql=%s", findSql)
            return
        result = bizConn.fetchAllRows()
        idsArr = []
        values = []
        for row in result:
            idx = row[0]
            idsArr.append(str(idx))
            sn = row[1]
            delivery_order = row[2]
            delivery_time = row[3]
            send_merchant_name = row[4]
            # (sn,device_type,material_code,delivery_order,delivery_time,delivery_merchant,pk_name,pk_period,
            # settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax,install_price,
            # install_price_tax,cost,flag,return,channel)
            strValue = "(%s,%s,%s,%s,%s,%s,%s,%s," \
                       "%s,%s,%s,%s,%s,%s,%s,%s," \
                       "%s,%s,%s,%s,%s)" % (
                           paddstr(sn), paddnum(devType), paddstr(productCode), paddstr(delivery_order),
                           paddstr(delivery_time),
                           paddstr(send_merchant_name), paddstr(pkName), paddnum(pkTerm),
                           paddstr(matchCusName), paddstr(str(rowId)), paddstr(settleTime), paddnum(pkPrice),
                           paddnum(pkPriceTax), paddnum(hwPrice), paddnum(hwPriceTax), paddnum(installPrice),
                           paddnum(installPriceTax), paddnum(unitCost), paddnum(0), paddnum(1), paddstr(saleType))
            values.append(strValue)
        matchTotal = len(result)
        if matchTotal > 0:
            updateSql = "update %s set `return`= 1 where id in (%s)" % (tblName, ','.join(idsArr))
            if not bizConn.update(updateSql):
                logger.error("handle_return_device fail, sql=%s", updateSql)
                return
            insertSql = "insert into " + dstTblName + " (sn,device_type,material_code,delivery_order," \
                        "delivery_time,delivery_merchant,pk_name,pk_period," \
                        "settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax," \
                        "install_price,install_price_tax,cost,flag, `return`, channel) values %s" % ','.join(values)
            if not bizConn.insert(insertSql):
                logger.error("handle_return_device fail, sql=%s", insertSql)
                return
            status = 2
            if abs(total) == matchTotal:
                status = 1
            if data == 2020:
                updateSql = "update " + srcTblName + " set status = %d,`match`=%d where id=%d" % (
                    status, matchTotal, rowId)
            else:
                updateSql = "update " + srcTblName + " set status = %d,match_total=%d where id=%d" % (
                    status, matchTotal, rowId)
            if not bizConn.update(updateSql):
                logger.error("handle_return_device fail, sql=%s", updateSql)
                return
            # logger.info("match_direct_settlement_delivery_order cusName=%s month=%s need=%d find=%d",
            #            cusName, strFinDate, total, len(result))
        else:
            logger.error("handle_return_device cusName=%s month=%s need=%d find=%d",
                         matchCusName, strFinDate, total, len(result))


def update_gh_settletime():
    '''更新广汇汽车结算时间，往后推要一个月'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "select id,settle_time from dj_device_details where settle_merchant = '广汇汽车' and settle_time is not NULL"
    if not bizConn.query(sql):
        logger.error("update_gh_settletime fail, sql=%s", sql)
        return
    result = bizConn.fetchAllRows()
    count = 0
    logger.info("update_gh_settletime total=%d", len(result))
    for v in result:
        idx = v[0]
        settleTime = v[1]
        nextsettleTime = settleTime + relativedelta(months=1)
        updateSql = "update dj_device_details set settle_time='%s' where id=%d" % (nextsettleTime, idx)
        # logger.info("settletime=%s nextmonth=%s",settleTime,nextsettleTime)
        count += 1
        if not bizConn.update(updateSql):
            logger.error("update_gh_settletime fail, sql=%s", updateSql)
            return
        if count % 1000 == 0:
            logger.info("update_gh_settletime progress=%d", count)
    logger.info("update_gh_settletime done")


def active_direct_record_device():
    '''激活直销设备
    按照1-3个月内激活
    '''
    bizConn = MySQL(g_pro_biz_dbcfg)
    settMerDict = {}
    tblName = "dj_2022_settle_details_record"
    tblName = "dj_2021_settle_details"

    query_settlement_merchant(bizConn, settMerDict)
    sql = f"select id,settle_time,settle_merchant from {tblName} where channel='直销' and device_type in (6,12501) " \
          f"and `return` = 0 and pk_period > 0 and active_time is null"
    if not bizConn.query(sql):
        logger.error("active_direct_record_device fail, sql=%s", sql)
        return
    result = bizConn.fetchAllRows()
    count = 0
    logger.info("active_direct_record_device total=%d", len(result))
    for v in result:
        idx = v[0]
        settleTime = v[1]
        settleMer = v[2]
        settMerInfo = settMerDict.get(settleMer, None)
        shopList = []
        if settMerInfo is not None:
            shopList = settMerInfo.shopList
        else:
            shopInfo = ShopMerSales()
            shopInfo.merName = settleMer
            shopList.append(shopInfo)
        activeMerchant = random.choice(shopList)
        shopName = activeMerchant.merName

        activeDate = settleTime + relativedelta(months=random.randint(0, 2))
        monthRange = calendar.monthrange(activeDate.year, activeDate.month)
        dayx = random.randint(activeDate.day, monthRange[1])
        activeDate = activeDate.replace(day=dayx)
        hour = random.randint(0, 199) % 13 + 8  # 8点开始
        munite = random.randint(0, 199) % 60
        sencond = random.randint(0, 199) % 60
        activeTime = "%04d-%02d-%02d %02d:%02d:%02d" % (
            activeDate.year, activeDate.month, activeDate.day, hour, munite, sencond)

        if activeTime > '2022-08-01 00:00:00':
            updateSql = f"update {tblName} set active_time=null,active_merchant='' where id=%d" % (
                 idx)
        else:
            updateSql = f"update {tblName} set active_time='%s',active_merchant='%s' where id=%d" % (
                activeTime, shopName, idx)
        # logger.info("settletime=%s nextmonth=%s",settleTime,nextsettleTime)
        count += 1
        if not bizConn.update(updateSql):
            logger.error("active_direct_record_device fail, sql=%s", updateSql)
            return
        if count % 1000 == 0:
            logger.info("active_direct_record_device progress=%d", count)
    logger.info("active_direct_record_device done")


def match_not_direct_202101_settlment(devType=8, mode=0):
    """根据未匹配的订单生成设备"""
    bizConn = MySQL(g_pro_biz_dbcfg)
    settMerDict = {}
    query_settlement_merchant(bizConn, settMerDict)
    if devType == 8:
        deliverytTblName = 'dj_2021_settle_details_202101_his_dx_gps'
        resultTblName = 'dj_2021_settle_details_202101'
        strType = "('GPS','OBD')"
        tblName = 'dj_2021_settlment_order_finance'
    else:
        return

    sql = "SELECT id,finance_time,trim(cus_name),product_code,total,hw_unit_price_tax,hw_unit_price,pk_unit_price_tax," \
          "pk_unit_price,install_unit_price_tax,install_unit_price,product_name,pk_term,pk_name,cost,match_total," \
          "sale_type from " \
          "%s where sale_type = '代销' and business_type = '智慧门店SAAS服务' and device_type in %s and " \
          "product_type = '硬件' and status <> 1 " \
          "and finance_time < '2021/2/1' order by finance_time " % (tblName, strType)

    if not bizConn.query(sql):
        logger.error("match_not_direct_settlement_delivery_order fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    for v in res:
        rowId = v[0]
        finTime = v[1]
        finDate = datetime.datetime.strptime(finTime, '%Y/%m/%d')
        #finDate = datetime.datetime.strptime(finTime, '%Y-%m-%d %H:%M:%S')
        strFinDate = finDate.strftime('%Y-%m')
        settleTime = finDate.strftime('%Y-%m-%d')
        cusName = v[2]
        matchCusName = cusName
        saleType = v[16]
        if cusName == '绵阳鑫驰骋商贸有限公司' or cusName == '成都市艾潇商贸有限公司':
            matchCusName = '四川新艾潇商贸有限公司'
        elif cusName in ('广汇汽车服务有限责任公司天津武清分公司', '东台宝通汽车服务有限责任公司',
                         '广汇汽车服务有限责任公司东台分公司', '上海宝信实嘉汽车销售有限公司天津武清分公司'):
            matchCusName = '广汇汽车'
            cusName = '广汇汽车'
            saleType = '广汇'

        productCode = v[3]
        total = v[4]
        if total <= 0:
            logger.error("match_not_direct_202101_settlment cusName=%s month=%s total=%d ",
                         cusName, strFinDate, total)
            continue
        hwPriceTax = v[5]
        hwPrice = v[6]
        pkPriceTax = v[7]
        pkPrice = v[8]
        installPriceTax = v[9]
        installPrice = v[10]
        productName = v[11]
        if devType == 8:
            pkTerm = v[12]
            pkName = getPkName(v[13], pkTerm)
        else:
            pkTerm = getPkTerm(v[13])
            pkName = v[13]

        cost = v[14]
        preMatchTotal = v[15]
        #saleType = v[16]
        if preMatchTotal is None:
            preMatchTotal = 0
        if cost is None:
            cost = 0
        unitCost = cost * 1.0 / total
        total = total - preMatchTotal
        settMerInfo = settMerDict.get(matchCusName, None)
        shopList = []
        if settMerInfo is not None:
            shopList = settMerInfo.shopList
        if mode == 0:
            findSql = "select id,sn,delivery_order,delivery_time,delivery_merchant,active_time,active_merchant from {} " \
                      "where device_type = {} and alloc = 0  and settle_merchant like '%{}%' and delivery_time  <= '{}' " \
                      "order by delivery_time limit {}".format(deliverytTblName, devType, matchCusName, settleTime, total)


        if not bizConn.query(findSql):
            logger.error("match_not_direct_settlement_delivery_order fail, sql=%s", sql)
            return
        result = bizConn.fetchAllRows()
        idsArr = []
        values = []

        for row in result:
            idx = row[0]
            idsArr.append(str(idx))
            sn = row[1]
            delivery_order = row[2]
            delivery_time = row[3]
            send_merchant_name = row[4]

            activeTime = row[5]
            activeMerchant = row[6]
            # (sn,device_type,material_code,delivery_order,delivery_time,delivery_merchant,pk_name,pk_period,
            # settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax,install_price,
            # install_price_tax,cost,flag)
            strValue = "(%s,%s,%s,%s,%s,%s,%s,%s," \
                       "%s,%s,%s,%s,%s,%s,%s,%s," \
                       "%s,%s,%s,%s,%s,%s)" % (
                           paddstr(sn), paddnum(devType), paddstr(productCode), paddstr(delivery_order),
                           paddstr(delivery_time),
                           paddstr(send_merchant_name), paddstr(pkName), paddnum(pkTerm),
                           paddstr(cusName), paddstr(str(rowId)), paddstr(settleTime), paddnum(pkPrice),
                           paddnum(pkPriceTax), paddnum(hwPrice), paddnum(hwPriceTax), paddnum(installPrice),
                           paddnum(installPriceTax), paddnum(unitCost), paddstr(activeTime), paddstr(activeMerchant),
                           paddnum(mode), paddstr(saleType))
            values.append(strValue)
        matchTotal = len(result)
        if matchTotal > 0:
            updateSql = "update " + deliverytTblName + " set alloc= 1 where id in (%s)" % ','.join(idsArr)
            if not bizConn.update(updateSql):
                logger.error("match_not_direct_settlement_delivery_order fail, sql=%s", updateSql)
                return
            insertSql = "insert into " + resultTblName + " (sn,device_type,material_code,delivery_order," \
                                                         "delivery_time,delivery_merchant,pk_name,pk_period," \
                                                         "settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax," \
                                                         "install_price,install_price_tax,cost,active_time,active_merchant,flag,channel) values %s" % \
                        ','.join(values)
            if not bizConn.insert(insertSql):
                logger.error("match_direct_settlement_delivery_order fail, sql=%s", insertSql)
                return
            status = 2
            if total == matchTotal:
                status = 1
            matchTotal += preMatchTotal
            updateSql = "update %s set status = %d,match_total=%d where id=%d" % (tblName, status, matchTotal, rowId)
            if not bizConn.update(updateSql):
                logger.error("match_not_direct_settlement_delivery_order fail, sql=%s", updateSql)
                return
            # logger.info("match_direct_settlement_delivery_order cusName=%s month=%s need=%d find=%d",
            #            cusName, strFinDate, total, len(result))
        else:
            logger.error("match_not_direct_settlement_delivery_order cusName=%s month=%s devtype=%d need=%d find=%d",
                         cusName, strFinDate, devType, total, len(result))


def modify_direct_20211012_install_price():
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "select id,cus_name,install_unit_price,finance_time from glsx_biz_data.dj_2021_settlment_order_10_12 " \
          "where install_unit_price > 0 and business_type = '智慧门店SAAS服务' and product_type = '硬件'"
    if not bizConn.query(sql):
        logger.error(sql)
        return
    result = bizConn.fetchAllRows()
    for v in result:
        idx = v[0]
        cusName = v[1]
        installPrice = v[2]
        settleTime = v[3]
        updateSql = "update dj_2021_settle_details set install_price=%f " \
                    "where settle_order='%d' and settle_merchant='%s' and settle_time='%s'" % (
            installPrice, idx, cusName, settleTime)
        logger.info(updateSql)
        if not bizConn.update(updateSql):
            return


def modify_direct_20211012_hw_price():
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "select sn,settle_merchant,settle_time, hw_price from glsx_biz_data.dj_2021_settle_details_income_modify"
    if not bizConn.query(sql):
        logger.error(sql)
        return
    result = bizConn.fetchAllRows()
    for v in result:
        sn = v[0]
        settleMerchant = v[1]
        settleTime = v[2]
        income = v[3]
        findSql = "select id,pk_price,hw_price,install_price from dj_2021_settle_details " \
                  "where sn='%s' and settle_merchant='%s' and settle_time='%s'" % (sn,settleMerchant,settleTime)
        if not bizConn.query(findSql):
            logger.error(findSql)
            return
        res = bizConn.fetchAllRows()
        for row in res:
            idx = row[0]
            pkPrice = row[1]
            if pkPrice is None:
                pkPrice = 0.0
            hwPrice = row[2]
            if hwPrice is None:
                pkPrice = 0.0
            insPrice = row[3]
            if insPrice is None:
                insPrice = 0.0
            newHwPrice = income - pkPrice - insPrice
            #logger.info("sn=%s settlemer=%s settletime=%s income=%f newHwPrice=%f", sn, settleMerchant, settleTime, income, newHwPrice)
            newIncome = newHwPrice + pkPrice + insPrice
            updateSql = "update dj_2021_settle_details set hw_price=%f where id=%d " % (newHwPrice, idx)
            logger.info(updateSql)
            if not bizConn.update(updateSql):
                return


def modify_direct_20211012_cost_price(isReturn=False):
    bizConn = MySQL(g_pro_biz_dbcfg)
    if isReturn:
        sql = "select sn,trim(settle_merchant),settle_time,hw_cost from dj_2021_settle_details_return_finnace_check"
    else:
        sql = "select sn,trim(settle_merchant),settle_time,hw_cost from dj_2021_settle_details_cost_modify"
    if not bizConn.query(sql):
        logger.error(sql)
        return
    result = bizConn.fetchAllRows()
    for v in result:
        sn = v[0]
        settleMerchant = v[1]
        settleTime = v[2]
        hwCost = v[3]

        if isReturn:
            updateSql = "update dj_2021_settle_details_return_agg set hw_cost=%f " \
                        "where sn='%s' and settle_merchant='%s' and settle_time='%s'" % (
                            hwCost, sn, settleMerchant, settleTime)
        else:
            updateSql = "update dj_2021_settle_details_agg set hw_cost=%f " \
                        "where sn='%s' and settle_merchant='%s' and settle_time='%s'" % (
                            hwCost, sn, settleMerchant, settleTime)
        logger.info(updateSql)
        if not bizConn.update(updateSql):
            return


if __name__ == "__main__":
    ''''''

    startTime = '2022-09-01'
    endTime = '2023-01-01'
    #query_supplychain_delivery_device(startTime, endTime, devType=8)
    # 导入记录仪 车充设备
    #query_supplychain_delivery_device(startTime, endTime,devType=6)

    #更新供应链出库设备的结算客户 6表示: 记录仪 车充
    #update_delivery_device_settle_merchant(6, 2021)
    #update_delivery_device_settle_merchant(8, 2021)

    #处理直销 （原代销）
    #match_not_direct_settlement_delivery_order(startTime, 8, 0)
    #匹配客户渠道 （原直销）
    #match_direct_settlement_delivery_order(startTime, devType=8, findmode=0)

    #记录仪直销 （原代销）
    #match_not_direct_settlement_delivery_order(startTime, devType=6, mode=0)
    #记录仪渠道 （原直销）
    #match_direct_settlement_delivery_order(startTime, devType=6, findmode=0)

    #车充直销 （原代销）
    #match_not_direct_settlement_delivery_order(startTime, devType=12501, mode=0)
    #车充渠道 （原直销）
    #match_direct_settlement_delivery_order(startTime, devType=12501, findmode=0)

    #处理退货
    #直销追踪器退货 -- 一般不会又直销的退货

    # #追踪器
    handle_return_device('直销', data=2021, devType=8)
    #记录仪
    handle_return_device('直销', data=2021, devType=6)
    #车充退货
    handle_return_device('渠道', data=2021, devType=12501)

    #渠道追踪器退货
    handle_return_device('渠道', data=2021, devType=8)

    #记录仪
    handle_return_device('渠道', data=2021, devType=6)
    # 车充退货
    handle_return_device('渠道', data=2021, devType=12501)

    #active_direct_record_device()

    print("done")