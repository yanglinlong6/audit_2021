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
import pandas as pd
import pymysql

logger = logging.getLogger('unmatchlog')
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler('unmatchlog.log')
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

'''
有源GPS设备-无屏追踪器终端
'''
g_meterial_map = {
    '无源GPS设备-GT702': 'GT702',
    '无源GPS设备（GT702）': 'GT702',
    '无源GPS设备（GT703B）': 'GT703B',
    '无源GPS设备（GT703小）': 'GT703',
    '无源GPS设备（GT703）': 'GT703',
    '无源GPS设备（GT705）': 'GT705',

    '无源GPS设备（GT803）': 'GT803',

    '无源GPS设备（GT805）': 'GT805',

    '无源GPS设备（GT806-62）': 'GT806-62',

    '有源GPS设备-GT701': 'GT701',

    '有源GPS设备（GT700B）': 'GT700B',

    '有源GPS设备（GT700F）': 'GT700F',

    '有源GPS设备（GT701C）': 'GT701C',

    '有源GPS设备（GT800B）': 'GT800B',

    '有源GPS设备（GT800B）（含流量）': 'GT800B',
    '有源GPS设备（GT800C）': 'GT800C',

    '有源GPS设备（GT800F）': 'GT800F',

    '有源GPS设备（GT801A）': 'GT801A',  # 3

    '有源GPS设备（GT801C-1）': 'GT801C',

    '有源GPS设备（GT801C）': 'GT801C',
    '有源GPS设备（GT801C）（含流量）': 'GT801C',

    '有源GPS设备（GT801D）': 'GT801D',

    '有源GPS设备（GT801F）': 'GT801F',

    '有源GPS设备（GT801F）（含流量）': 'GT801F',
    '有源GPS设备（GT801）': 'GT801',

    '有源GPS设备-无屏追踪器终端': 'GT700',

    '4GAI小精灵记录仪 通用（含卡）': 'RE001',
    '4GAI小精灵记录仪（豪华专用版）': 'RE001',
    '隐藏记录仪（中低端）': 'RE002',
    '隐藏记录仪 通用/U型 (不含卡)': 'RE002',
    '4GAI小精灵记录仪 专用（含卡）': 'RE001',
}

g_device_prefix_dict = {
    'GT702': '7',
    'GT703B': '16',
    'GT703': '5',
    'GT705': '19',
    'GT803': '26',
    'GT805': '29',
    'GT806-62': '62',
    'GT806': '56',
    'GT701': '4',
    'GT700B': '15',
    'GT700F': '11',
    'GT701C': '18',
    'GT800B': '15',
    'GT800C': '63',
    'GT800F': '11',
    'GT801A': '35',
    'GT801C': '17',
    'GT801D': '82',
    'GT801F': '87',
    'GT801': '25',
    'GT700': '8',

    'RE001': 'SH26H',
    'RE002': 'SH26H'
}
snPreDict = {
    '11': '22',
    '15': '21',
    '16': '23',
    '17': '24',
    '26': '36',
    '29': '39',
    '62': '68'
}

monthDict = {
    '2020年1月':'2020-01-31',
    '2020年2月':'2020-02-28',
    '2020年3月':'2020-03-31',
    '2020年4月':'2020-04-30',
    '2020年5月':'2020-05-30',
    '2020年6月':'2020-06-30',
    '2020年7月':'2020-07-31',
    '2020年8月':'2020-08-30',
    '2020年9月':'2020-09-30',
    '2020年10月':'2020-10-30',
    '2020年11月':'2020-11-30',
    '2020年12月':'2020-12-30',
    '2021年1月': '2021-01-30',
    '2021年2月': '2021-02-28',
    '2021年3月': '2021-03-31',
    '2021年4月': '2021-04-30',
    '2021年5月': '2021-05-31',
    '2021年6月': '2021-06-30',
    '2021年7月': '2021-07-30',
    '2021年8月': '2021-08-30',
    '2021年9月': '2021-09-30',
    '2021年10月': '2021-10-31',
    '2021年11月': '2021-11-30',
    '2021年12月': '2021-12-30',
}


def genDeliveryTime(settleDate, mode=0):
    """根据结算随机生成发货时间"""
    settleDate = datetime.datetime.strptime(settleDate, '%Y-%m-%d')
    monthRange = calendar.monthrange(settleDate.year, settleDate.month)
    if mode == -1:
        dayx = 0 - random.randint(7, 60)
        deliveryDate = settleDate + relativedelta(days=dayx)
    else:
        #dayx = random.randint(1, monthRange[1] - 7)
        deliveryDate = settleDate
    hour = random.randint(0, 199) % 13 + 8  # 8点开始
    munite = random.randint(0, 199) % 60
    sencond = random.randint(0, 199) % 60
    delivryDate = datetime.datetime.now().replace(year=deliveryDate.year, month=deliveryDate.month,
                                                  day=deliveryDate.day, hour=hour, minute=munite, second=sencond)
    return delivryDate


def genDeliveryOrder(deliveryDate):
    """生成订单"""
    # 2021 01 04 16 43 51 79849566
    # deliveryDate = datetime.datetime.now()
    strOrder = "%04d%02d%02d%02d%02d%02d%03d" % \
               (deliveryDate.year, deliveryDate.month, deliveryDate.day, deliveryDate.hour,
                deliveryDate.minute, deliveryDate.second, deliveryDate.microsecond)
    return strOrder


def query_alloc_gps_virtual_sn(dbConn, devPrefixAllocDict, tableName, prefix):
    '''虚拟设备'''
    sql = f"select SUBSTR(sn,1,7),max(sn) from {tableName} where is_virtual = 1 and sn like '{prefix}%' GROUP BY SUBSTR(sn,1,7)"
    if not dbConn.query(sql):
        print(f"query_alloc_virtual_sn fail, sql={sql}")
        return
    result = dbConn.fetchAllRows()
    dateGenDict = devPrefixAllocDict.get(prefix)
    if dateGenDict is None:
        dateGenDict = {}
    #sn = 'SH25H%02d%02d%02d%04d'
    for row in result:
        preSn = row[0]
        sn = row[1]
        '''FFYMMDDNNN'''
        yearx = preSn[2: 3]
        monthx = preSn[3:5]
        dayx = preSn[5:7]
        maxNum = sn[7:11]
        genDate = f"202{yearx}-{monthx}-{dayx} 00:00:00"
        newDate = datetime.datetime.strptime(genDate, "%Y-%m-%d %H:%M:%S")
        dateGenDict[newDate] = int(maxNum)+1
    devPrefixAllocDict[prefix] = dateGenDict


def query_alloc_chechong_virtual_sn(dbConn, devPrefixAllocDict, tableName, prefix):
    '''虚拟设备'''
    sql = f"select SUBSTR(sn,1,8),max(sn) from {tableName} where is_virtual = 1 and sn like '{prefix}%' GROUP BY SUBSTR(sn,1,8)"
    if not dbConn.query(sql):
        print(f"query_alloc_virtual_sn fail, sql={sql}")
        return
    result = dbConn.fetchAllRows()
    dateGenDict = devPrefixAllocDict.get(prefix)
    if dateGenDict is None:
        dateGenDict = {}
    for row in result:
        preSn = row[0]
        sn = row[1]
        '''FFFYMMDDNNN'''
        yearx = preSn[3: 4]
        monthx = preSn[4:6]
        dayx = preSn[6:8]
        maxNum = sn[8:12]
        genDate = f"202{yearx}-{monthx}-{dayx} 00:00:00"
        newDate = datetime.datetime.strptime(genDate, "%Y-%m-%d %H:%M:%S")
        dateGenDict[newDate] = int(maxNum)+1
    devPrefixAllocDict[prefix] = dateGenDict


def query_alloc_record_virtual_sn(dbConn, devPrefixAllocDict, tableName, prefix):
    '''虚拟设备'''
    sql = f"select SUBSTR(sn,1,11),max(sn) from {tableName} where is_virtual = 1 and LENGTH(sn) = 15 and sn like '{prefix}%' GROUP BY SUBSTR(sn,1,11)"
    if not dbConn.query(sql):
        print(f"query_alloc_virtual_sn fail, sql={sql}")
        return
    result = dbConn.fetchAllRows()
    dateGenDict = devPrefixAllocDict.get(prefix)
    if dateGenDict is None:
        dateGenDict = {}
    for row in result:
        preSn = row[0]
        sn = row[1]
        '''SH23G2201290003'''
        yearx = preSn[5: 7]
        monthx = preSn[7:9]
        dayx = preSn[9:11]
        maxNum = sn[11:16]
        genDate = f"20{yearx}-{monthx}-{dayx} 00:00:00"
        newDate = datetime.datetime.strptime(genDate, "%Y-%m-%d %H:%M:%S")
        dateGenDict[newDate] = int(maxNum)+1
    devPrefixAllocDict[prefix] = dateGenDict


def gen_device_by_delivery(deliveryDate, deliveryNum, devPrefixAllocDict, snList, prefix):
    '''根据发货生成设备号'''
    productDate = deliveryDate - relativedelta(days=1)
    productDate = productDate.replace(hour=0, minute=0, second=0, microsecond=0)
    # prefix = '863'
    devAlloc = devPrefixAllocDict.get(prefix, None)
    if devAlloc is None:
        ''''''
        devAlloc = {}
    # 获取
    needAllocNum = deliveryNum
    while needAllocNum > 0:
        ''''''
        alloc = devAlloc.get(productDate, None)
        if alloc is None:
            alloc = 0
        if prefix in ('SH26H', 'SH23G', 'SH24G'):
            dateTotal = 9999
        else:
            # 后3位是序列号
            dateTotal = 999
        dateLess = dateTotal - alloc
        startId = alloc + 1
        # 仍然够分配
        arrValues = []

        if dateLess > 0:
            if needAllocNum > dateLess:
                # 当日生产全部分配
                allocThisTime = dateLess
            else:
                # 分配完成
                allocThisTime = needAllocNum
                devAlloc[productDate] = alloc + needAllocNum
            needAllocNum -= allocThisTime
            devAlloc[productDate] = alloc + allocThisTime

            for idx in range(startId, startId + allocThisTime):
                if len(prefix) == 1:
                    '''FYYMMDDNNN'''
                    sn = prefix + "%02d%02d%02d%03d" % (
                        productDate.year % 100, productDate.month, productDate.day, idx)
                elif prefix == 'SH26H':
                    sn = 'SH25H%02d%02d%02d%04d' % (
                        productDate.year % 100, productDate.month, productDate.day, idx)
                elif prefix in ['SH23G','SH24G']:
                    sn = 'SH23G%02d%02d%02d%04d' % (
                        productDate.year % 100, productDate.month, productDate.day, idx)
                else:
                    # 86207100129
                    '''FFYMMDDNNN'''
                    sn = prefix + "%d%02d%02d%03d" % (
                        productDate.year % 10, productDate.month, productDate.day, idx)
                # logger.info("gen_device_by_delivery deliverydate=%s  productdate=%s %s",deliveryDate,productDate,sn)
                # (delivery_id,delivery_date,sn,product_date)
                # strValue = "(%d,'%s','%s','%s','%s')" % (delivId, deliveryDate, devType, sn, productDate)
                # arrValues.append(strValue)
                snList.append(sn)
        productDate = productDate - relativedelta(days=1)

    if devPrefixAllocDict.get(prefix) is None:
        devPrefixAllocDict[prefix] = devAlloc
    logger.info("gen_device_by_delivery done total=%d", len(snList))
    return prefix


def calc_un_match_order():
    """根据未匹配的订单生成设备"""
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "SELECT id,finance_time,cus_name,product_code,total,hw_unit_price_tax,hw_unit_price,pk_unit_price_tax," \
          "pk_unit_price,install_unit_price_tax,install_unit_price,product_name,hw_term,pk_name,match_total  from " \
          "dj_2021_settlment_order where product_code <> 'GLSX50029' and product_code not like '%FW%'  and status = 0"

    if not bizConn.query(sql):
        logger.error("sync_device_data fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    devPrefixAllocDict = {}
    for row in res:
        rowId = row[0]
        finnTime = row[1]  # 财务时间
        if finnTime == '21年1月份':
            settleTime = '2021-01-31'
        elif finnTime == '21年2月份':
            settleTime = '2021-02-28'
        # 生成发货时间
        deliveryTime = genDeliveryTime(settleTime)
        deliveryOrder = genDeliveryOrder(deliveryTime)
        cusName = row[2]
        prodCode = row[3]
        total = row[4]
        hwPriceTax = row[5]
        hwPrice = row[6]
        pkPriceTax = row[7]
        pkPrice = row[8]
        installPriceTax = row[9]
        installPrice = row[10]
        prodName = row[11]
        hwTerm = row[12]
        pkName = row[13]
        matchTotal = row[14]
        if matchTotal is not None:
            total -= matchTotal
        genDevList = []
        prefix = gen_device_by_delivery(prodName, deliveryTime, total, devPrefixAllocDict, genDevList)
        arrValues = []
        for sn in genDevList:
            # (sn, is_virtual, device_type, material_code, delivery_order, delivery_time, delivery_merchant, pk_name,
            # pk_period, settle_merchant, settle_order, settle_time, pk_price, pk_price_tax, hw_price, hw_price_tax,
            # install_price, install_price_tax)
            if prefix == 'SH26H':
                devtype = 6
            else:
                devtype = 8
            settleDetail = "(%s,1,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % \
                           (paddstr(sn), paddnum(devtype), paddstr(prodCode), paddstr(deliveryOrder),
                            paddstr(deliveryTime), paddstr(cusName), paddstr(pkName), paddnum(hwTerm), paddstr(cusName),
                            paddstr(str(rowId)), paddstr(settleTime), paddnum(pkPrice), paddnum(pkPriceTax),
                            paddnum(hwPrice), paddnum(hwPriceTax), paddnum(installPrice), paddnum(installPriceTax))
            arrValues.append(settleDetail)
        if len(arrValues) > 0:
            insertSql = "insert into dj_2021_settle_details (sn,is_virtual,device_type,material_code,delivery_order," \
                        "delivery_time,delivery_merchant,pk_name,pk_period,settle_merchant,settle_order,settle_time," \
                        "pk_price,pk_price_tax,hw_price,hw_price_tax,install_price, install_price_tax) values " + \
                        ",".join(arrValues)
            if not bizConn.insert(insertSql):
                logger.error("calc_un_match_order fail, sql=%s", insertSql)
                return
        logger.info("calc_un_match_order cusname=%s total devs %d", cusName, len(arrValues))

    logger.info("calc_un_match_order done")


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


def calc_not_direct_device_active_time(deliveryTime, settleTime):
    '''计算发货时间到结算时间之间的时间'''
    deliveryDate = deliveryTime#datetime.datetime.strptime(str(deliveryTime), '%Y-%m-%d %H:%M:%S')
    settleDate = datetime.datetime.strptime(str(settleTime), '%Y-%m-%d')
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


gPrefixAllocDict = {}
def match_gen_sn_settlement_delivery_order(startTime,devType, prefix):
    """根据未匹配的订单生成设备"""
    bizConn = MySQL(g_pro_biz_dbcfg)
    mode = 0
    settMerDict = {}

    query_settlement_merchant(bizConn, settMerDict)
    if devType == 8:
        resultTblName = 'dj_2022_settle_details'
        strType = "('GPS','OBD')"
        tblName = 'dj_2022_settlment_order_06plus'
        query_alloc_gps_virtual_sn(bizConn, gPrefixAllocDict, resultTblName, prefix)

    elif devType == 6:
        resultTblName = 'dj_2022_settle_details_record'
        strType = "('记录仪')"
        tblName = 'dj_2022_settlment_order_06plus'
        query_alloc_record_virtual_sn(bizConn, gPrefixAllocDict, resultTblName, prefix)
    elif devType == 12501:
        resultTblName = 'dj_2022_settle_details_record'
        strType = "('车充')"
        tblName = 'dj_2022_settlment_order_06plus'
        query_alloc_chechong_virtual_sn(bizConn, gPrefixAllocDict, resultTblName, prefix)

    #sale_type = '代销' and business_type = '智慧门店SAAS服务' and
    sql = "SELECT id,finance_time,trim(cus_name),product_code,total,hw_unit_price_tax,hw_unit_price,pk_unit_price_tax," \
          "pk_unit_price,install_unit_price_tax,install_unit_price,product_name,pk_term,pk_name,cost,match_total," \
          "sale_type from %s where   device_type in %s and product_type = '硬件' " \
          "and status <> 1 and business_type = 'SAAS系统订阅服务—旧版本' " \
          "and finance_time >= '%s' order by finance_time " % (tblName, strType, startTime)

    if not bizConn.query(sql):
        logger.error("match_not_direct_settlement_delivery_order fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    for v in res:
        rowId = v[0]
        finDate = v[1]
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
        saleType = v[16]

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

        # 生成发货时间
        if saleType == '渠道':
            deliveryTime = genDeliveryTime(settleTime, 0)
        else:
            deliveryTime = genDeliveryTime(settleTime, -1)

        deliveryOrder = genDeliveryOrder(deliveryTime)

        genDevList = []
        gen_device_by_delivery(deliveryTime, total, gPrefixAllocDict, genDevList, prefix)
        values = []
        for sn in genDevList:
            sn = sn
            delivery_order = deliveryOrder
            delivery_time = deliveryTime
            send_merchant_name = cusName
            if saleType == '渠道':
                activeTime = None
            else:
                activeTime = calc_not_direct_device_active_time(delivery_time, settleTime)
            if len(shopList) == 0:
                activeMerchant = send_merchant_name
            else:
                activeMerchant = random.choice(shopList).merName  # send_merchant_name
            # (sn,device_type,material_code,delivery_order,delivery_time,delivery_merchant,pk_name,pk_period,
            # settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax,install_price,
            # install_price_tax,cost,flag)
            strValue = "(%s,1,%s,%s,%s,%s,%s,%s,%s," \
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
        matchTotal = len(values)
        if matchTotal > 0:

            insertSql = "insert into " + resultTblName + " (sn,is_virtual,device_type,material_code,delivery_order," \
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
                         cusName, strFinDate, devType, total, len(values))



def add_record_settlement_delivery_order():
    """根据未匹配的订单生成设备"""
    bizConn = MySQL(g_pro_biz_dbcfg)
    mode = 0
    devType = 6
    settMerDict = {}
    query_settlement_merchant(bizConn, settMerDict)
    resultTblName = 'dj_2021_settle_details_record'
    fileName = 'C:\\Users\\333\\Desktop\\记录仪修改.xlsx'
    sheetName = '新增客户'
    data = pd.read_excel(fileName, sheet_name=sheetName, header=0)

    for idx in data.index:
        settleMonth = data.loc[idx, '月份']
        cusName = data.loc[idx, '客户']
        total = data.loc[idx, '数量']
        pkPrice = data.loc[idx, '套餐价格']
        hwPrice = data.loc[idx, '硬件价格']
        saleType = data.loc[idx, '渠道']
        settleTime = monthDict.get(settleMonth)
        if pkPrice is not None and pkPrice > 0.0:
            pkTerm = 36
            pkName = '记录仪运营服务三年版'
        else:
            pkTerm = None
            pkName = ''
        productCode = ''
        finDate = datetime.datetime.strptime(settleTime, '%Y-%m-%d')
        strFinDate = finDate.strftime('%Y-%m')
        settleTime = finDate.strftime('%Y-%m-%d')

        hwPriceTax = None
        pkPriceTax = None
        installPriceTax = None
        installPrice = None
        cost = 0
        unitCost = cost * 1.0 / total
        settMerInfo = settMerDict.get(cusName, None)
        shopList = []
        if settMerInfo is not None:
            shopList = settMerInfo.shopList

        # 生成发货时间
        if saleType == '渠道':
            deliveryTime = genDeliveryTime(settleTime, 0)
        else:
            deliveryTime = genDeliveryTime(settleTime, -1)

        deliveryOrder = genDeliveryOrder(deliveryTime)

        genDevList = []
        gen_device_by_delivery(deliveryTime, total, gPrefixAllocDict, genDevList)
        values = []
        for sn in genDevList:
            sn = sn
            delivery_order = deliveryOrder
            delivery_time = deliveryTime
            send_merchant_name = cusName
            if saleType == '渠道':
                activeTime = None
            else:
                activeTime = calc_not_direct_device_active_time(delivery_time, settleTime)
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
                           paddstr(cusName), paddstr(None), paddstr(settleTime), paddnum(pkPrice),
                           paddnum(pkPriceTax), paddnum(hwPrice), paddnum(hwPriceTax), paddnum(installPrice),
                           paddnum(installPriceTax), paddnum(unitCost), paddstr(activeTime), paddstr(activeMerchant),
                           paddnum(mode), paddstr(saleType))
            values.append(strValue)
        matchTotal = len(values)
        if matchTotal > 0:
            insertSql = "insert into " + resultTblName + " (sn,device_type,material_code,delivery_order," \
                                                         "delivery_time,delivery_merchant,pk_name,pk_period," \
                                                         "settle_merchant,settle_order,settle_time,pk_price,pk_price_tax,hw_price,hw_price_tax," \
                                                         "install_price,install_price_tax,cost,active_time,active_merchant,flag,channel) values %s" % \
                        ','.join(values)
            if not bizConn.insert(insertSql):
                logger.error("match_direct_settlement_delivery_order fail, sql=%s", insertSql)
                return
        else:
            logger.error("match_not_direct_settlement_delivery_order cusName=%s month=%s devtype=%d need=%d find=%d",
                         cusName, strFinDate,  total, len(values))


def update_device_price():
    ''''''
    bizConn = MySQL(g_pro_biz_dbcfg)
    resultTblName = 'dj_2021_settle_details_record'
    fileName = 'C:\\Users\\333\\Desktop\\记录仪修改.xlsx'
    sheetName = '修改单价'
    data = pd.read_excel(fileName, sheet_name=sheetName, header=0)
    strFormat = "date_format(settle_time,'%Y-%m')"
    for idx in data.index:
        settleTime = data.loc[idx, '月份']
        cusName = data.loc[idx, '客户']
        addpkPrice = data.loc[idx, '价格']
        settleMonth = monthDict.get(settleTime)

        finDate = datetime.datetime.strptime(settleMonth, '%Y-%m-%d')
        strFinDate = finDate.strftime('%Y-%m')

        updateSql = "update dj_2021_settle_details_record set pk_price=pk_price+%f " \
                    "where settle_merchant='%s' and %s='%s'" % (addpkPrice, cusName,strFormat,strFinDate)
        if not bizConn.update(updateSql):
            logger.info("update_device_price fail, sql=%s", updateSql)
            return
        logger.info("update_device_price settleTime=%s settleMerchant=%s ", strFinDate, cusName,)


def update_device_alloc(deviceType, startTime, endTime):
    '''回滚'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    if deviceType == 8:
        detailTable = "dj_2022_settle_details"
        deliveryTable = "dj_2021_delivery_device"
    else:
        detailTable = "dj_2022_settle_details_record"
        deliveryTable = "dj_2021_delivery_device_record"

    sql = f"SELECT sn FROM {detailTable} where settle_time >= '{startTime}' and settle_time < '{endTime}' and is_virtual = 0"
    if not bizConn.query(sql):
        return
    res = bizConn.fetchAllRows()
    snList = []
    progess = 0
    for v in res:
        snList.append(paddstr(v[0]))
        progess += 1
        if len(snList) >= 100:
            updateSql = f"update {deliveryTable} set alloc=0 where sn in ({','.join(snList)})"
            if not bizConn.update(updateSql):
                logger.error("fail, sql=" + updateSql)
                return
            print(f"total={len(res)} progress={progess} ")
            snList = []
    if len(snList) > 0:
        updateSql = f"update {deliveryTable} set alloc=0 where sn in ({','.join(snList)})"
        bizConn.update(updateSql)
    print(f"total={len(res)} done ")


if __name__ == "__main__":
    ''''''
    startTime = '2022-09-01'
    endTime = '2022-12-01'

    #追踪器设备
    #match_gen_sn_settlement_delivery_order(startTime, devType=8, prefix='19')
    #记录仪设备
    match_gen_sn_settlement_delivery_order(startTime, devType=6, prefix='SH23G')
    #'SH26H', 'SH23G', 'SH24G'
    #车充设备
    #match_gen_sn_settlement_delivery_order(startTime, devType=12501, prefix='864')

    #回滚分配的设备
    # update_device_alloc(8, startTime, endTime)
    # update_device_alloc(6, startTime, endTime)





