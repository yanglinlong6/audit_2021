import random

from dateutil.relativedelta import relativedelta
from pyhive import presto

from MysqlUtility import MySQL, paddstr, paddnum
import logging

logger = logging.getLogger('utils')
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler('utils.log')
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


def update_merchant_createtime():
    '''根据激活时间更新商户的创建时间'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = '''
    SELECT active_merchant,minActiveTime,minSettleTime,B.created_date FROM 
(SELECT active_merchant,MIN(active_time) as minActiveTime,MIN(settle_time) as minSettleTime FROM dj_2021_settle_details 
where active_merchant <> ''  GROUP BY active_merchant) as A  
LEFT JOIN dj_system_merchant B ON (A.active_merchant = B.merchant_name)
    '''
    if not bizConn.query(sql):
        logger.error("update_merchant_createtime fail, sql=%s", sql)
        return
    res = bizConn.fetchAllRows()
    for v in res:
        merchant = v[0]
        minActiveTime = v[1]
        minSettleTime = v[2]
        createTime = v[3]
        logger.info("merchant=%s minActiveTime=%s minSettleTime=%s,createTime=%s",
                    merchant, minActiveTime, minSettleTime,createTime)
        if minSettleTime is not None and minSettleTime < minActiveTime:
            minActiveTime = minSettleTime

        minusDays = random.randint(3, 15)
        newCreateTime = minActiveTime - relativedelta(days=minusDays)
        newUpdateTime = newCreateTime
        updateSql = "update dj_system_merchant set created_date='%s',updated_date='%s' where merchant_name='%s'" % (
            newCreateTime, newUpdateTime, merchant)
        if not bizConn.update(updateSql):
            return



def add_unmatch_merchant():
    '''未匹配的商户'''
    settleMerList = [
        '兰州超凡汽车用品有限公司',
        '长沙县湘龙街道达阳汽车用品商行',
    ]

    #settleMerchant = '庞大精配网（天津）网络科技有限公司'
    prestoConn = presto.connect(host=g_presto_host, port=g_presto_port, catalog='hive', schema='glsx_data_warehouse',
                                source='zmx').cursor()

    '''t_dj_merchant t_dim_merchant '''
    for merchant in settleMerList:
        settleMerchant = merchant
        pstSql = "select merchant_id,merchant_name,merchant_type,prov_code,province,city_code,city,area_code,area,lng,lat " \
                 "from t_dim_merchant where merchant_name like '%{}%' limit 1".format(merchant)

        #pstSql = "select merchant_id,merchant_name,merchant_type,province_id,province_name,city_id,city_name,area_id,area_name,lng,lat " \
        #         "from t_dj_merchant where merchant_name like '%{}%' limit 1".format(merchant)

        prestoConn.execute(pstSql)
        merchantArr = []
        strColunms = "(merchant_id,account_name,merchant_name,user_type,merchant_type,province_id,province_name," \
                     "city_id,city_name,area_id,area_name,lng,lat,settle_merchant_name,created_by," \
                     "created_date,updated_by,updated_date,deleted_flag)"

        for v in prestoConn:
            ''''''
            merchantId = v[0]
            accountName = merchantId
            merchantName = v[1]
            usertype = 0
            # merchant_id,merchant_name,merchant_type,prov_code,province,city_code,city,area_code,area,lng,lat
            merchantType = v[2]
            provCode = v[3]
            provName = v[4]
            cityCode = v[5]
            cityName = v[6]
            areaCode = v[7]
            area = v[8]
            lng = v[9]
            lat = v[10]
            createdBy = "admin"
            strValue = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s,now(),'N')" % (
                paddnum(merchantId), paddstr(merchantId),paddstr(merchantName),paddnum(usertype),
                paddnum(merchantType), paddnum(provCode), paddstr(provName),
                paddnum(cityCode), paddstr(cityName), paddnum(areaCode), paddstr(area),
                paddnum(lng), paddnum(lat), paddstr(settleMerchant), paddstr(createdBy), paddstr(createdBy))
            merchantArr.append(strValue)

        if len(merchantArr) > 0:
            insertSql = "insert into dj_system_merchant " + strColunms + " values " + ",".join(merchantArr) + \
                        " on duplicate key update merchant_id=values(merchant_id)"

            bizConn = MySQL(g_pro_biz_dbcfg)
            if not bizConn.insert(insertSql):
                logger.error("add_unmatch_merchant fail, sql=%s", insertSql)
                return
        else:
            logger.warning("add_unmatch_merchant %s found=%d", pstSql, len(merchantArr))

    logger.info("add_unmatch_merchant done merchant=%d", len(merchantArr))


def add_spjt_merchant():
    '''结算客户： 济南泰乐汽车用品有限公司'''
    sql = "SELECT merchant_code,name,merchant_name FROM bs_dealer_user_info where `name` like '%顺骋%'"
    settleMerchant = '济南泰乐汽车用品有限公司'
    prestoConn = presto.connect(host=g_presto_host, port=g_presto_port, catalog='hive', schema='glsx_data_warehouse',
                                source='zmx').cursor()
    supplyConn = MySQL(g_supply_dbcfg)
    bizConn = MySQL(g_pro_biz_dbcfg)
    if not supplyConn.query(sql):
        logger.error("add_spjt_merchant fail, sql=%s", sql)
        return
    result = supplyConn.fetchAllRows()
    for v in result:
        merCode = v[0]
        bizMerName = v[1]
        logger.info(v)
        pstSql = "select merchant_id,merchant_name,merchant_type,prov_code,province,city_code,city,area_code,area,lng,lat " \
                 "from t_dim_merchant where merchant_id = '{}' limit 1".format(merCode)
        prestoConn.execute(pstSql)
        merchantArr = []
        strColunms = "(merchant_id,account_name,merchant_name,user_type,merchant_type,province_id,province_name," \
                     "city_id,city_name,area_id,area_name,lng,lat,settle_merchant_name,created_by," \
                     "created_date,updated_by,updated_date,deleted_flag)"

        for v in prestoConn:
            ''''''
            merchantId = v[0]
            accountName = merchantId
            merchantName = bizMerName #v[1]
            usertype = 0
            # merchant_id,merchant_name,merchant_type,prov_code,province,city_code,city,area_code,area,lng,lat
            merchantType = v[2]
            provCode = v[3]
            provName = v[4]
            cityCode = v[5]
            cityName = v[6]
            areaCode = v[7]
            area = v[8]
            lng = v[9]
            lat = v[10]
            createdBy = "admin"
            strValue = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s,now(),'N')" % (
                paddnum(merchantId), paddstr(merchantId),paddstr(merchantName),paddnum(usertype),
                paddnum(merchantType), paddnum(provCode), paddstr(provName),
                paddnum(cityCode), paddstr(cityName), paddnum(areaCode), paddstr(area),
                paddnum(lng), paddnum(lat), paddstr(settleMerchant), paddstr(createdBy), paddstr(createdBy))
            merchantArr.append(strValue)

        if len(merchantArr) > 0:
            insertSql = "insert into dj_system_merchant " + strColunms + " values " + ",".join(merchantArr) + \
                        " on duplicate key update merchant_id=values(merchant_id)"
            if not bizConn.insert(insertSql):
                logger.error("add_spjt_merchant fail, sql=%s", insertSql)
                return
        else:
            logger.warning("add_spjt_merchant %s found=%d", pstSql, len(merchantArr))


def update_merchant_settle_details(settleMer='上海冠松汽车用品发展有限公司'):
    '''森风集团'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    settleMerList = []
    #sql = "SELECT merchant_name from dj_system_merchant where settle_merchant_name='%s'" % settleMer
    sql = "SELECT merchant_name from dj_system_merchant where settle_merchant_name = '{}' " \
          "and province_name = '上海' and merchant_name <> '{}'".format(settleMer, settleMer)
    if not bizConn.query(sql):
        return
    result = bizConn.fetchAllRows()
    for v in result:
        settleMerList.append(v[0])

    #sql = "SELECT id FROM dj_2021_settle_details where settle_time > '2021-01-01' and settle_merchant = '%s'" % settleMer
    sql = "SELECT id FROM dj_2021_settle_details_record where settle_time > '2021-01-01' " \
          "and active_time is not null and settle_merchant = '%s'" % settleMer
    if not bizConn.query(sql):
        return
    res = bizConn.fetchAllRows()
    for v in res:
        idx = v[0]
        activeMer = random.choice(settleMerList)
        updateSql = "update dj_2021_settle_details_record set active_merchant = '%s' where id = %d" %(activeMer, idx)
        if not bizConn.update(updateSql):
            logger.error("update_merchant_settle_details settlemer=%s, sql=%s", updateSql)
            return
        logger.info(updateSql)
    logger.info("update_merchant_settle_details %s total=%d", activeMer, len(res))


def update_spjt_merchant_settle_details():
    '''森风集团'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    merchantName = '顺骋集团'
    merchantList = []
    sql = "SELECT merchant_name from dj_system_merchant where merchant_name like '%{}%'".format(merchantName)
    if not bizConn.query(sql):
        return
    result = bizConn.fetchAllRows()
    for v in result:
        merchantList.append(v[0])

    sql = "SELECT active_merchant,count(1) FROM dj_2021_settle_details " \
          "where settle_time > '2021-01-01' and active_merchant like '%{}%' " \
          "group by active_merchant".format(merchantName)
    if not bizConn.query(sql):
        return
    merchantActives = {}
    res = bizConn.fetchAllRows()
    topMerchant = []
    for v in res:
        mer = v[0]
        total = v[1]
        merchantActives[mer] = total
        if total >= 200:
            topMerchant.append(mer)
        logger.info("update_spjt_merchant_settle_details %s total=%d", mer, total)

    expMerchants = list(set(merchantList).difference(set(merchantActives.keys())))
    addMerchants = random.sample(expMerchants, 30)
    sql = "SELECT id FROM dj_2021_settle_details " \
          "where settle_time > '2021-01-01' and active_merchant = '%s' limit 60" % topMerchant[0]
    if not bizConn.query(sql):
        return
    index = 0
    result = bizConn.fetchAllRows()
    for v in result:
        idx = v[0]
        if index < len(addMerchants):
            activeMer = addMerchants[index]
        else:
            activeMer = random.choice(addMerchants)
        index += 1
        updateSql = "update dj_2021_settle_details set active_merchant = '%s' where id = %d" % (activeMer, idx)
        if not bizConn.update(updateSql):
            logger.error("update_merchant_settle_details settlemer=%s, sql=%s", updateSql)
            return
        logger.info(updateSql)

    logger.info(addMerchants)



def add_new_merchant():
    '''结算客户： 济南泰乐汽车用品有限公司'''
    prestoConn = presto.connect(host=g_presto_host, port=g_presto_port, catalog='hive', schema='glsx_dws',
                                source='zmx').cursor()

    bizConn = MySQL(g_pro_biz_dbcfg)

    strColunms = "(merchant_id,account_name,merchant_name,user_type,merchant_type,province_id,province_name," \
                 "city_id,city_name,area_id,area_name,lng,lat,settle_merchant_name,created_by," \
                 "created_date,updated_by,updated_date,deleted_flag)"
    pstSql = "select merchant_id,account_name,merchant_name,user_type,merchant_type,province_id,province_name," \
             "city_id,city_name,area_id,area_name,lng,lat,created_by,created_date,updated_by," \
             "updated_date from dim_device_merchant " \
             "where created_date > '2021-01-01' and created_date < '2021-10-01' and business_type='glDj' " \
             "and parent_merchant_name ='庞大汽贸集团股份有限公司' limit 105"
    settleMerchant = '庞大汽贸集团股份有限公司'
    prestoConn.execute(pstSql)
    merchantArr = []
    for v in prestoConn:
        ''''''
        merchantId = v[0]
        accountName = v[1]
        merchantName = v[2]
        usertype = v[3]
        merchantType = v[4]
        provCode = v[5]
        provName = v[6]
        cityCode = v[7]
        cityName = v[8]
        areaCode = v[9]
        area = v[10]
        lng = v[11]
        lat = v[12]
        createdBy = v[13]
        createdDate = v[14]
        updatedBy = v[15]
        updatedDate = v[16]
        strValue = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'N')" % (
            paddnum(merchantId), paddstr(accountName),paddstr(merchantName),paddnum(usertype),
            paddnum(merchantType), paddnum(provCode), paddstr(provName),
            paddnum(cityCode), paddstr(cityName), paddnum(areaCode), paddstr(area),
            paddnum(lng), paddnum(lat), paddstr(settleMerchant), paddstr(createdBy),paddstr(createdDate),
            paddstr(updatedBy),paddstr(updatedDate))
        merchantArr.append(strValue)
        logger.info(strValue)

    if len(merchantArr) > 0:
        logger.info(len(merchantArr))

        insertSql = "insert into dj_system_merchant " + strColunms + " values " + ",".join(merchantArr) + \
                    " on duplicate key update merchant_id=values(merchant_id)"
        if not bizConn.insert(insertSql):
            logger.error("add_spjt_merchant fail, sql=%s", insertSql)
            return

    else:
        logger.warning("add_spjt_merchant %s found=%d", pstSql, len(merchantArr))



def update_merchant_settle_details_shbx(settleMer='广汇汽车'):
    '''森风集团'''
    bizConn = MySQL(g_pro_biz_dbcfg)
    settleMerList = []
    #sql = "SELECT merchant_name from dj_system_merchant where settle_merchant_name='%s'" % settleMer
    sql = "SELECT merchant_name from dj_system_merchant where settle_merchant_name = '{}' " \
          "and merchant_name not like  '%{}%'".format(settleMer, '上海宝信')
    if not bizConn.query(sql):
        return
    result = bizConn.fetchAllRows()
    for v in result:
        settleMerList.append(v[0])

    #sql = "SELECT id FROM dj_2021_settle_details where settle_time > '2021-01-01' and settle_merchant = '%s'" % settleMer
    sql = "SELECT id FROM dj_2021_settle_details_gps where  active_merchant like '%{}%'".format('上海宝信')
    if not bizConn.query(sql):
        return
    res = bizConn.fetchAllRows()
    for v in res:
        idx = v[0]
        activeMer = random.choice(settleMerList)
        updateSql = "update dj_2021_settle_details_gps set active_merchant = '%s' where id = %d" %(activeMer, idx)
        if not bizConn.update(updateSql):
            logger.error("update_merchant_settle_details settlemer=%s, sql=%s", updateSql)
            return
        logger.info(updateSql)
    logger.info("update_merchant_settle_details %s total=%d", activeMer, len(res))


def random_active_time(year):
    '''
    根据真实设备激活时间段随机数据
    '''
    g_ghdj_dbcfg = {'host': '47.97.203.216', 'port': 3306, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i', 'db': 'glsx_dj',
                    'charset': 'utf8'}
    ghDbConn = MySQL(g_ghdj_dbcfg)
    bizDbConn = MySQL(g_pro_biz_dbcfg)

    strStart = "%04d-01-01"%year
    strEnd = "%04d-01-01" %(year+1)
    strFormat = "date_format(active_date,'%H')"
    sql = "select %s,COUNT(1) from dj_insure_device where active_date > '%s' " \
          "and active_date < '%s' GROUP BY %s"%(strFormat,strStart,strEnd,strFormat)
    hourDict = {}
    if not ghDbConn.query(sql):
        logger.error("random_active_time fail,sql=%s",sql)
        return
    res = ghDbConn.fetchAllRows()
    for v in res:
        hh = int(v[0])
        count = v[1]
        hourDict[hh] = count

    sql = "select id,active_time from dj_2021_settle_details where " \
          "active_time > '%s' and active_time < '%s'" % (strStart, strEnd)
    if not bizDbConn.query(sql):
        logger.error("random_active_time fail,sql=%s", sql)
        return
    result = bizDbConn.fetchAllRows()
    sameActDict = {}
    for row in result:
        idx = row[0]
        actTime = row[1]
        sameActDict[idx] = actTime
    hourHotal = sum(hourDict.values())
    sameTotal = len(sameActDict.keys())
    allSameIds = sameActDict.keys()
    for hour in hourDict.keys():
        calcTotal = int(hourDict.get(hour)*sameTotal/hourHotal)
        logger.info("random_active_time hour=%d total=%d", hour, calcTotal)
        if calcTotal > 0:
            calcIds = random.sample(allSameIds, calcTotal)
            allSameIds = list(set(allSameIds).difference(calcIds))
            for idx in calcIds:
                newActTime = sameActDict.get(idx)
                newActTime = newActTime.replace(hour=hour)
                updateSql = "update dj_2021_settle_details set active_time='%s' " \
                            "where id=%d"%(newActTime, idx)
                if not bizDbConn.update(updateSql):
                    logger.error("random_active_time fail,sql=%s", updateSql)
                    return
            logger.info("random_active_time year=%d hour=%d done", year, hour)
    logger.info("random_active_time year=%d done", year)


def calc_dj_duplicate_device():
    '''统计嘀加重复的设备'''
    sql = "SELECT sn,COUNT(1) from dj_2021_settle_details_return where `return`=0 GROUP BY sn HAVING COUNT(1) > 1"
    bizDbConn = MySQL(g_pro_biz_dbcfg)
    if not bizDbConn.query(sql):
        logger.error("calc_dj_duplicate_device fail, sql=%s", sql)
        return
    res = bizDbConn.fetchAllRows()
    total = 0
    logger.info("calc_dj_duplicate_device total=%d", len(res))
    for v in res:
        sn = v[0]
        querySql = "select id,sn from dj_2021_settle_details " \
                   "where sn='%s' and delivery_order='' order by id desc limit 1" % sn
        if not bizDbConn.query(querySql):
            logger.error("calc_dj_duplicate_device fail, sql=%s", querySql)
            return
        result = bizDbConn.fetchAllRows()
        for row in result:
            idx = row[0]
            newSn = row[1]
            newSn += '1'
            updateSql = "update dj_2021_settle_details set sn='%s' where id=%d" % (newSn, idx)
            if not bizDbConn.update(updateSql):
                logger.error("calc_dj_duplicate_device fail, sql=%s", updateSql)
                return
            total += 1
        if total % 10 == 0:
            logger.info("calc_dj_duplicate_device progress=%d", total)
    logger.info("calc_dj_duplicate_device done, total=%d", total)


def calc_dj_frms_duplicate_device():
    '''计算重复设备'''
    '''统计嘀加重复的设备'''
    #d_eshield_device_details_new_place d_eshield_device_details_2021
    sql = "SELECT sn from d_eshield_device_details_2021 where sn in (SELECT sn FROM dj_2021_settle_details)"
    bizDbConn = MySQL(g_pro_biz_dbcfg)
    if not bizDbConn.query(sql):
        logger.error("calc_dj_duplicate_device fail, sql=%s", sql)
        return
    res = bizDbConn.fetchAllRows()
    total = 0
    logger.info("calc_dj_duplicate_device total=%d", len(res))
    for v in res:
        sn = v[0]
        querySql = "select id,sn from dj_2021_settle_details where sn='%s' " % sn
        if not bizDbConn.query(querySql):
            logger.error("calc_dj_duplicate_device fail, sql=%s", querySql)
            return
        result = bizDbConn.fetchAllRows()
        for row in result:
            idx = row[0]
            newSn = row[1]
            prex = str(random.randint(0,9))
            newSn += prex
            updateSql = "update dj_2021_settle_details set sn='%s' where id=%d" % (newSn, idx)
            if not bizDbConn.update(updateSql):
                logger.error("calc_dj_duplicate_device fail, sql=%s", updateSql)
                return
            total += 1
        if total % 10 == 0:
            logger.info("calc_dj_duplicate_device progress=%d", total)
    logger.info("calc_dj_duplicate_device done, total=%d", total)


def update_return_delievery_duplicate_device():
    '''计算重复设备'''
    '''统计嘀加重复的设备'''
    #d_eshield_device_details_new_place d_eshield_device_details_2021
    sql = "SELECT id,sn,settle_merchant from dj_2021_settle_details where `flag` = 4 and delivery_merchant is null"
    bizDbConn = MySQL(g_pro_biz_dbcfg)
    if not bizDbConn.query(sql):
        logger.error("calc_dj_duplicate_device fail, sql=%s", sql)
        return
    res = bizDbConn.fetchAllRows()
    total = 0
    logger.info("calc_dj_duplicate_device total=%d", len(res))
    for v in res:
        idx = v[0]
        sn = v[1]
        settleMer = v[2]
        prex = str(random.randint(0,9))
        newSn = sn + prex
        updateSql = "update dj_2021_settle_details set sn='%s',delivery_merchant='%s' where id=%d" % (newSn, settleMer,idx)
        if not bizDbConn.update(updateSql):
            logger.error("update_return_delievery_duplicate_device fail, sql=%s", updateSql)
            return
        total += 1
        if total % 10 == 0:
            logger.info("update_return_delievery_duplicate_device progress=%d", total)
    logger.info("update_return_delievery_duplicate_device done, total=%d", total)


def random_active_time_hour(year):
    '''
    根据真实设备激活时间段随机数据
    '''
    g_ghdj_dbcfg = {'host': '47.97.203.216', 'port': 3306, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i', 'db': 'glsx_dj',
                    'charset': 'utf8'}
    ghDbConn = MySQL(g_ghdj_dbcfg)
    bizDbConn = MySQL(g_pro_biz_dbcfg)

    strStart = "%04d-01-01"%year
    strEnd = "%04d-01-01" %(year+1)
    strFormat = "date_format(active_date,'%H')"
    sql = "select %s,COUNT(1) from dj_insure_device where active_date > '%s' " \
          "and active_date < '%s' GROUP BY %s"%(strFormat,strStart,strEnd,strFormat)
    hourDict = {}
    if False == ghDbConn.query(sql):
        logger.error("random_active_time fail,sql=%s",sql)
        return
    res = ghDbConn.fetchAllRows()
    for v in res:
        hh = int(v[0])
        count = v[1]
        hourDict[hh] = count

    sql = "select id,active_time from dj_2021_settle_details where " \
          "active_time > '%s' and active_time < '%s'"%(strStart,strEnd)
    if False == bizDbConn.query(sql):
        logger.error("random_active_time fail,sql=%s",sql)
        return
    result = bizDbConn.fetchAllRows()
    sameActDict = {}
    for row in result:
        idx = row[0]
        actTime = row[1]
        sameActDict[idx] = actTime

    hourHotal = sum(hourDict.values())
    #same
    idsDictArr = [sameActDict]
    for idsDict in idsDictArr:
        sameTotal = len(idsDict.keys())
        allSameIds = idsDict.keys()
        for hour in hourDict.keys():
            calcTotal = int(hourDict.get(hour)*sameTotal/hourHotal)
            logger.info("random_active_time hour=%d total=%d",hour,calcTotal)
            if calcTotal > 0:
                calcIds = random.sample(allSameIds,calcTotal)
                allSameIds = list(set(allSameIds).difference(calcIds))
                for idx in calcIds:
                    actimetime = idsDict.get(idx)
                    actimetime = actimetime.replace(hour=hour)
                    updateSql = "update dj_2021_settle_details set active_time='%s' " \
                                "where id=%d"%(actimetime,idx)
                    if bizDbConn.update(updateSql) == False:
                        logger.error("random_active_time fail,sql=%s",updateSql)
                        return
                logger.info("random_active_time year=%d hour=%d done",year,hour)

    logger.info("random_active_time year=%d done",year)


def update_customer_saletype():
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "select distinct cus_name,sale_type from dj_2022_settlment_order_finance_0506"
    bizConn.query(sql)
    result = bizConn.fetchAllRows()
    for row in result:
        cusName = row[0]
        saleType = row[1]
        updateSql = f"update dj_2022_settlment_order_0506 set sale_type='{saleType}' where cus_name='{cusName}'"
        if not bizConn.update(updateSql):
            print(f"update_customer_saletype fail, sql={updateSql}")
            return
        print(cusName,saleType)


def update_device_type():
    bizConn = MySQL(g_pro_biz_dbcfg)
    sql = "SELECT cus_name,sale_type,COUNT(1) FROM dj_2022_settlment_order_finance_06plus WHERE " \
          "finance_time >= '2022-08-01' and business_type = 'SAAS系统订阅服务—旧版本' GROUP BY cus_name,sale_type"
    if not bizConn.query(sql):
        return
    result = bizConn.fetchAllRows()
    for row in result:
        cusName = row[0]
        saleType = row[1]
        updateSql = f"update dj_2022_settlment_order_06plus set sale_type = '{saleType}' " \
                    f"WHERE finance_time >= '2022-08-01' and cus_name='{cusName}'"
        if not bizConn.update(updateSql):
            return
        print(updateSql)

if __name__ == "__main__":
    ''''''
    #update_device_type()
    #random_active_time_hour(2022)

    #update_merchant_createtime()

    #add_unmatch_merchant()

    #add_spjt_merchant()

    #update_merchant_settle_details('上海冠松汽车用品发展有限公司')

    #add_new_merchant()

    #update_merchant_settle_details_shbx('广汇汽车')

    #random_active_time(2021)

    #calc_dj_duplicate_device()

    #calc_dj_frms_duplicate_device()

    #update_return_delievery_duplicate_device()

    #update_customer_saletype()






