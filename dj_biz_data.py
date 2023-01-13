# -*- coding:utf-8 -*-
from __future__ import print_function
from pyhive import presto  # or import hive
import datetime
import re

import random
import logging
from dateutil.relativedelta import relativedelta
from dateutil import  rrule
from dateutil import parser
from MysqlUtility import MySQL,paddstr,paddnum

logger = logging.getLogger('mylog')
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler('my.log')
fileHandler.setLevel(logging.DEBUG)

cnslhandler = logging.StreamHandler()
cnslhandler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
cnslhandler.setFormatter(formatter)
logger.addHandler(cnslhandler)

g_tsp_srcdbcfg = {'host': '192.168.3.227', 'port': 3306, 'user': 'dev_admin', 'passwd': '1f7e1ed2',
               'db': 'glsx_biz_data', 'charset': 'utf8'}

g_dev_audit_dbcfg = {'host': '192.168.3.227', 'port': 3306, 'user': 'dev_admin', 'passwd': '1f7e1ed2',
                 'db': 'glsx_audit', 'charset': 'utf8'}
g_dev_biz_dbcfg = {'host': '192.168.3.227', 'port': 3306, 'user': 'dev_admin', 'passwd': '1f7e1ed2',
               'db': 'glsx_biz_data', 'charset': 'utf8'}

fetchsize = 1000
env = 'pro'
if env == 'dev':
    g_papdev_dbcfg = {'host': '47.115.93.31', 'port': 3307, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i',
                      'db': 'glsx_pap', 'charset': 'utf8'}
    g_fmrs_dbcfg = {'host': '47.115.150.194', 'port': 3307, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i', 'db': 'gps',
                    'charset': 'utf8'}
    g_gldj_dbcfg = {'host': '47.115.93.31', 'port': 3306, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i',
                    'db': 'glsx_dj', 'charset': 'utf8'}
    g_ghdj_dbcfg = {'host': '47.97.203.216', 'port': 3306, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i', 'db': 'glsx_dj',
                    'charset': 'utf8'}

    g_xydj_dbcfg = {'host': '121.18.128.91', 'port': 3306, 'user': 'biz_glsx_dj', 'passwd': 'fdaewip', 'db': 'glsx_dj',
                   'charset': 'utf8'}

    g_cyb_dbcfg = {'host': '120.77.222.103', 'port': 3306, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i', 'db': 'cyb_os',
                   'charset': 'utf8'}
    g_supply_dbcfg = {'host': '47.115.93.31', 'port': 3306, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i',
                      'db': 'glsx_supplychain', 'charset': 'utf8'}

    glsx_sales_dbcfg ={'host': '120.77.222.103', 'port': 3306, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i',
                    'db': 'glsx_sales', 'charset': 'utf8'}

    g_audit_dbcfg = {'host': '192.168.3.227', 'port': 3306, 'user': 'dev_admin', 'passwd': '1f7e1ed2',
                       'db': 'glsx_audit', 'charset': 'utf8'}

    g_biz_dbcfg = {'host': '192.168.3.227', 'port': 3306, 'user': 'dev_admin', 'passwd': '1f7e1ed2',
                     'db': 'glsx_biz_data', 'charset': 'utf8'}

    g_glsx_ddh_dbcfg =  {'host': '172.16.0.161', 'port': 3306, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i', 'db': 'glsx_ddh',
                   'charset': 'utf8'}


    g_presto_host = '192.168.5.35'
    g_presto_port = 9090
else:
    g_papdev_dbcfg = {'host': '192.168.5.23', 'port': 3307, 'user': 'data_house_user', 'passwd': 'ZhaiMiAxIg',
                      'db': 'glsx_pap', 'charset': 'utf8'}

    g_fmrs_dbcfg = {'host': '192.168.5.22', 'port': 3307, 'user': 'data_house_user', 'passwd': 'ZhaiMiAxIg', 'db': 'gps',
                    'charset': 'utf8'}

    g_gldj_dbcfg = {'host': '192.168.5.23', 'port': 3306, 'user': 'data_house_user', 'passwd': 'ZhaiMiAxIg',
                    'db': 'glsx_dj', 'charset': 'utf8'}

    g_ghdj_dbcfg = {'host': '47.97.203.216', 'port': 3306, 'user': 'data_house_user', 'passwd': 'ZhaiMiAxIg', 'db': 'glsx_dj',
                    'charset': 'utf8'}

    g_ghdj_dbcfg = {'host': '47.97.203.216', 'port': 3306, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i', 'db': 'glsx_dj',
                    'charset': 'utf8'}

    g_xydj_dbcfg = {'host': '121.18.128.91', 'port': 3306, 'user': 'biz_glsx_dj', 'passwd': 'fdaewip', 'db': 'glsx_dj',
                   'charset': 'utf8'}

    g_cyb_dbcfg = {'host': '172.16.0.161', 'port': 3306, 'user': 'data_house_user', 'passwd': 'ZhaiMiAxIg', 'db': 'cyb_os',
                   'charset': 'utf8'}

    g_supply_dbcfg = {'host': '192.168.5.23', 'port': 3306, 'user': 'data_house_user', 'passwd': 'ZhaiMiAxIg',
                      'db': 'glsx_supplychain', 'charset': 'utf8'}

    glsx_sales_dbcfg = {'host': '172.16.0.161', 'port': 3306, 'user': 'data_house_user', 'passwd': 'ZhaiMiAxIg',
                        'db': 'glsx_sales', 'charset': 'utf8'}

    g_audit_dbcfg = {'host': '192.168.5.20', 'port': 3307, 'user': 'data_house_user', 'passwd': 'ZhaiMiAxIg', 'db': 'glsx_audit',
                    'charset': 'utf8'}

    g_biz_dbcfg = {'host': '192.168.5.20', 'port': 3306, 'user': 'biz_glsx_data', 'passwd': 'unEKczDc', 'db': 'glsx_biz_data',
                    'charset': 'utf8'}

    g_glsx_ddh_dbcfg =  {'host': '172.16.0.161', 'port': 3306, 'user': 'user_zhaimx', 'passwd': 'dOCN0L7i', 'db': 'glsx_ddh',
                   'charset': 'utf8'}

    g_presto_host = '192.168.5.35'
    g_presto_port = 9090

def genRandomTime(actDate):
    '''根据日期生产随机时间'''
    hour =  random.randint(0,199)%13 +8 #8点开始
    munite = random.randint(0,199)%60
    sencond = random.randint(0,199)%60
    strTime = "%s %02d:%02d:%02d"%(actDate,hour,munite,sencond)
    return  strTime


def getTableSyncLog(auditConn,tblName):
    '''获取'''
    if False == auditConn.query("select synId from glsx_sync_log where tblName='%s'"%tblName):
        logger.error("getTableSyncLog failed, tblName=%s",tblName)
        return
    result = auditConn.fetchOneRow()
    ret = 0
    if result is None:
        ret = 0
    else:
        if result[0] is None:
            ret = 0
        else:
            ret = int(result[0])
    logger.info("getTableSyncLog tablename=%s result=%d",tblName,ret)
    return ret


def updateTableSynLog(auditConn, tblName, idx):
    '''更新'''
    if not auditConn.insert("insert into glsx_sync_log (tblName,synId,createtime,updatetime) values "
                 "('%s',%d,now(),now()) on duplicate key update synId=values(synId),updatetime=values(updatetime)"%(tblName,idx)):
        logger.error("updateTableSynLog failed, tblName=%s id=%d", tblName, id)
        return
    logger.info("updateTableSynLog tablename=%s id=%d", tblName, idx)


class DevInfo:
    sn = ''
    merId = 0
    merName = ''

g_dev_map_dict = {} #全局存储设备映射信息
def query_cache_dev_mapping():
    '''查询设备映射并缓存起来'''
    bizDbConn = MySQL(g_dev_biz_dbcfg)
    sql = "select fakeSn,realSn,merchant_id,merchant_name from device_mapping"
    if not bizDbConn.query(sql):
        logger.error("query_cache_dev_mapping fail,sql=%s",sql)
        return
    result = bizDbConn.fetchAllRows()
    for v in result:
        dev = DevInfo()
        dev.sn = v[0]
        dev.merId = v[2]
        dev.merName = v[3]
        g_dev_map_dict[v[1]] = dev
    logger.info("query_cache_dev_mapping total = %d", len(g_dev_map_dict.keys()))


def sync_car_permanent(dataFrom):
    '''同步常住地'''
    fetch = fetchsize
    if dataFrom == 'DJ':#广联嘀加
        srcDbConn = MySQL(g_gldj_dbcfg)
        tblName = 'dj_car_permanent_gl'
    elif dataFrom == 'GUARD':#广汇嘀加
        srcDbConn = MySQL(g_ghdj_dbcfg)
        tblName = 'dj_car_permanent_gh'
    else:
        return
    auditConn = MySQL(g_audit_dbcfg)
    strColunms = "sn,stand_no,name,mobile,location,distance,is_nearest,vehicle_id,total_stay_times,azimuth,azimuth_desc," \
                 "merchant_code,pmerchant_name,merchant_id,created_by,created_date,updated_by,updated_date,deleted_flag"

    bizscreenConn = MySQL(g_biz_dbcfg)
    startId = getTableSyncLog(auditConn, tblName)
    count = 0
    while True:
        sql = "select id,%s from dj_car_permanent where  id > %d and deleted_flag = 'N' order by id limit %d" % (strColunms, startId, fetch)
        if not srcDbConn.query(sql):
            logger.error("sync_car_permanent query error, sql=" + sql)
            return
        arrValues = []
        result = srcDbConn.fetchAllRows()
        for v in result:
            arr = []
            id = int(v[0])
            count += 1
            if id > startId:
                startId = id
            '''
            strColunms = "sn,stand_no,name,mobile,location,distance,is_nearest,vehicle_id,total_stay_times,azimuth,azimuth_desc," \
            "merchant_code,pmerchant_name,merchant_id,created_by,created_date,updated_by,updated_date,deleted_flag"
            '''
            sn = v[1]
            fkInfo = g_dev_map_dict.get(sn,None)
            if fkInfo is None:
                logger.warning("sync_car_permanent can not find fksn, sn=%s",sn)
                continue
            sn = fkInfo.sn
            merId = fkInfo.merId
            merName = fkInfo.merName
            arr.append(paddstr(sn));arr.append(paddstr(v[2]));arr.append(paddstr(v[3]));arr.append(paddstr(v[4]));arr.append(paddstr(v[5]))
            arr.append(paddnum(v[6]));arr.append(paddnum(v[7]));arr.append(paddnum(v[8]));arr.append(paddnum(v[9]))
            arr.append(paddnum(v[10]));arr.append(paddstr(v[11]));arr.append(paddstr(v[12]));arr.append(paddstr(merName));arr.append(paddnum(merId))
            arr.append(paddstr(v[15]));arr.append(paddstr(v[16]));arr.append(paddstr(v[17]));arr.append(paddstr(v[18]));arr.append(paddstr(v[19]))
            value = "(" + ",".join(arr) + ")"
            arrValues.append(value)
        if len(result) == 0:
            logger.info ("sync_car_permanent total count=%d" % count)
            break
        if len(arrValues) > 0:
            insSql = "insert into dj_car_permanent (" + strColunms + ") values " + ",".join(arrValues)
            if False == bizscreenConn.insert(insSql):
                logger.error ("sync_car_permanent dst_conn.insert failed:%s" % insSql)
                break
            logger.info ("sync_car_permanent progress count=%d" % count)
            updateTableSynLog(auditConn, tblName, startId)


def sync_car_maintain(dataFrom):
    '''同步维修保养'''
    fetch = fetchsize
    if dataFrom == 'DJ':#广联嘀加
        srcDbConn = MySQL(g_gldj_dbcfg)
        tblName = 'dj_car_maintain_gl'
    elif dataFrom == 'GUARD':#广汇嘀加
        srcDbConn = MySQL(g_ghdj_dbcfg)
        tblName = 'dj_car_maintain_gh'
    else:
        return
    auditConn = MySQL(g_audit_dbcfg)

    strColunms = "sn,stand_no,total_mileage,in_time,prev_mileage,first_time," \
                 "remind_type,remind_rule,diff_mileage,diff_time,is_new,number_of_shops," \
                 "numbers,distance,svrtime,merchant_id,merchant_name,created_by,created_date,updated_by,deleted_flag," \
                 "plate_number,name,mobile,current_day_time,company,car_brand,car_model,car_system,regist_time,insurance_time," \
                 "status,diff_backshop_mileage,diff_backshop_time,vehicle_id,follow_result,follow_time,follow_remarks,maintain_source" \

    bizscreenConn = MySQL(g_biz_dbcfg)
    startId = getTableSyncLog(auditConn,tblName)
    count = 0
    while (True):
        sql = "select id,%s from dj_car_maintain where  id > %d and deleted_flag = 'N' order by id limit %d" % (
            strColunms,startId, fetch)
        if False == srcDbConn.query(sql):
            logger.error("sync_car_maintain query error, sql=" + sql)
            return
        arrValues = []
        result = srcDbConn.fetchAllRows()
        for v in result:
            arr = [];
            id = int(v[0]);
            count += 1
            if id > startId:
                startId = id

            sn = v[1]
            fkInfo = g_dev_map_dict.get(sn,None)
            if fkInfo is None:
                logger.warn("sync_car_maintain can not find fksn, sn=%s",sn)
                continue
            sn = fkInfo.sn
            merId = fkInfo.merId
            merName = fkInfo.merName

            arr.append(paddstr(sn));arr.append(paddstr(v[2]));arr.append(paddnum(v[3]));arr.append(paddstr(v[4]));
            arr.append(paddnum(v[5]));arr.append(paddstr(v[6]));arr.append(paddnum(v[7]));arr.append(paddnum(v[8]));
            arr.append(paddnum(v[9])); arr.append(paddnum(v[10]));arr.append(paddnum(v[11]));arr.append(paddnum(v[12]))
            arr.append(paddnum(v[13]));arr.append(paddnum(v[14]));arr.append(paddstr(str(v[15]))); arr.append(paddnum(merId))
            arr.append(paddstr(merName));arr.append(paddstr(v[18]));arr.append(paddstr(v[19]));arr.append(paddstr(v[20]));arr.append(paddstr(v[21]))

            arr.append(paddstr(v[22]));arr.append(paddstr(v[23]));arr.append(paddstr(v[24]));arr.append(paddstr(v[25]))
            arr.append(paddstr(v[26]));arr.append(paddstr(v[27]));arr.append(paddstr(v[28]));arr.append(paddstr(v[29]))
            arr.append(paddstr(v[30]));arr.append(paddstr(v[31]));arr.append(paddnum(v[32]));arr.append(paddnum(v[33]))
            arr.append(paddnum(v[34]));arr.append(paddnum(v[35]));arr.append(paddnum(v[36]));arr.append(paddstr(v[37]))
            arr.append(paddstr(v[38]));arr.append(paddnum(v[39]))
            value = "(" + ",".join(arr) + ")"
            arrValues.append(value)
        if len(result) == 0:
            logger.info ("sync_car_maintain total count=%d" % count)
            break
        if len(arrValues) > 0:
            insSql = "insert into dj_car_maintain (" + strColunms + ") values " + ",".join(arrValues)
            if False == bizscreenConn.insert(insSql):
                logger.error ("sync_car_maintain dst_conn.insert failed:%s" % insSql)
                break
            logger.info ("sync_car_maintain progress count=%d" % count)
        updateTableSynLog(auditConn, tblName, startId)


def sync_car_collision(dataFrom):
    '''同步维修保养'''
    fetch = 5#fetchsize
    if dataFrom == 'DJ':#广联嘀加
        srcDbConn = MySQL(g_gldj_dbcfg)
        tblName = 'dj_car_collision_gl'
    elif dataFrom == 'GUARD':#广汇嘀加
        srcDbConn = MySQL(g_ghdj_dbcfg)
        tblName = 'dj_car_collision_gh'
    else:
        return
    strColunms = "sn,stand_no,collision_time,collision_type,address,longitude,latitude,distance,speed," \
                 "merchant_id,merchant_name,created_by,created_date,updated_by,updated_date,deleted_flag," \
                 "name,mobile,status,follow_result,follow_time,vehicle_id,shop_warning_id,is_new," \
                 "number_of_shops,follow_remarks "
    auditConn = MySQL(g_audit_dbcfg)

    bizscreenConn = MySQL(g_biz_dbcfg)
    startId = getTableSyncLog(auditConn,tblName)
    count = 0
    while (True):
        sql = "select id,%s from dj_car_collision where id > %d and deleted_flag = 'N' order by id limit %d" % (
            strColunms,startId, fetch)
        if False == srcDbConn.query(sql):
            logger.error("dj_car_collision query error, sql=" + sql)
            return
        arrValues = []
        result = srcDbConn.fetchAllRows()
        for v in result:
            arr = [];
            id = int(v[0]);
            count += 1
            if id > startId:
                startId = id
            sn = v[1]
            fkInfo = g_dev_map_dict.get(sn,None)
            if fkInfo is None:
                logger.warning("sync_car_collision can not find fksn, sn=%s",sn)
                continue
            sn = fkInfo.sn
            merId = fkInfo.merId
            merName = fkInfo.merName
            #strColunms = "sn,stand_no,collision_time,collision_type,address,longitude,latitude,distance,speed," \
            #"merchant_id,merchant_name,created_by,created_date,updated_by,updated_date,deleted_flag"
            #"name,mobile,status,follow_result,follow_time,vehicle_id,shop_warning_id,is_new,number_of_shops,follow_remarks "

            arr.append(paddstr(sn));arr.append(paddstr(v[2]));arr.append(paddstr(v[3]));arr.append(paddnum(v[4]));
            arr.append(paddstr(v[5]));arr.append(paddstr(v[6]))
            arr.append(paddstr(v[7]));arr.append(paddnum(v[8]));arr.append(paddnum(v[9]));
            arr.append(paddnum(merId));arr.append(paddstr(merName))
            arr.append(paddstr(v[12]));arr.append(paddstr(v[13]));arr.append(paddstr(str(v[14]))); arr.append(paddstr(v[15]))
            arr.append(paddstr(v[16]))
            arr.append(paddstr(v[17]));arr.append(paddstr(v[18]));arr.append(paddnum(v[19]));arr.append(paddnum(v[20]))
            arr.append(paddstr(v[21]));arr.append(paddnum(v[22]));arr.append(paddnum(v[23]));arr.append(paddnum(v[24]))
            arr.append(paddnum(v[25]));arr.append(paddstr(v[26]))
            value = "(" + ",".join(arr) + ")"
            arrValues.append(value)
        if len(result) == 0:
            logger.info ("dj_car_collision total count=%d" % count)
            break
        if len(arrValues) > 0:
            insSql = "insert into dj_car_collision (" + strColunms + ") values " + ",".join(arrValues)
            if False == bizscreenConn.insert(insSql):
                logger.error ("dj_car_collision dst_conn.insert failed:%s" % insSql)
                break
            logger.info ("dj_car_collision progress count=%d" % count)
        updateTableSynLog(auditConn, tblName, startId)


def sync_car_loss_warning(dataFrom):
    '''流失预计'''
    fetch = fetchsize
    if dataFrom == 'DJ':#广联嘀加
        srcDbConn = MySQL(g_gldj_dbcfg)
        tblName = 'dj_car_loss_warning_gl'
    elif dataFrom == 'GUARD':#广汇嘀加
        srcDbConn = MySQL(g_ghdj_dbcfg)
        tblName = 'dj_car_loss_warning_gh'
    else:
        return
    strColunms = "sn,stand_no,enter_time,out_time,trigger_time,alarm_type,is_new,number_of_shops,current_mileage," \
                 "merchant_id,merchant_name,created_by,created_date,updated_by,updated_date,deleted_flag,merchant_type," \
                 "plate_number,name,mobile,task_id,vehicle_id,competition_rel_id"
    auditConn = MySQL(g_audit_dbcfg)
    bizscreenConn = MySQL(g_biz_dbcfg)
    startId = getTableSyncLog(auditConn,tblName)
    count = 0
    while True:
        sql = "select id,%s from dj_car_loss_warning where id > %d and deleted_flag = 'N' order by id limit %d" % (
            strColunms,startId, fetch)
        if not srcDbConn.query(sql):
            logger.error("sync_car_loss_warning query error, sql=" + sql)
            return
        arrValues = []
        result = srcDbConn.fetchAllRows()
        for v in result:
            arr = []
            id = int(v[0])
            count += 1
            if id > startId:
                startId = id

            sn = v[1]
            fkInfo = g_dev_map_dict.get(sn,None)
            if fkInfo is None:
                logger.warning("sync_car_collision can not find fksn, sn=%s",sn)
                continue
            sn = fkInfo.sn
            merId = fkInfo.merId
            merName = fkInfo.merName

            arr.append(paddstr(sn));arr.append(paddstr(v[2]));arr.append(paddstr(v[3]));arr.append(paddstr(v[4]))
            arr.append(paddstr(v[5]));arr.append(paddnum(v[6]));arr.append(paddnum(v[7]));arr.append(paddnum(v[8]))
            arr.append(paddnum(v[9]));arr.append(paddnum(merId));arr.append(paddstr(merName))
            arr.append(paddstr(v[12]));arr.append(paddstr(v[13]));arr.append(paddstr(str(v[14]))); arr.append(paddstr(v[15]))
            arr.append(paddstr(v[16]));arr.append(paddnum(v[17]))
            arr.append(paddstr(v[18]));arr.append(paddstr(v[19]));arr.append(paddstr(v[20]));arr.append(paddstr(v[21]));
            arr.append(paddnum(v[22]));arr.append(paddnum(v[23]))
            value = "(" + ",".join(arr) + ")"
            arrValues.append(value)
        if len(result) == 0:
            logger.info("sync_car_loss_warning total count=%d" % count)
            break
        if len(arrValues) > 0:
            insSql = "insert into dj_car_loss_warning (" + strColunms + ") values " + ",".join(arrValues)
            if not bizscreenConn.insert(insSql):
                logger.error("sync_car_loss_warning dst_conn.insert failed:%s" % insSql)
                break
            logger.info("sync_car_loss_warning progress count=%d" % count)
        updateTableSynLog(auditConn, tblName, startId)


def sync_car_shop_warning(dataFrom):
    '''到店'''
    fetch = fetchsize
    if dataFrom == 'DJ':#广联嘀加
        srcDbConn = MySQL(g_gldj_dbcfg)
        tblName = 'dj_car_shop_warning_gl'
    elif dataFrom == 'GUARD':#广汇嘀加
        srcDbConn = MySQL(g_ghdj_dbcfg)
        tblName = 'dj_car_shop_warning_gh'
    else:
        return

    auditConn = MySQL(g_audit_dbcfg)
    strColunms = "sn,stand_no,enter_time,out_time,trigger_time,alarm_type,is_new,last_time_distance,last_time_day," \
                 "merchant_id,merchant_name,created_by,created_date,updated_by,updated_date,deleted_flag," \
                 "plate_number,name,mobile,merchant_type,task_id,vehicle_id"
    bizscreenConn = MySQL(g_biz_dbcfg)
    startId = getTableSyncLog(auditConn,tblName)
    count = 0
    while True:
        sql = "select id,%s from dj_car_shop_warning where id > %d and deleted_flag = 'N' order by id limit %d" % (
            strColunms,startId, fetch)
        if False == srcDbConn.query(sql):
            logger.error("sync_car_shop_warning query error, sql=" + sql)
            return
        arrValues = []
        result = srcDbConn.fetchAllRows()
        for v in result:
            arr = []
            id = int(v[0])
            count += 1
            if id > startId:
                startId = id

            sn = v[1]
            fkInfo = g_dev_map_dict.get(sn, None)
            if fkInfo is None:
                logger.warning("sync_car_shop_warning can not find fksn, sn=%s",sn)
                continue
            sn = fkInfo.sn
            merId = fkInfo.merId
            merName = fkInfo.merName

            arr.append(paddstr(sn));arr.append(paddstr(v[2]));arr.append(paddstr(v[3]));arr.append(paddstr(v[4]))
            arr.append(paddstr(v[5]));arr.append(paddnum(v[6]));arr.append(paddnum(v[7]));arr.append(paddnum(v[8]))
            arr.append(paddnum(v[9]));arr.append(paddnum(merId));arr.append(paddstr(merName))
            arr.append(paddstr(v[12]));arr.append(paddstr(v[13]));arr.append(paddstr(str(v[14])));arr.append(paddstr(v[15]))
            arr.append(paddstr(v[16]))
            arr.append(paddstr(v[17]));arr.append(paddstr(v[18]));arr.append(paddstr(v[19]));arr.append(paddnum(v[20]))
            arr.append(paddstr(v[21]));arr.append(paddnum(v[22]))
            value = "(" + ",".join(arr) + ")"
            arrValues.append(value)
        if len(result) == 0:
            logger.info ("sync_car_shop_warning total count=%d" % count)
            break
        if len(arrValues) > 0:
            insSql = "insert into dj_car_shop_warning (" + strColunms + ") values " + ",".join(arrValues)
            if not bizscreenConn.insert(insSql):
                logger.error ("sync_car_shop_warning dst_conn.insert failed:%s" % insSql)
                break
            logger.info ("sync_car_shop_warning progress count=%d" % count)
        updateTableSynLog(auditConn, tblName, startId)


def sync_car_days_mileage(dataFrom):
    ''''''
    fetch = fetchsize
    if dataFrom == 'DJ':#广联嘀加
        srcDbConn = MySQL(g_gldj_dbcfg)
        tblName = 'dj_car_days_mileage_gl'
    elif dataFrom == 'GUARD':#广汇嘀加
        srcDbConn = MySQL(g_ghdj_dbcfg)
        tblName = 'dj_car_days_mileage_gh'
    else:
        return

    strColunms = "sn,total_mileage,current_dates,write_time,device_type,merchant_id," \
                 "created_by,created_date,updated_by,updated_date,deleted_flag"
    auditConn = MySQL(g_audit_dbcfg)
    bizscreenConn = MySQL(g_biz_dbcfg)
    startId = getTableSyncLog(auditConn,tblName)
    count = 0
    while True:
        sql = "select id,%s from dj_car_days_mileage where id > %d and deleted_flag = 'N' order by id limit %d" % (
            strColunms,startId, fetch)
        if not srcDbConn.query(sql):
            logger.error("sync_car_days_mileage query error, sql=" + sql)
            return
        arrValues = []
        result = srcDbConn.fetchAllRows()
        for v in result:
            ''''''
            arr = []
            id = int(v[0])
            count += 1
            if id > startId:
                startId = id

            sn = v[1]
            fkInfo = g_dev_map_dict.get(sn,None)
            if fkInfo is None:
                logger.warning("sync_car_day_mileage can not find fksn, sn=%s",sn)
                continue
            sn = fkInfo.sn
            merId = fkInfo.merId
            merName = fkInfo.merName

            arr.append(paddstr(sn));arr.append(paddnum(v[2]));arr.append(paddstr(v[3]));arr.append(paddstr(v[4]))
            arr.append(paddnum(v[5]));arr.append(paddnum(merId));arr.append(paddstr(v[7]));arr.append(paddstr(v[8]))
            arr.append(paddstr(v[9]));arr.append(paddstr(v[10]));arr.append(paddstr(v[11]))
            value = "(" + ",".join(arr) + ")"
            arrValues.append(value)
        if len(result) == 0:
            logger.info("sync_car_days_mileage total count=%d" % count)
            break
        if len(arrValues) > 0:
            insSql = "insert into dj_car_days_mileage (" + strColunms + ") values " + ",".join(arrValues)
            insSql += " on duplicate key update updated_date=values(updated_date)"
            if False == bizscreenConn.insert(insSql):
                logger.error("sync_car_days_mileage dst_conn.insert failed:%s" % insSql)
                break
            logger.info("sync_car_days_mileage progress count=%d" % count)
        updateTableSynLog(auditConn, tblName, startId)


def sync_sys_merchant(dataFrom):
    '''同步商户数据'''
    fetch = fetchsize
    count = 0
    if dataFrom == 'DJ':  # 广联嘀加
        srcDbConn = MySQL(g_gldj_dbcfg)
        tblName = 'dj_system_merchant_gl'
    elif dataFrom == 'GUARD':  # 广汇嘀加
        srcDbConn = MySQL(g_ghdj_dbcfg)
        tblName = 'dj_system_merchant_gh'
        dataSrc = '1'
    elif dataFrom == 'XY':  # 轩宇
        srcDbConn = MySQL(g_xydj_dbcfg)
        tblName = 'dj_system_merchant_xy'
    else:
        return
    auditConn = MySQL(g_audit_dbcfg)
    strColunms = "merchant_id,account_name,merchant_name,short_name,user_type,merchant_type,contact_name," \
                 "phone,channel_id,address,province_id,province_name,city_id,city_name,area_id,area_name," \
                 "lng,lat,shop_photo,business_hours,parent_merchant_id,parent_merchant_name,created_by," \
                 "created_date,updated_by,updated_date,deleted_flag"#,datafrom

    screenConn = MySQL(g_audit_dbcfg)
    startId = getTableSyncLog(auditConn, tblName)
    count = 0
    while (True):
        #去掉爱卡汽车的
        sql = "select * from dj_system_merchant where id > %d and (parent_merchant_id <> 44194495 or parent_merchant_id is NULL ) " \
              "and deleted_flag = 'N' order by id limit %d" % (startId, fetch)
        if False == srcDbConn.query(sql):
            logger.error("sync_sys_merchant query error, sql=" + sql)
            return
        arrValues = []
        result = srcDbConn.fetchAllRows()
        for v in result:
            arr = [];
            id = int(v[0]);
            count += 1
            if id > startId:
                startId = id

            provCode = v[11]
            cityCode = v[13]

            arr.append(paddnum(v[1]));arr.append(paddstr(v[2]));arr.append(paddstr(v[3]));arr.append(paddstr(v[4]))
            arr.append(paddnum(v[5]));arr.append(paddnum(v[6]));arr.append(paddstr(v[7]));arr.append(paddstr(v[8]))
            arr.append(paddnum(v[9]));arr.append(paddstr(v[10]));arr.append(paddnum(provCode));arr.append(paddstr(v[12]))
            arr.append(paddnum(cityCode));arr.append(paddstr(v[14]));arr.append(paddnum(v[15]));arr.append(paddstr(v[16]))
            arr.append(paddnum(v[17]));arr.append(paddnum(v[18]));arr.append(paddstr(v[19]));arr.append(paddstr(v[20]))
            arr.append(paddnum(v[21]));arr.append(paddstr(v[22]));arr.append(paddstr(v[23]));arr.append(paddstr(v[24]))
            arr.append(paddstr(v[25]));arr.append(paddstr(v[26]));arr.append(paddstr(v[27]));#arr.append(dataSrc)
            value = "(" + ",".join(arr) + ")"
            arrValues.append(value)
        if len(arrValues) == 0:
            logger.info("sync_sys_merchant total count=%d" % count)
            break
        insSql = "insert into dj_system_merchant (" + strColunms + ") values " + ",".join(arrValues)
        insSql += " on duplicate key update updated_date=values(updated_date)"
        if False == screenConn.insert(insSql):
            logger.error("sync_sys_merchant dst_conn.insert failed:%s" % insSql)
            break
        logger.info("sync_sys_merchant progress count=%d" % count)
        updateTableSynLog(auditConn, tblName, startId)


def sync_system_login_log(dataFrom):
    '''系统登录日志'''
    auditConn = MySQL(g_audit_dbcfg)
    fetch = fetchsize
    if dataFrom == 'DJ':  # 广联嘀加
        srcDbConn = MySQL(g_gldj_dbcfg)
        tblName = 'dj_system_login_log_gl'
    elif dataFrom == 'GUARD':  # 广汇嘀加
        srcDbConn = MySQL(g_ghdj_dbcfg)
        tblName = 'dj_system_login_log_gh'
    else:
        return

    screenConn = MySQL(g_biz_dbcfg)
    startId = getTableSyncLog(auditConn, tblName)
    count = 0
    while (True):
        sql = "select slogin.id,suser.merchant_id,smer.merchant_name,slogin.login_name,slogin.login_time,slogin.created_date,slogin.updated_date from " \
              "(select id,user_id,login_name,login_time,created_date,updated_date from dj_system_login_log " \
              "where id > %d order by id limit %d) as slogin " \
              "left join dj_system_user as suser on (slogin.user_id = suser.id) " \
              "left join dj_system_merchant as smer on (suser.merchant_id = smer.merchant_id)" % (
        startId, fetch)
        if False == srcDbConn.query(sql):
            logger.error("sync_system_login_log query error, sql=" + sql)
            return
        arrValues = []
        result = srcDbConn.fetchAllRows()
        for v in result:
            arr = [];
            id = int(v[0]);
            count += 1
            if id > startId:
                startId = id

            merId = v[1]
            merName = v[2]
            loginName = v[3]
            loginTime = v[4]
            createTime = v[5]
            updateTime = v[6]
            arr.append(paddnum(merId));arr.append(paddstr(merName));arr.append(paddstr(loginName));arr.append(paddstr(loginTime))
            arr.append(paddstr(createTime));arr.append(paddstr(updateTime))
            value = "(" + ",".join(arr) + ")"
            arrValues.append(value)
        if len(arrValues) == 0:
            logger.info("sync_system_login_log total count=%d" % count)
            break
        insSql = "insert into dj_system_login_log (merchant_id,merchant_name,login_name,login_time,create_time,update_time) values " + ",".join(arrValues)
        if False == screenConn.insert(insSql):
            logger.error("sync_system_login_log dst_conn.insert failed:%s" % insSql)
            break
        logger.info("sync_system_login_log progress count=%d" % count)
        updateTableSynLog(auditConn, tblName, startId)

def sync_audit_dj_device():
    '''同步dj设备'''
    fetch = fetchsize

    #srcDbConn = MySQL(g_audit_dbcfg)
    srcDbConn = MySQL(g_dev_audit_dbcfg)
    strSrcColunms = "device_sn,sim_no,device_active_time,seq_no,insure_period,device_type,effect_date,end_date, " \
                    "'1' as device_status,merchant,merchant_id,settle_merchant"

    strDstColunms = "device_sn,sim_no,active_time,seq_no,period,device_type,effect_date,end_date,device_status," \
                 "merchant_id,merchant,settle_merchant,created_by,created_date,updated_by,updated_date,deleted_flag"
    bizDataConn = MySQL(g_biz_dbcfg)
    #startId = getTableSyncLog(bizDataConn,tblName)
    #if startId is None:
    startId = 0
    count = 0
    while True:
        sql = "select id,%s from audit_insure_order where device_active_time > '2018-01-01 00:00:00' and settle_merchant <> ''" \
              "and id > %d order by id limit %d"% (strSrcColunms,startId, fetch)
        if False == srcDbConn.query(sql):
            print ("sync_audit_device fail,sql=%s"%sql)
            return
        result = srcDbConn.fetchAllRows()
        arrValue = []
        for v in result:
            count += 1
            id = int(v[0])
            if id > startId:
                startId = id
            sn = paddstr(v[1])
            sim = paddstr(v[2])
            activeTime = genRandomTime(str(v[3])[0:10])
            seqno = paddstr(v[4])
            if v[5] is None:
                period = paddnum(36)
            else:
                period = paddnum(v[5])
            devtype = paddnum(v[6])

            if v[7] is None:
                ''''''
                effectDate = paddstr(activeTime)
                endDate = datetime.datetime.strptime(activeTime,'%Y-%m-%d %H:%M:%S') + relativedelta(months=36)
                endDate = paddstr(str(endDate))
            else:
                effectDate = paddstr(v[7])
                endDate = paddstr(v[8])

            status = paddstr(v[9])
            merchant = paddstr(v[10])
            merchantId = paddnum(v[11])
            settleMer = paddstr(v[12])
            strActDate = paddstr(activeTime)
            createDate = paddstr(activeTime) #使用激活时间
            updateDate = paddstr(activeTime)  # 使用激活时间
            createBy = paddstr('system')
            updateBy = paddstr('system')
            delFlag = paddstr('N')
            #strDstColunms = "device_sn,sim_no,active_time,seq_no,period,device_type,effect_date,end_date,device_status," \
            #                "merchant,created_by,created_date,updated_by,updated_date,deleted_flag"
            strValue = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"%(
                sn,sim,strActDate,seqno,period,devtype,effectDate,endDate,status,merchantId,merchant,
                settleMer,createBy,createDate,updateBy,updateDate,delFlag)
            arrValue.append(strValue)
        if len(arrValue) == 0:
            break
        insertSql = "insert into dj_device_info (%s) VALUES %s" % (strDstColunms,','.join(arrValue))
        if False == bizDataConn.insert(insertSql):
            print ("sync_audit_device fail, sql=%s"%insertSql)
            return
        print ("sync_audit_device progress count=%d"%count)
    print("sync_audit_device done total=%d" % count)


def sync_audit_record_dj_device():
    '''同步dj设备'''
    fetch = fetchsize

    #srcDbConn = MySQL(g_audit_dbcfg)
    srcDbConn = MySQL(g_dev_audit_dbcfg)
    strSrcColunms = "sn,iccid,active_time,merchant,merchant_id,settle_merchat"

    strDstColunms = "device_sn,sim_no,active_time,seq_no,period,device_type,effect_date,end_date,device_status," \
                 "merchant_id,merchant,settle_merchant,created_by,created_date,updated_by,updated_date,deleted_flag"
    bizDataConn = MySQL(g_biz_dbcfg)
    #startId = getTableSyncLog(bizDataConn,tblName)
    #if startId is None:
    startId = 0
    count = 0
    while True:
        sql = "select id,%s from audit_record_device where settle_merchat is not null  and id > %d order by id limit %d"% (strSrcColunms,startId, fetch)
        if False == srcDbConn.query(sql):
            print ("sync_audit_record_dj_device fail,sql=%s"%sql)
            return
        result = srcDbConn.fetchAllRows()
        arrValue = []
        for v in result:
            count += 1
            id = int(v[0])
            if id > startId:
                startId = id
            sn = paddstr(v[1])
            sim = paddstr(v[2])
            activeTime = paddstr(str(v[3]))#genRandomTime(str(v[3])[0:10])
            seqno = paddstr('')
            period = paddnum(36)
            devtype = paddnum(6)
            effectDate = paddstr(str(v[3]))
            endDate =  paddstr(str(v[3] + relativedelta(months=36)))
            status = paddstr(2)
            merchant = paddstr(v[4])
            merchantId = paddnum(v[5])
            settleMer = paddstr(v[6])
            strActDate = activeTime
            createDate = activeTime #使用激活时间
            updateDate = activeTime  # 使用激活时间
            createBy = paddstr('system')
            updateBy = paddstr('system')
            delFlag = paddstr('N')
            #strDstColunms = "device_sn,sim_no,active_time,seq_no,period,device_type,effect_date,end_date,device_status," \
            #                "merchant,created_by,created_date,updated_by,updated_date,deleted_flag"
            strValue = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"%(
                sn,sim,strActDate,seqno,period,devtype,effectDate,endDate,status,merchantId,merchant,
                settleMer,createBy,createDate,updateBy,updateDate,delFlag)
            arrValue.append(strValue)
        if len(arrValue) == 0:
            break
        insertSql = "insert into dj_device_info (%s) VALUES %s" % (strDstColunms,','.join(arrValue))
        if False == bizDataConn.insert(insertSql):
            print ("sync_audit_record_dj_device fail, sql=%s"%insertSql)
            return
        print ("sync_audit_record_dj_device progress count=%d"%count)
    print("sync_audit_record_dj_device done total=%d" % count)


def sync_tsp_device():
    ''''''
    fetch = fetchsize
    srcDbConn = MySQL(g_tsp_srcdbcfg)

    strColunms = "sn,imsi,simno,iccid,device_type,device_name,package_name,status,active_time,period,pk_begin_time,pk_end_time," \
                    "active_month,create_time,update_time"

    bizDataConn = MySQL(g_biz_dbcfg)
    startId = 0
    count = 0
    while True:
        sql = "select id,%s from tsp_device_info where  id > %d order by id limit %d"% (strColunms,startId, fetch)
        if False == srcDbConn.query(sql):
            print ("sync_audit_device fail,sql=%s"%sql)
            return
        result = srcDbConn.fetchAllRows()
        arrValue = []
        for v in result:
            count += 1
            id = int(v[0])
            if id > startId:
                startId = id
            sn = paddstr(v[1])
            imsi = paddstr(v[2])
            simno = paddstr(v[3])
            iccid = paddstr(v[4])
            devtype = paddnum(v[5])
            devname = paddstr(v[6])
            pkname = paddstr(v[7])
            status = paddnum(v[8])
            activeTime = paddstr(v[9])

            pkstart = paddstr(v[11])
            pkend = paddstr(v[12])
            rrPeriod = rrule.rrule(freq=rrule.MONTHLY,dtstart=v[11],until=v[12])
            period = paddnum(rrPeriod.count() - 1)

            activemonth = paddstr(v[13])
            createDate = paddstr(v[14])
            updateDate = paddstr(v[15])

            strColunms = "sn,imsi,simno,iccid,device_type,device_name,package_name,status,active_time,period,pk_begin_time,pk_end_time," \
                         "active_month,create_time,update_time"
            strValue = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"%(sn,imsi,simno,iccid,devtype,devname,pkname,status,
                        activeTime,period,pkstart,pkend,activemonth,createDate,updateDate)
            arrValue.append(strValue)

        if len(arrValue) == 0:
            break

        insertSql = "insert into tsp_device_info (%s) VALUES %s" % (strColunms,','.join(arrValue))
        insertSql += " on duplicate key update update_time=values(update_time)"
        if False == bizDataConn.insert(insertSql):
            print ("sync_tsp_device fail, sql=%s"%insertSql)
            return
        print ("sync_tsp_device progress count=%d"%count)
    print("sync_tsp_device done total=%d" % count)


def insert_fmrs_device(dbConn, values):
    ''''''
    sql = "insert into fmrs_device_info (sn,imsi,sim_no,iccid,device_type,device_name,status,active_time," \
          "period,pk_begin_time,pk_end_time,create_time,update_time) values " + ','.join(values) + \
          " on DUPLICATE key update update_time = values(update_time)"
    if False == dbConn.update(sql):
        logger.error("insert_fmrs_device fail, sql=%s",sql)
        return -1
    return 0


def sync_fmrs_device(year = 2020):
    ''''''
    fmrsConn = MySQL(g_fmrs_dbcfg)
    bizConn = MySQL(g_biz_dbcfg)
    if year < 2020:
        strStart = "%04d-01-01 00:00:00"%year
        strEnd = "%04d-01-01 00:00:00"%(year+1)
        sql = '''
        SELECT DISTINCT a.`sn`,a.`simPhone`,a.`iccid`,a.`imsi`,8 AS deviceType,'GPS' AS deviceName,a.`activeDate`,
	f.`creditPeriod`,e.`successTime`,e.`endTime`
	FROM `d_track_info` a
	LEFT JOIN `d_vehicle` b ON b.`vehicleId`=a.`vehicleId`
	LEFT JOIN `d_class` c ON c.`classid`=b.`classId`
	LEFT JOIN `d_group` d ON d.`vehicleGroupId`=c.`vehicleGroupId` AND d.`vehicleGroupId`!=267
	LEFT JOIN `d_vehicle_insurance` e ON e.`vehicleId`=a.`vehicleId`
	LEFT JOIN `d_vehicle_workorder` f ON f.`workOrderId`=b.`workOrderId`
	WHERE a.`activeDate` BETWEEN '{}' AND '{}'
	AND LEFT(a.`sn`,2)!='66' AND d.`vehicleGroupId`>=0
        '''
        sql = sql.format(strStart,strEnd)
    else:
        sql = '''
        SELECT DISTINCT a.`sn`,a.`simPhone`,a.`iccid`,a.`imsi`,8 AS deviceType,'GPS' AS deviceName,a.`activeDate`,
	f.`creditPeriod` ,e.`successTime` ,e.`endTime`
	FROM `d_track_info` a
	LEFT JOIN `d_vehicle` b ON b.`vehicleId`=a.`vehicleId`
	LEFT JOIN `d_class` c ON c.`classid`=b.`classId`
	LEFT JOIN `d_group` d ON d.`vehicleGroupId`=c.`vehicleGroupId` AND (d.`vehicleGroupId`!=78 AND d.`vehicleGroupId`!=267)
	LEFT JOIN `d_vehicle_insurance` e ON e.`vehicleId`=a.`vehicleId`
	LEFT JOIN `d_vehicle_workorder` f ON f.`workOrderId`=b.`workOrderId`
	WHERE a.`activeDate` BETWEEN '2020-12-14 18:30:00' AND '2020-12-16 10:41:48'
		AND LEFT(a.`sn`,2)!='66' AND d.`vehicleGroupId`>=0
        '''
    if False == fmrsConn.query(sql):
        logger.error("sync_fmrs_device fail,sql=%s",sql)
        return

    result = fmrsConn.fetchAllRows()
    arrValues = []
    total = 0
    for v in result:
        sn = v[0]
        sim = v[1]
        iccid = v[2]
        imsi = v[3]
        devType = v[4]
        devName = v[5]
        activeTime = v[6]
        period = v[7]
        if period is None or period == '':
            period = 36
        else:
            res = re.sub('\D',"",period)
            if res == '':
                period = 36
            else:
                period = int(res)
        if period > 100:
            logger.warn("sync_fmrs_device period error, %s",v[7])
            period = 36

        succTime = activeTime
        endTime = activeTime + relativedelta(months=period)
        createTime = activeTime
        updateTime = endTime
        status = 1
        #(sn,imsi,sim_no,iccid,device_type,device_name,status,active_time,period,pk_begin_time,pk_end_time,create_time,update_time)
        strValue = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"%(paddstr(sn),paddstr(imsi),paddstr(sim),paddstr(iccid),paddnum(devType),
                    paddstr(devName),paddnum(status),paddstr(str(activeTime)),paddnum(period),paddstr(str(succTime)),
                    paddstr(str(endTime)),paddstr(str(createTime)),paddstr(str(updateTime)))
        total += 1
        arrValues.append(strValue)
        if len(arrValues) >= 1000:
            if 0 != insert_fmrs_device(bizConn,arrValues):
                return
            else:
                logger.info("sync_fmrs_device year=%d count=%s",year,total)
                arrValues = []
    if len(arrValues) > 0:
        if 0 != insert_fmrs_device(bizConn, arrValues):
            return
        else:
            logger.info("sync_fmrs_device year=%d count=%s", year, total)
    logger.info("sync_fmrs_device done year=%d total=%s", year, total)


def adjust_tsp_device_time():
    '''更新大于144月的'''
    sql = 'SELECT id,pk_begin_time,pk_end_time from tsp_device_info where period <> 144 and period <> 120 and period > 100'
    bizDataConn = MySQL(g_biz_dbcfg)
    if False == bizDataConn.query(sql):
        logger.error("adjust_tsp_device_time fail,sql",sql)
        return
    result = bizDataConn.fetchAllRows()
    for v in result:
        id = v[0]
        pkbegin = v[1]
        pkend = pkbegin + relativedelta(months=120)
        print (id,pkbegin,pkend)
        updateSql = "update tsp_device_info set period=%d,pk_end_time='%s' where id = %d" %(120,str(pkend),id)
        if False == bizDataConn.update(updateSql):
            logger.error("adjust_tsp_device_time fail,sql=%s",updateSql)
            return


def adjust_tsp_error_time():
    ''''''
    sql = 'SELECT id,active_time,period from tsp_device_info where active_time > pk_end_time'
    bizDataConn = MySQL(g_biz_dbcfg)
    if False == bizDataConn.query(sql):
        logger.error("adjust_tsp_device_time fail,sql",sql)
        return
    result = bizDataConn.fetchAllRows()
    for v in result:
        id = v[0]
        pkbegin = v[1]
        period = int(v[2])
        pkend = pkbegin + relativedelta(months=period)
        print (id,pkbegin,pkend)
        updateSql = "update tsp_device_info set pk_begin_time='%s',pk_end_time='%s' where id = %d" %(str(pkbegin),str(pkend),id)
        if False == bizDataConn.update(updateSql):
            logger.error("adjust_tsp_device_time fail,sql=%s",updateSql)
            return

def merchant_adjust():
    ''''''
    auditDbConn = MySQL(g_dev_audit_dbcfg)
    devbizDbConn = MySQL( g_dev_audit_dbcfg)#g_dev_biz_dbcfg
    probizDbConn = MySQL(g_biz_dbcfg)
    strColunms = "merchant_id,account_name,merchant_name,short_name,user_type,merchant_type,contact_name," \
                 "phone,channel_id,address,province_id,province_name,city_id,city_name,area_id,area_name," \
                 "lng,lat,shop_photo,business_hours,parent_merchant_id,parent_merchant_name,created_by," \
                 "created_date,updated_by,updated_date,deleted_flag"
    sql = "select merchant_id,merchant_name,settle_mer_name from t_merchant_settle_relation where settle_mer_name <> ''"
    if False == auditDbConn.query(sql):
        logger.error("merchant_adjust fail,sql=%s",sql)
        return
    result = auditDbConn.fetchAllRows()
    count = 0
    fail = 0
    for v in result:
        merId = int(v[0])
        merName = str(v[1])
        setlleName = str(v[2])

        sql = "select id,%s from dj_system_merchant where merchant_id = %d "%(strColunms,merId)
        if False == devbizDbConn.query(sql):
            logger.error("merchant_adjust fail,sql=%s", sql)
            return

        arrValues = []
        result = devbizDbConn.fetchAllRows()
        for v in result:
            arr = [];
            count += 1
            provCode = v[11]
            cityCode = v[13]

            arr.append(paddnum(v[1]));arr.append(paddstr(v[2]));arr.append(paddstr(v[3]));arr.append(paddstr(v[4]))
            arr.append(paddnum(v[5]));arr.append(paddnum(v[6]));arr.append(paddstr(v[7]));arr.append(paddstr(v[8]))
            arr.append(paddnum(v[9]));arr.append(paddstr(v[10]));arr.append(paddnum(provCode));arr.append(paddstr(v[12]))
            arr.append(paddnum(cityCode));arr.append(paddstr(v[14]));arr.append(paddnum(v[15]));arr.append(paddstr(v[16]))
            arr.append(paddnum(v[17]));arr.append(paddnum(v[18]));arr.append(paddstr(v[19]));arr.append(paddstr(v[20]))
            arr.append(paddnum(v[21]));arr.append(paddstr(v[22]));arr.append(paddstr(v[23]));arr.append(paddstr(v[24]))
            arr.append(paddstr(v[25]));arr.append(paddstr(v[26]));arr.append(paddstr(v[27]));
            arr.append(paddstr(setlleName))
            value = "(" + ",".join(arr) + ")"
            arrValues.append(value)
        if len(arrValues) == 0:
            fail += 1
            logger.warn("merchant_adjust merId = %d merName=%s fail=%s",merId,merName,fail)
            continue

        '''
        insSql = "insert into dj_system_merchant (" + strColunms + ",settle_merchant_name" + ") values " + ",".join(arrValues)
        insSql += " on duplicate key update updated_date=values(updated_date)"
        if False == probizDbConn.insert(insSql):
            logger.error("sync_sys_merchant dst_conn.insert failed:%s" % insSql)
            break
        logger.info("sync_sys_merchant progress count=%d" % count)
        '''


def device_period_adjust():
    '''设备套餐修正'''
    bizConn = MySQL(g_biz_dbcfg)
    sql = "SELECT id,active_time FROM dj_device_info where period is  NULL and effect_date is NULL"
    if False == bizConn.query(sql):
        logger.error("device_period_adjust fail,sql=%s",sql)
        return
    res = bizConn.fetchAllRows()
    total = len(res)
    count = 0
    for v in res:
        id = int(v[0])
        activeTime = v[1]
        period = 36
        startTime = activeTime
        endTime = activeTime + relativedelta(months=36)
        updateSql = "update dj_device_info set period=%d,effect_date='%s',end_date='%s' where id = %d"%(
            period,str(startTime),str(endTime),id)
        count += 1
        if False == bizConn.update(updateSql):
            logger.error("device_period_adjust sql=%s",updateSql)
            return
        if count%1000 == 0:
            logger.info("device_period_adjust total=%d progress=%d",total,count)
    logger.info("device_period_adjust done total=%d progress=%d", total, count)


if __name__ == "__main__":
    query_cache_dev_mapping()
    #sync_car_collision('DJ')
    #sync_car_collision('GUARD')

    #sync_car_loss_warning('DJ')
    #sync_car_loss_warning('GUARD')

    sync_car_maintain('DJ')
    #sync_car_maintain('GUARD')

    #sync_car_shop_warning('DJ')
    #sync_car_shop_warning('GUARD')

    #sync_car_permanent('DJ')
    #sync_car_permanent('GUARD')

    #sync_car_days_mileage('DJ')
    #sync_car_days_mileage('GUARD')

    '''
    sync_sys_merchant('GUARD')
    sync_sys_merchant('DJ')
    sync_sys_merchant('XY')
    '''

    #sync_system_login_log('DJ')
    #sync_system_login_log('GUARD')

    #sync_audit_dj_device()

    #period = rrule.rrule(freq=rrule.MONTHLY, dtstart=parser.parse('2018-12-11 00:00:00'), until=parser.parse("2019-02-11 00:00:00"))
    #print (period.count())
    #sync_tsp_device()

    #rrPeriod = rrule.rrule(freq=rrule.MONTHLY, dtstart=parser.parse('2017-12-29 10:29:52'), until=parser.parse('2117-12-29 10:29:52'))
    #print (rrPeriod.count())

    #adjust_tsp_device_time()
    #adjust_tsp_error_time()

    #merchant_adjust()

    #sync_fmrs_device(2017)
    #sync_fmrs_device(2018)
    #sync_fmrs_device(2019)
    #sync_fmrs_device(2020)

    #device_period_adjust()

    #sync_audit_record_dj_device()

