from MysqlUtility import MySQL, paddstr, paddnum
import datetime
from dateutil.relativedelta import relativedelta
import logging

g_pro_biz_dbcfg = {'host': '192.168.5.20', 'port': 3306, 'user': 'biz_glsx_data', 'passwd': 'unEKczDc',
                   'db': 'glsx_biz_data', 'charset': 'utf8'}

g_pro_biz_dbcfg = {'host': '192.168.5.25', 'port': 3310, 'user': 'biz_device_detail', 'passwd': 'M6khz3CQ',
               'db': 'device_detail',
               'charset': 'utf8'}


def get_months(begin, end):
    begin_year, end_year = begin.year, end.year
    begin_month, end_month = begin.month, end.month
    if begin_year == end_year:
        months = end_month - begin_month
    else:
        months = (end_year - begin_year) * 12 + end_month - begin_month
    return months + 1


def calc_gap_month_of_year(year, pk_start, pk_end):
    yearStart = datetime.datetime.strptime(f"{year}-01-01", '%Y-%m-%d')
    yearEnd = datetime.datetime.strptime(f"{year}-12-31", '%Y-%m-%d')
    gap_month = 0

    if pk_start < yearStart < pk_end:
        calcStartTime = yearStart
    elif yearStart <= pk_start <= yearEnd:
        calcStartTime = pk_start
    else:
        calcStartTime = None

    if pk_end < yearStart:
        calcEndTime = None
    elif yearStart <= pk_end <= yearEnd:
        calcEndTime = pk_end
    else:
        calcEndTime = yearEnd

    if calcStartTime is not None and calcEndTime is not None:
        gap_month = get_months(calcStartTime, calcEndTime)
    return gap_month


def calc_remain_year(year, start_share_month, end_share_month, price, remainTotal, amount):
    """
    计算每年余额
    :param year:
    :return:
    """
    gap_months = calc_gap_month_of_year(year, start_share_month, end_share_month)
    if gap_months > 0:
        share = gap_months * price  #
        remainShare = remainTotal - share  #
    else:
        share = 0
        remainShare = 0

    if remainShare > 1.0:
        remainTotal = remainShare
    else:
        remainTotal = amount
        remainShare = 0
    return share, remainShare, remainTotal


def calc_income_share(calcType='收入'):
    """
    计算收入的分摊
    :return:
    """
    fileName = calcType + ".csv"
    fileHandle = open(file=fileName, mode='w', encoding="utf-8")
    bizConn = MySQL(g_pro_biz_dbcfg)
    if calcType == '收入':
        # and settle_merchant='重庆卡达能汽车用品有限公司'
        sql = f"select settle_merchant,case when device_type=8 then '追踪器' when device_type=6 then '记录仪' when device_type=12501 then '车充' end," \
              f"pk_period,DATE_FORMAT(settle_time, '%Y-%m') as settle_month," \
              f"DATE_FORMAT(active_time,'%Y-%m') as active_month,COUNT(1) as count," \
              f"sum(IFNULL(pk_price,0) + IFNULL(hw_price, 0) + IFNULL(install_price, 0)) as amount, channel " \
              f"from dj_device_details where  settle_time < '2022-01-01 00:00:00' and device_type = 8 " \
              f"GROUP BY settle_merchant,device_type,pk_period,DATE_FORMAT(settle_time, '%Y-%m'),DATE_FORMAT(active_time,'%Y-%m'),channel " \
              f"order by settle_month"
    elif calcType == '成本':
        sql = f"select settle_merchant,case when device_type=8 then '追踪器' when device_type=6 then '记录仪' when device_type=12501 then '车充' end," \
              f"pk_period,DATE_FORMAT(settle_time, '%Y-%m') as settle_month," \
              f"DATE_FORMAT(active_time,'%Y-%m') as active_month,COUNT(1) as count," \
              f"sum(IFNULL(hw_cost,0) + IFNULL(install_cost, 0) ) as amount, channel " \
              f"from dj_device_details where  settle_time < '2022-01-01 00:00:00' and device_type = 8 " \
              f"GROUP BY settle_merchant,device_type,pk_period,DATE_FORMAT(settle_time, '%Y-%m'),DATE_FORMAT(active_time,'%Y-%m'),channel " \
              f"order by settle_month"

    if not bizConn.query(sql):
        return
    result = bizConn.fetchAllRows()
    # datetime_2017_end = datetime.datetime.strptime("2017-12-31", '%Y-%m-%d')
    headName = f"客户名称,渠道,服务期限,数量,结算月份,不含税收入,开始计费月份,结束计费月份,不含税单价,月摊收入,"\
          f"2017年摊销金额,2017年末累计摊销余额,2018年摊销金额,"\
          f"2018年末累计摊销余额,2019年摊销金额,2019年末累计摊销余额,2020年摊销金额,2020年末累计摊销余额,2021年摊销金额,"\
          f"2021年末累计摊销余额,2022年摊销金额,2022年末累计摊销余额,产品分类,激活开始月份,激活结束月份 \n"
    fileHandle.write(headName)
    for row in result:
        settleMer = row[0]
        devName = row[1]
        period = row[2]
        if period == 0 or period is None:
            period = 12
        settle_start_month = row[3]
        settle_end_month = datetime.datetime.strptime(settle_start_month, '%Y-%m') \
                           + relativedelta(months=period) - relativedelta(days=1)

        active_month = row[4]
        total = row[5]
        amount = row[6]
        channel = row[7]

        if active_month is None:
            start_share = '2025-12'
            active_start_month = ''
            active_end_month = ''
        else:
            start_share = active_month
            active_start_month = datetime.datetime.strptime(start_share, '%Y-%m')
            active_end_month = active_start_month + relativedelta(months=period) - relativedelta(days=1)

        if (channel == '代销' or channel == '广汇') and (active_start_month == '' or active_start_month < settle_end_month):
            start_share = settle_start_month

        start_share_month = datetime.datetime.strptime(start_share, '%Y-%m')
        end_share_month = start_share_month + relativedelta(months=period) - relativedelta(days=1)

        price = amount / total
        share_per_month = amount / period

        '''
        gap_2018_months = get_months(start_share_month, datetime_2017_end)
        if gap_2018_months > 0:
            remainShare2018 = amount - gap_2018_months * share_per_month #2018年末摊销余额
        else:
            remainShare2018 = 0

        if remainShare2018 > 0:
            remainTotal = remainShare2018
        else:
            remainTotal = amount
        '''

        remainTotal = amount

        share_2017, remainShare2017, remainTotal = calc_remain_year(2017, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)

        share_2018, remainShare2018, remainTotal = calc_remain_year(2018, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)

        share_2019, remainShare2019, remainTotal = calc_remain_year(2019, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)
        share_2020, remainShare2020, remainTotal = calc_remain_year(2020, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)
        share_2021, remainShare2021, remainTotal = calc_remain_year(2021, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)
        share_2022, remainShare2022, remainTotal = calc_remain_year(2022, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)

        lineName = f"{settleMer},{channel},{period},{total},{settle_start_month},{amount},{settle_start_month},"\
              f"{settle_end_month},{price},{share_per_month},{share_2017},{remainShare2017},{share_2018},"\
              f"{remainShare2018},{share_2019},{remainShare2019},{share_2020},{remainShare2020},"\
              f"{share_2021},{remainShare2021},{share_2022},{remainShare2022},{devName},"\
              f"{active_start_month},{active_end_month}\n"
        fileHandle.write(lineName)
    fileHandle.close()


def calc_return_share(calcType):
    """
    退货
    :return:
    """
    fileName = calcType + ".csv"
    fileHandle = open(file=fileName, mode='w', encoding="utf-8")
    bizConn = MySQL(g_pro_biz_dbcfg)
    if calcType == '退货成本':
        sql = f"select settle_merchant,case when device_type=8 then '追踪器' when device_type=6 then '记录仪' when device_type=12501 then '车充' end," \
              f"pk_period,DATE_FORMAT(settle_time, '%Y-%m') as settle_month,COUNT(1) as count," \
              f"sum(IFNULL(hw_cost,0) + IFNULL(install_cost, 0) ) as amount, channel " \
              f"from dj_device_details_return where  settle_time < '2022-01-01 00:00:00' and device_type = 8 " \
              f"GROUP BY settle_merchant,device_type,pk_period,DATE_FORMAT(settle_time, '%Y-%m'),channel " \
              f"order by settle_month"
    elif calcType == '退货收入':
        sql = f"select settle_merchant,case when device_type=8 then '追踪器' when device_type=6 then '记录仪' when device_type=12501 then '车充' end," \
              f"pk_period,DATE_FORMAT(settle_time, '%Y-%m') as settle_month,COUNT(1) as count," \
              f"sum(0-IFNULL(pk_price, 0) + IFNULL(hw_price, 0) + IFNULL(install_price, 0)) as amount, channel " \
              f"from dj_device_details_return where  settle_time < '2022-01-01 00:00:00' and device_type = 8 " \
              f"GROUP BY settle_merchant,device_type,pk_period,DATE_FORMAT(settle_time, '%Y-%m'),channel " \
              f"order by settle_month"
    if not bizConn.query(sql):
        return
    result = bizConn.fetchAllRows()
    headName = f"客户名称,渠道,服务期限,数量,结算月份,不含税收入,开始计费月份,结束计费月份,不含税单价,月摊收入,"\
          f"2017年摊销金额,2017年末累计摊销余额,2018年摊销金额,"\
          f"2018年末累计摊销余额,2019年摊销金额,2019年末累计摊销余额,2020年摊销金额,2020年末累计摊销余额,2021年摊销金额,"\
          f"2021年末累计摊销余额,2022年摊销金额,2022年末累计摊销余额,产品分类 \n"
    fileHandle.write(headName)
    for row in result:
        settleMer = row[0]
        devName = row[1]
        period = row[2]
        if period == 0 or period is None:
            period = 12
        settle_start_month = row[3]
        settle_end_month = datetime.datetime.strptime(settle_start_month, '%Y-%m') \
                           + relativedelta(months=period) - relativedelta(days=1)
        total = row[4]
        amount = row[5]
        channel = row[6]

        start_share_month = datetime.datetime.strptime(settle_start_month, '%Y-%m')
        end_share_month = settle_end_month

        price = amount / total
        share_per_month = amount / period

        remainTotal = amount

        share_2017, remainShare2017, remainTotal = calc_remain_year(2017, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)

        share_2018, remainShare2018, remainTotal = calc_remain_year(2018, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)

        share_2019, remainShare2019, remainTotal = calc_remain_year(2019, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)
        share_2020, remainShare2020, remainTotal = calc_remain_year(2020, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)
        share_2021, remainShare2021, remainTotal = calc_remain_year(2021, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)
        share_2022, remainShare2022, remainTotal = calc_remain_year(2022, start_share_month,
                                                                    end_share_month, share_per_month, remainTotal,
                                                                    amount)

        lineName = f"{settleMer},{channel},{period},{total},{settle_start_month},{amount},{settle_start_month},"\
              f"{settle_end_month},{price},{share_per_month},{share_2017},{remainShare2017},{share_2018},"\
              f"{remainShare2018},{share_2019},{remainShare2019},{share_2020},{remainShare2020},"\
              f"{share_2021},{remainShare2021},{share_2022},{remainShare2022},{devName}\n"
        fileHandle.write(lineName)
    fileHandle.close()

'''
计算分摊数据
'''
if __name__ == "__main__":
    calc_income_share(calcType='收入')
    calc_income_share(calcType='成本')
    #calc_return_share('退货收入')
    #calc_return_share('退货成本')
