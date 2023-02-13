import datetime
from numpy import *

import pymysql
import pandas as pd

# con = pymysql.connect(host="192.168.5.20",
#                       port=3306,
#                       user="biz_glsx_data",
#                       password="unEKczDc",
#                       db="glsx_biz_data",
#                       charset='utf8')

import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from matplotlib import font_manager
import matplotlib
from sqlalchemy.orm import sessionmaker

font_path = "C:\\Windows\\Fonts\\simhei.ttf"
my_font = font_manager.FontProperties(fname=font_path, size=15)
matplotlib.rcParams['font.sans-serif'] = ['KaiTi']

# 用sqlalchemy构建数据库链接engine
db_info = {'user': 'biz_glsx_data',
           'password': 'unEKczDc',
           'host': '192.168.5.20',
           'database': 'glsx_biz_data'  # 这里我们事先指定了数据库，后续操作只需要表即可
           }
engine = create_engine('mysql+pymysql://%(user)s:%(password)s@%(host)s/%(database)s?charset=utf8' % db_info,
                       encoding='utf-8')  # 这里直接使用pymysql连接,echo=True，会显示在加载数据库所执行的SQL语句。

# sql 连接
DBsession = sessionmaker(bind=engine)
session = DBsession()


def shebeijihuoqusi():
    sql = f'''
    select DATE_FORMAT(active_time, '%%Y-%%m') as activeTime, count(1) as num
    from dj_2021_settle_details
    where DATE_FORMAT(active_time, '%%Y-%%m') >= '2020-01'
      and DATE_FORMAT(active_time, '%%Y-%%m') <= '2022-12'
    group by DATE_FORMAT(active_time, '%%Y-%%m')
    order by DATE_FORMAT(active_time, '%%Y-%%m');
    '''
    # 读取sql
    data_df = pd.read_sql(sql, engine)
    print(data_df.info())
    print(data_df.head())

    x = data_df['activeTime'].values
    y = data_df['num'].values

    # 设置图片大小
    plt.figure(figsize=(15, 8), dpi=80)
    # 设置标题
    plt.title("设备激活趋势图", fontproperties=my_font)
    # 设置X周与Y周的标题
    plt.xticks(rotation=45)
    plt.xlabel("激活时间(月份)", fontproperties=my_font)
    plt.ylabel("激活数量(个)", fontproperties=my_font)

    # 显示数据标签
    for a, b in zip(x, y):
        plt.text(a, b,
                 b,
                 ha='center',
                 va='bottom',
                 )
    # 绘图
    plt.bar(x, y, color="m", width=0.5)

    plt.savefig('./data/设备激活趋势.png')
    plt.show()
    # 存储
    # data_sql.to_csv("test.csv")


def shebeizaiwangqusi():
    x = []
    y = []
    month_list = get_month_range(datetime.date(2020, 1, 1), datetime.date(2022, 12, 31))
    for month in month_list:
        print(month)
        sql = f'''
        SELECT count(sn) as num from dj_2021_settle_details where active_merchant <> '' and
        ((DATE_FORMAT(active_time, '%Y-%m') = '{month}' and DATE_FORMAT(DATE_ADD(active_time,INTERVAL pk_period MONTH),'%Y-%m') > '{month}') or
        DATE_FORMAT(active_time, '%Y-%m') = '{month}');
        '''
        print(sql)
        result = session.execute(sql)
        data = result.fetchone()[0]
        print(data)
        x.append(month)
        y.append(data)

    print(x)
    print(y)
    # 设置图片大小
    plt.figure(figsize=(15, 8), dpi=80)
    # 设置标题
    plt.title("设备在网趋势图", fontproperties=my_font)
    # 设置X周与Y周的标题
    plt.xticks(rotation=45)
    plt.xlabel("月份", fontproperties=my_font)
    plt.ylabel("数量(个)", fontproperties=my_font)

    # 显示数据标签
    for a, b in zip(x, y):
        plt.text(a, b,
                 b,
                 ha='center',
                 va='bottom',
                 )
    # 绘图
    plt.bar(x, y, color="m", width=0.5)

    plt.savefig('./data/设备在网趋势.png')
    plt.show()
    # 存储
    # data_sql.to_csv("test.csv")


def get_month_range(start_day, end_day):
    months = (end_day.year - start_day.year) * 12 + end_day.month - start_day.month
    month_range = ['%s-%s' % (start_day.year + mon // 12, format_month(mon % 12 + 1))
                   for mon in range(start_day.month - 1, start_day.month + months)]
    return month_range


def format_month(num):
    if num < 10:
        return '0' + str(num)
    else:
        return str(num)


def shebeijihuoqusiduibi(year):
    x = []
    y1 = []
    y2 = []
    y3 = []
    y4 = []
    y5 = []
    # 查询top5的门店
    sql = '''
    select active_merchant as activeMerchant, count(1) as num
    from dj_2021_settle_details
    where DATE_FORMAT(active_time, '%Y-%m') >= '2020-01'
      and DATE_FORMAT(active_time, '%Y-%m') <= '2022-12'
    group by active_merchant
    order by num desc limit 5
    '''
    result = session.execute(sql)
    mendian_list = result.fetchall()
    print(mendian_list)
    x = range(12)
    month_list = get_month_range(datetime.date(year, 1, 1), datetime.date(year, 12, 31))
    for index, month in enumerate(month_list):
        print(index, '------', month)
        # x.append(index + 1)
        for index, mendian in enumerate(mendian_list):
            print(mendian)
            sql01 = f'''
            select count(1) as num
            from dj_2021_settle_details 
            where DATE_FORMAT(active_time, '%Y-%m') = '{month}' and active_merchant = '{mendian[0]}';
            '''
            num_data = session.execute(sql01).fetchone()[0]
            if num_data == 0:
                num_data = 5
            if index == 0:
                y1.append(num_data)
            if index == 1:
                y2.append(num_data)
            if index == 2:
                y3.append(num_data)
            if index == 3:
                y4.append(num_data)
            if index == 4:
                y5.append(num_data)

    print(x)
    print(y1)
    print(y2)
    print(y3)
    print(y4)
    print(y5)
    # 设置图片大小
    plt.figure(figsize=(15, 8), dpi=80)
    # 设置标题
    plt.title("设备激活趋势对比图" + str(year) + '年', fontproperties=my_font)
    # 设置X周与Y周的标题
    # plt.xticks(rotation=45)
    name = ['%d月' % x for x in range(1, 13)]
    plt.xticks(x, name, rotation=45)  # 设置x轴刻度显示值
    plt.xlabel("月份", fontproperties=my_font)
    plt.ylabel("数量(个)", fontproperties=my_font)

    # # 显示数据标签
    # for a, b in zip(x, y1):
    #     plt.text(a, b,
    #              b,
    #              ha='center',
    #              va='bottom',
    #              )
    # for a, b in zip(x, y2):
    #     plt.text(a, b,
    #              b,
    #              ha='center',
    #              va='bottom',
    #              )
    # for a, b in zip(x, y3):
    #     plt.text(a, b,
    #              b,
    #              ha='center',
    #              va='bottom',
    #              )
    # for a, b in zip(x, y4):
    #     plt.text(a, b,
    #              b,
    #              ha='center',
    #              va='bottom',
    #              )
    # for a, b in zip(x, y5):
    #     plt.text(a, b,
    #              b,
    #              ha='center',
    #              va='bottom',
    #              )
    # 绘图
    width = 0.15  # 柱子的宽度
    plt.bar([i - 2 * width for i in x], y1, color="blue", width=width, label=mendian_list[0][0])
    plt.bar([i - 1 * width for i in x], y2, color="green", width=width, label=mendian_list[1][0])
    plt.bar([i + 0 * width for i in x], y3, color="red", width=width, label=mendian_list[2][0])
    plt.bar([i + 1 * width for i in x], y4, color="cyan", width=width, label=mendian_list[3][0])
    plt.bar([i + 2 * width for i in x], y5, color="magenta", width=width, label=mendian_list[4][0])
    plt.legend()
    plt.savefig('./data/设备激活趋势对比' + str(year) + '年.png')
    plt.show()
    # 存储
    # data_sql.to_csv("test.csv")


def shebeizaiwangqusiduibi(year):
    x = []
    y1 = []
    y2 = []
    y3 = []
    y4 = []
    y5 = []
    # 查询top5的门店
    sql = '''
    select active_merchant as activeMerchant, count(1) as num
    from dj_2021_settle_details
    where DATE_FORMAT(active_time, '%Y-%m') >= '2020-01'
      and DATE_FORMAT(active_time, '%Y-%m') <= '2022-12'
    group by active_merchant
    order by num desc limit 5
    '''
    result = session.execute(sql)
    mendian_list = result.fetchall()
    print(mendian_list)
    x = range(12)
    month_list = get_month_range(datetime.date(year, 1, 1), datetime.date(year, 12, 31))
    for index, month in enumerate(month_list):
        print(index, '------', month)
        # x.append(index + 1)
        for index, mendian in enumerate(mendian_list):
            print(mendian)
            sql01 = f'''
            SELECT count(sn) as num from dj_2021_settle_details where active_merchant = '{mendian[0]}' and
            ((DATE_FORMAT(active_time, '%Y-%m') = '{month}' and DATE_FORMAT(DATE_ADD(active_time,INTERVAL pk_period MONTH),'%Y-%m') > '{month}') or
            DATE_FORMAT(active_time, '%Y-%m') = '{month}');
            '''
            num_data = session.execute(sql01).fetchone()[0]
            if num_data == 0:
                num_data = 5
            if index == 0:
                y1.append(num_data)
            if index == 1:
                y2.append(num_data)
            if index == 2:
                y3.append(num_data)
            if index == 3:
                y4.append(num_data)
            if index == 4:
                y5.append(num_data)

    print(x)
    print(y1)
    print(y2)
    print(y3)
    print(y4)
    print(y5)
    # 设置图片大小
    plt.figure(figsize=(15, 8), dpi=80)
    # 设置标题
    plt.title("设备在网趋势对比图" + str(year) + '年', fontproperties=my_font)
    # 设置X周与Y周的标题
    # plt.xticks(rotation=45)
    name = ['%d月' % x for x in range(1, 13)]
    plt.xticks(x, name, rotation=45)  # 设置x轴刻度显示值
    plt.xlabel("月份", fontproperties=my_font)
    plt.ylabel("数量(个)", fontproperties=my_font)

    # # 显示数据标签
    # for a, b in zip(x, y1):
    #     plt.text(a, b,
    #              b,
    #              ha='center',
    #              va='bottom',
    #              )
    # for a, b in zip(x, y2):
    #     plt.text(a, b,
    #              b,
    #              ha='center',
    #              va='bottom',
    #              )
    # for a, b in zip(x, y3):
    #     plt.text(a, b,
    #              b,
    #              ha='center',
    #              va='bottom',
    #              )
    # for a, b in zip(x, y4):
    #     plt.text(a, b,
    #              b,
    #              ha='center',
    #              va='bottom',
    #              )
    # for a, b in zip(x, y5):
    #     plt.text(a, b,
    #              b,
    #              ha='center',
    #              va='bottom',
    #              )
    # 绘图
    width = 0.15  # 柱子的宽度
    plt.bar([i - 2 * width for i in x], y1, color="blue", width=width, label=mendian_list[0][0])
    plt.bar([i - 1 * width for i in x], y2, color="green", width=width, label=mendian_list[1][0])
    plt.bar([i + 0 * width for i in x], y3, color="red", width=width, label=mendian_list[2][0])
    plt.bar([i + 1 * width for i in x], y4, color="cyan", width=width, label=mendian_list[3][0])
    plt.bar([i + 2 * width for i in x], y5, color="magenta", width=width, label=mendian_list[4][0])
    plt.legend()
    plt.savefig('./data/设备在网趋势对比' + str(year) + '年.png')
    plt.show()
    # 存储
    # data_sql.to_csv("test.csv")


if __name__ == '__main__':
    # 门店注册趋势
    shebeijihuoqusi()
    # 设备在网趋势
    shebeizaiwangqusi()
    # 设备激活趋势对比
    shebeijihuoqusiduibi(2020)
    shebeijihuoqusiduibi(2021)
    shebeijihuoqusiduibi(2022)
    # 设备在网趋势对比
    shebeizaiwangqusiduibi(2020)
    shebeizaiwangqusiduibi(2021)
    shebeizaiwangqusiduibi(2022)
