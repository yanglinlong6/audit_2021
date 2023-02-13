import datetime

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


def mendianzhucequsi():
    sql = '''
    select DATE_FORMAT(created_date, '%%Y-%%m') as createdDate, count(1) as num
    from dj_system_merchant
    where DATE_FORMAT(created_date, '%%Y-%%m') >= '2020-01'
       and DATE_FORMAT(created_date, '%%Y-%%m') <= '2022-12'
       and deleted_flag = 'N'
    group by DATE_FORMAT(created_date, '%%Y-%%m')
    order by DATE_FORMAT(created_date, '%%Y-%%m') ;
    '''
    # 读取sql
    data_df = pd.read_sql(sql, engine)
    print(data_df.info())
    print(data_df.head())

    x = data_df['createdDate'].values
    y = data_df['num'].values

    # 设置图片大小
    plt.figure(figsize=(15, 8), dpi=80)
    # 设置标题
    plt.title("门店注册趋势图", fontproperties=my_font)
    # 设置X周与Y周的标题
    plt.xticks(rotation=30)
    plt.xlabel("创建时间(月份)", fontproperties=my_font)
    plt.ylabel("注册数量(个)", fontproperties=my_font)

    # 显示数据标签
    for a, b in zip(x, y):
        plt.text(a, b,
                 b,
                 ha='center',
                 va='bottom',
                 )
    # 绘图
    plt.bar(x, y, color="m", width=0.5)

    plt.savefig('./data/门店注册趋势.png')
    plt.show()
    # 存储
    # data_sql.to_csv("test.csv")


def mendianquyufenbu():
    sql = '''
    select province_name as provinceName, count(1) as num
    from dj_system_merchant
    where DATE_FORMAT(created_date, '%%Y-%%m') >= '2018-01' and province_name is not null and deleted_flag = 'N'
    group by province_name
    order by num desc;
    '''
    # 读取sql
    data_df = pd.read_sql(sql, engine)
    print(data_df.info())
    print(data_df.head())

    x = data_df['provinceName'].values
    print(x)
    y = data_df['num'].values

    # 设置图片大小
    plt.figure(figsize=(15, 8), dpi=80)
    # 设置标题
    plt.title("门店区域分布图", fontproperties=my_font)
    # 设置X周与Y周的标题
    # plt.xticks(rotation=30)
    plt.xlabel("省份区域", fontproperties=my_font)
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

    plt.savefig('./data/门店区域分布.png')
    plt.show()
    # 存储
    # data_sql.to_csv("test.csv")


def mendianhuoyuequsi():
    x = []
    y = []
    month_list = get_month_range(datetime.date(2020, 1, 1), datetime.date(2022, 12, 31))
    for month in month_list:
        print(month)
        sql = f'''
        SELECT count(DISTINCT active_merchant) as num from dj_2021_settle_details where active_merchant <> '' and
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
    plt.title("门店活跃趋势图", fontproperties=my_font)
    # 设置X周与Y周的标题
    plt.xticks(rotation=30)
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

    plt.savefig('./data/门店活跃趋势.png')
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


if __name__ == '__main__':
    # 门店注册趋势
    mendianzhucequsi()
    # 门店区域分布
    mendianquyufenbu()
    # 门店活跃趋势
    mendianhuoyuequsi()
