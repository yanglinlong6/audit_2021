# -*- coding:utf-8 -*-
from __future__ import print_function
import time
import MySQLdb
import sys


# reload(sys)
# sys.setdefaultencoding('utf8')


#
def paddstr(strx):
    return "NULL" if strx == None else "\'" + MySQLdb.escape_string(str(strx)).decode() + "\'"


def paddnum(numx, default='NULL'):
    return default if numx == None else str(numx)


class MySQL:
    u'''对MySQLdb常用函数进行封装的类'''

    error_code = ''  # MySQL错误号码

    _instance = None  # 本类的实例
    _conn = None  # 数据库conn
    _cur = None  # 游标

    _TIMEOUT = 30  # 默认超时30秒
    _timecount = 0

    def __init__(self, dbconfig):
        u'构造器：根据数据库连接参数，创建MySQL连接'
        try:
            self._conn = MySQLdb.connect(host=dbconfig['host'],
                                         port=dbconfig['port'],
                                         user=dbconfig['user'],
                                         passwd=dbconfig['passwd'],
                                         db=dbconfig['db'],
                                         charset=dbconfig['charset'])
        except MySQLdb.Error as e:
            self.error_code = e.args[0]
            error_msg = 'MySQL error! ', e.args[0], e.args[1]
            print(error_msg)

            # 如果没有超过预设超时时间，则再次尝试连接，
            if self._timecount < self._TIMEOUT:
                interval = 5
                self._timecount += interval
                time.sleep(interval)
                return self.__init__(dbconfig)
            else:
                raise Exception(error_msg)

        self._cur = self._conn.cursor()
        self._instance = MySQLdb

    def query(self, sql):
        u'执行 SELECT 语句'
        try:
            # self._cur.execute("SET NAMES utf8")
            result = self._cur.execute(sql)
            result = True
        except MySQLdb.Error as e:
            self.error_code = e.args[0]
            print("数据库错误代码:", e.args[0], e.args[1])
            result = False
        return result

    def update(self, sql):
        u'执行 UPDATE 及 DELETE 语句'
        try:
            # self._cur.execute("SET NAMES utf8")
            result = self._cur.execute(sql)
            self._conn.commit()
            result = True
        except MySQLdb.Error as e:
            self.error_code = e.args[0]
            print("数据库错误代码:", e.args[0], e.args[1])
            result = False
        return result

    def insert(self, sql, data=None):
        u'执行 INSERT 语句。如主键为自增长int，则返回新生成的ID'
        try:
            self._cur.execute("SET NAMES utf8")
            if data == None:
                self._cur.execute(sql)
            else:
                self._cur.executemany(sql, data)
            self._conn.commit()
            return True
            return self._conn.insert_id()
        except MySQLdb.Error as e:
            self.error_code = e.args[0]
            return False

    def callproc(self, procname, *arg):
        try:
            self._cur.execute("SET NAMES utf8")
            self._cur.callproc(procname, arg)
            self._conn.commit()
            result = True
        except MySQLdb.Error as e:
            self.error_code = e.args[0]
            print("数据库错误代码:", e.args[0], e.args[1])
            result = False
        return result

    def fetchAllRows(self):
        u'返回结果列表'
        return self._cur.fetchall()

    def fetchOneRow(self):
        u'返回一行结果，然后游标指向下一行。到达最后一行以后，返回None'
        return self._cur.fetchone()

    def getRowCount(self):
        u'获取结果行数'
        return self._cur.rowcount

    def commit(self):
        u'数据库commit操作'
        self._conn.commit()

    def rollback(self):
        u'数据库回滚操作'
        self._conn.rollback()

    def __del__(self):
        u'释放资源（系统GC自动调用）'
        try:
            self._cur.close()
            self._conn.close()
        except:
            pass

    def close(self):
        u'关闭数据库连接'
        self.__del__()
