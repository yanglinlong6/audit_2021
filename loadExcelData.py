import pandas as pd
import pymysql

g_pro_biz_dbcfg = {
    "host": "192.168.5.2",
    "port": 3306,
    "user": "biz_glsx_data",
    "passwd": "unEKczDc",
    "db": "glsx_biz_data",
    "charset": "utf8",
}


def loadFinnanceExcelToMysql(fileName, sheetName):
    # tableName = "dj_2022_settlment_order_finance_06plus"
    tableName = "dj_2023_settlment_order_finance"

    """将Excel表数据导入mysql"""
    conn_updata = pymysql.Connect(
        host="192.168.5.2",
        port=3306,
        user="biz_glsx_data",
        passwd="unEKczDc",
        db="glsx_biz_data",
        charset="utf8",
    )

    # pd.set_option('display.max_columns', 40)
    data = pd.read_excel(fileName, sheet_name=sheetName, header=0)
    newData = data[
        [
            "单据类型",
            "单据编号",
            "业务日期",
            "客户",
            "渠道",
            "物料编码",
            "物料名称",
            "产品类型",
            "设备类型",
            "计价数量",
            "含税单价",
            "单价",
            "收入",
            "业务类型",
            "分摊期限",
        ]
    ]
    """
    newData.rename(columns={'单据类型': 'order_type', '单据编号':'order_no', '业务日期':'finance_time',
                                      '客户':'cus_name', '渠道':'sale_type', '物料编码':'product_code',
                            '物料名称':'product_name', '产品类型':'product_type', '设备类型':'device_type',
                            '计价数量':'total', '含税单价':'hw_unit_price_tax', '单价':'hw_unit_price',
                            '收入':'income', '业务类型':'business_type', '分摊期限':'hw_term'})
    """
    newData["含税单价"].fillna(0, inplace=True)
    newData["单价"].fillna(0, inplace=True)
    newData["收入"].fillna(0, inplace=True)
    newData["分摊期限"].fillna(0, inplace=True)
    newData["设备类型"].fillna("", inplace=True)
    newData["产品类型"].fillna("", inplace=True)
    newData["计价数量"].fillna(0, inplace=True)
    newData["业务类型"].fillna("", inplace=True)

    print(newData.head())
    valueArr = []
    for idx in newData.index:
        hwPriceTax = newData.loc[idx, "含税单价"]
        hwPrice = newData.loc[idx, "单价"]
        value = (
            newData.loc[idx, "单据类型"],
            newData.loc[idx, "单据编号"],
            newData.loc[idx, "业务日期"],
            newData.loc[idx, "客户"],
            newData.loc[idx, "渠道"],
            newData.loc[idx, "物料编码"],
            newData.loc[idx, "物料名称"],
            newData.loc[idx, "产品类型"],
            newData.loc[idx, "设备类型"],
            newData.loc[idx, "计价数量"],
            hwPriceTax,
            hwPrice,
            newData.loc[idx, "收入"],
            newData.loc[idx, "业务类型"],
            newData.loc[idx, "分摊期限"],
        )
        # print(value)
        valueArr.append(value)

    cs = conn_updata.cursor()
    sql = (
        "insert into "
        + tableName
        + "(order_type,order_no,finance_time,cus_name,sale_type,product_code,"
        "product_name,product_type,device_type,total,hw_unit_price_tax,"
        "hw_unit_price,income,business_type,hw_term) values "
        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    cs.executemany(sql, valueArr)
    conn_updata.commit()
    cs.close()


def loadDjExcelToMysql(fileName, sheetName):
    # tableName = "dj_2022_settlment_order_06plus"
    tableName = "dj_2023_settlment_order"
    """将Excel表数据导入mysql"""
    conn_updata = pymysql.Connect(
        host="192.168.5.2",
        port=3306,
        user="biz_glsx_data",
        passwd="unEKczDc",
        db="glsx_biz_data",
        charset="utf8",
    )
    data = pd.read_excel(fileName, sheet_name=sheetName, header=0)
    newData = data[
        [
            "单据类型",
            "单据编号",
            "业务日期",
            "客户",
            "渠道",
            "物料编码",
            "物料名称",
            "产品类型",
            "设备类型",
            "计价数量",
            "含税单价",
            "单价",
            "硬件（总）（不含税）",
            "硬件税率",
            "服务（不含税）",
            "服务税率",
            "服务套餐",
            "收入",
            "业务类型",
            "分摊期限",
            "安装",
            "安装税率",
        ]
    ]
    newData["含税单价"].fillna(0, inplace=True)
    newData["单价"].fillna(0, inplace=True)
    newData["收入"].fillna(0, inplace=True)
    newData["分摊期限"].fillna(0, inplace=True)
    newData["硬件（总）（不含税）"].fillna(0, inplace=True)
    newData["硬件税率"].fillna(0, inplace=True)
    newData["服务（不含税）"].fillna(0, inplace=True)
    newData["服务税率"].fillna(0, inplace=True)
    newData["服务套餐"].fillna("", inplace=True)
    newData["产品类型"].fillna("", inplace=True)
    newData["设备类型"].fillna("", inplace=True)
    newData["安装"].fillna(0, inplace=True)
    newData["安装税率"].fillna(0, inplace=True)

    # print(newData)
    valueArr = []
    for idx in newData.index:
        hwPriceTax = newData.loc[idx, "含税单价"]
        hwPrice = newData.loc[idx, "单价"]
        newHwPrice = newData.loc[idx, "硬件（总）（不含税）"]
        newHwTaxRate = newData.loc[idx, "硬件税率"]
        if newHwPrice != 0:
            hwPrice = newHwPrice
            hwPriceTax = newHwTaxRate
        pkPrice = newData.loc[idx, "服务（不含税）"]
        pkPriceRate = newData.loc[idx, "服务税率"]
        client = newData.loc[idx, "客户"].strip()
        value = (
            newData.loc[idx, "单据类型"],
            newData.loc[idx, "单据编号"],
            newData.loc[idx, "业务日期"],
            client,
            newData.loc[idx, "渠道"],
            newData.loc[idx, "物料编码"],
            newData.loc[idx, "物料名称"],
            newData.loc[idx, "产品类型"],
            newData.loc[idx, "设备类型"],
            newData.loc[idx, "计价数量"],
            hwPriceTax,
            hwPrice,
            pkPrice,
            pkPriceRate,
            newData.loc[idx, "服务套餐"],
            newData.loc[idx, "收入"],
            newData.loc[idx, "业务类型"],
            newData.loc[idx, "分摊期限"],
            newData.loc[idx, "分摊期限"],
            newData.loc[idx, "安装"],
            newData.loc[idx, "安装税率"],
        )
        print(value)

        valueArr.append(value)

    cs = conn_updata.cursor()
    sql = (
        "insert into "
        + tableName
        + "(order_type,order_no,finance_time,cus_name,sale_type,product_code,"
        "product_name,product_type,device_type,total,hw_unit_price_tax,"
        "hw_unit_price,pk_unit_price,pk_unit_price_tax,pk_name,income,"
        "business_type,hw_term,pk_term,install_unit_price,install_unit_price_tax) "
        "values "
        "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    cs.executemany(sql, valueArr)
    conn_updata.commit()


def updateCustomerChannel():
    """更新客户的渠道"""
    dbConn = pymysql.Connect(
        host="192.168.5.2",
        port=3306,
        user="biz_glsx_data",
        passwd="unEKczDc",
        db="glsx_biz_data",
        charset="utf8",
    )
    querySql = "SELECT cus_name,sale_type,count(1) from dj_2022_settlment_order_finance  group by cus_name,sale_type"
    dbCursor = dbConn.cursor()
    dbCursor.execute(querySql)
    result = dbCursor.fetchall()
    cusNameDict = {}
    for v in result:
        cusName = v[0]
        saleType = v[1]
        cusNameDict[cusName] = saleType
    querySql = "SELECT cus_name,count(1) from dj_2022_settlment_order where finance_time < '2022-02-01' group by cus_name"
    dbCursor = dbConn.cursor()
    dbCursor.execute(querySql)
    result = dbCursor.fetchall()

    for v in result:
        newCusName = v[0]
        newSaleType = cusNameDict.get(newCusName, None)
        if newSaleType is None:
            print(f"can not find cusname={newCusName}")
            continue

        updateSql = f"update dj_2022_settlment_order set sale_type='{newSaleType}' where finance_time < '2022-02-01' and cus_name='{newCusName}'"
        dbCursor = dbConn.cursor()
        dbCursor.execute(updateSql)
    dbConn.commit()
    print("done")


if __name__ == "__main__":
    # 财务原表

    filePath = "G:\\财务审计\\EXCEL文档\\应收拆分数据3月份\\"

    # fileName = filePath + "数据源-激活调整用23年2-3月.xlsx"
    # sheetName = "Sheet1"
    # loadFinnanceExcelToMysql(fileName, sheetName)

    # E盾
    # fileName = filePath + "4.数据源-激活调整用23年3月-陈冰恋.xlsx"
    # sheetName = "3.虎哥E盾--渠道应收单原表"
    # loadDjExcelToMysql(fileName, sheetName)
    # # 渠道嘀加
    # fileName = filePath + "2.拆分数据-渠道2023年3月嘀加数据-柳云飞.xlsx"
    # sheetName = "Sheet2"
    # loadDjExcelToMysql(fileName, sheetName)
    # # # 直销
    fileName = filePath + "1.数据源-激活调整用2023年3月-廖敏玲.xlsx"
    sheetName = "廖敏玲"
    loadDjExcelToMysql(fileName, sheetName)

    print("done")
