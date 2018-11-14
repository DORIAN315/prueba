import datetime
import re
import numpy as np
import statistics
import sys
from datetime import datetime, timedelta
import calendar

import pymysql

print("gooo")
dbGlobal = pymysql.connect(host="localhost", user="erfectsto27",
                           passwd="ser27bert27", db="unilever_erfectstore_27")


cur = dbGlobal.cursor()

# today = datetime.now()
# if today.month < 10:
#     month = "0%s" % (today.month)
#     year = "%s" % (today.year)
#     day = "%s" % (today.day)
# else:
#     month = "%s" % (today.month)
#     year = "%s" % (today.year)
#     day = "%s" % (today.day)

month = "08"
year = "2018"
day = "20"

if day == '1':
    if month == '01':
        month = '12'
        year1 = int(year) - 1

        if year1:
            year = "%s" % (year1)
    else:
        month1 = int(month) - 1
        if month1 < 10:
            month = "0%s" % (month1)
        else:
            month = "%s" % (month1)

        cursor = dbGlobal.cursor()
        cursor.execute(
            "DELETE FROM `osageneral_ecuador_data` WHERE month` = {0} and  `year` = {1} ".format(month, year))
        dbGlobal.commit()
        cursor.execute(
            "DELETE FROM  `osakpi_ecuador_data` WHERE month` = {0} and  `year` = {1} ".format(month, year))
        dbGlobal.commit()
        cursor.execute(
            "DELETE FROM `osasalepoint_ecuador_data` WHERE month` = {0} and  `year` = {1} ".format(month, year))
        dbGlobal.commit()
        cursor.execute(
            "DELETE FROM `osasku_ecuador_data` WHERE month` = {0} and  `year` = {1} ".format(month, year))
        dbGlobal.commit()
        print('eliminando registros del mes ', month, ' annio ', year)


else:
    cursor = dbGlobal.cursor()
    cursor.execute(
        "DELETE FROM `osageneral_ecuador_data` WHERE month` = {0} and  `year` = {1} ".format(month, year))
    dbGlobal.commit()
    cursor.execute(
        "DELETE FROM  `osakpi_ecuador_data` WHERE month` = {0} and  `year` = {1} ".format(month, year))
    dbGlobal.commit()
    cursor.execute(
        "DELETE FROM `osasalepoint_ecuador_data` WHERE month` = {0} and  `year` = {1} ".format(month, year))
    dbGlobal.commit()
    cursor.execute(
        "DELETE FROM `osasku_ecuador_data` WHERE month` = {0} and  `year` = {1} ".format(month, year))
    dbGlobal.commit()
    print('eliminando registros del mes ', month, ' annio ', year)


cursor = dbGlobal.cursor()
cursor.execute("""select nitpdv,YEAR(polldate) AS year,month(polldate) as month,week(polldate) as week,channel,format,client,category,trademark,supplier,sum(yes),sum(spent),sum(uncoded) from tbls_reportsdata where kpi IN ('PRECIO','INNOVACIONES','SURTIDO','CUMPLIMIENTO PROMOCION') and YEAR(polldate) = {1} and countryname in ('ECUADOR') and month(polldate) = '{0}' GROUP BY nitpdv,week(polldate),channel,format,supplier,client,category,trademark""".format(month,year))
row = cursor.fetchone()
resp_yes = []
while row is not None:
    resp_yes.append({
        'nitpdv': row[0],
        'year': row[1],
        'month': row[2],
        'week': row[3],
        'channel': row[4],
        'format': row[5],
        'client': row[6],
        'category': row[7],
        'trademark': row[8],
        'supplier': row[9],
        'countyes': row[10],
        'countspent': row[11],
        'countuncoded': row[12],
    })
    row = cursor.fetchone()

print("inicio insert t1")
for yes in resp_yes:

    t1 = float(yes['countyes'])
    t2 = float(yes['countspent'])
    t3 = float(yes['countuncoded'])

    total = t1 + t2 + t3
    if total:
        p1 = (t1*100)/total
        p3 = (t3*100)/total
        p2 = (t2*100)/total
    else:
        p1 = 0.0
        p2 = 0.0
        p3 = 0.0

    # print(yes['countryname'],yes['year'],yes['month'],yes['month'],yes['week'],yes['channel'],yes['format'],yes['client'],yes['category'],yes['trademark'],yes['supplier'],t1,p1,t2,p2,t3,p3)

    sql = "INSERT INTO `osageneral_ecuador_data` (`nitpdv`, `year`, `month`, `week`, `channel`, `typology`, `client`, `category`, `make`, `supplier`, `osa`, `countosa`, `spent`, `couentspent`, `uncoded`, `countuncoded`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}',{10},{11},{12},{13},{14},{15})".format(
        yes['nitpdv'], yes['year'], yes['month'], yes['week'], yes['channel'], yes['format'], yes['client'], yes['category'], yes['trademark'], yes['supplier'], t1, p1, t2, p2, t3, p3)
    cur.execute(sql)
    dbGlobal.commit()

print("fin insert t1")
# tabla 2
cursor4 = dbGlobal.cursor()
cursor4.execute(
    """select nitpdv,YEAR(polldate) AS year,month(polldate) as month,week(polldate) as week,channel,format,client,category,kpi,supplier,sum(yes),sum(spent),sum(uncoded) from tbls_reportsdata where kpi IN ('PRECIO','INNOVACIONES','SURTIDO','CUMPLIMIENTO PROMOCION') and YEAR(polldate) = {1} and countryname in ('ECUADOR') and month(polldate) = '{0}' GROUP BY nitpdv,week(polldate),channel,format,supplier,kpi,client,category """.format(month, year))
row4 = cursor4.fetchone()
resp2_yes = []
while row4 is not None:
    resp2_yes.append({
        'nitpdv': row4[0],
        'year': row4[1],
        'month': row4[2],
        'week': row4[3],
        'channel': row4[4],
        'format': row4[5],
        'client': row4[6],
        'category': row4[7],
        'kpi': row4[8],
        'supplier': row4[9],
        'countyes': row4[10],
        'countspent': row4[11],
        'countuncoded': row4[12],
    })
    row4 = cursor4.fetchone()

print("inicio insert t2")
for yes2 in resp2_yes:

    t1 = float(yes2['countyes'])
    t3 = float(yes2['countspent'])
    t2 = float(yes2['countuncoded'])

    total = t1 + t2 + t3
    if total:
        p1 = (t1*100)/total
        p3 = (t3*100)/total
        p2 = (t2*100)/total
    else:
        p1 = 0.0
        p2 = 0.0
        p3 = 0.0

    # print(yes2['countryname'],yes2['year'],yes2['month'],yes2['month'],yes2['week'],yes2['channel'],yes2['format'],yes2['client'],yes2['category'],yes2['kpi'],yes2['supplier'],t1,t2,t3,p1,p2,p3)

    sql = "INSERT INTO `osakpi_ecuador_data` (`nitpdv`, `year`, `month`, `week`, `channel`, `typology`, `client`, `category`, `supplier`, `kpi`, `osa`, `uncoded`, `spent`, `countosa`, `countuncoded`, `countspent`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}',{10},{11},{12},{13},{14},{15})".format(
        yes2['nitpdv'], yes2['year'], yes2['month'],  yes2['week'], yes2['channel'], yes2['format'], yes2['client'], yes2['category'],  yes2['supplier'], yes2['kpi'], t1, t2, t3, p1, p2, p3)
    cur.execute(sql)
    dbGlobal.commit()


print("fin insert t2")
# tabla 3

cursor7 = dbGlobal.cursor()
cursor7.execute(
    """select YEAR(polldate) AS year,month(polldate) as month,week(polldate) as week,channel,format,client,category,salepoint,supplier,nitpdv,sum(yes),sum(spent),sum(uncoded) from tbls_reportsdata where kpi IN ('PRECIO','INNOVACIONES','SURTIDO','CUMPLIMIENTO PROMOCION') and YEAR(polldate) = {1} and countryname in ('ECUADOR') and month(polldate) = '{0}' GROUP BY week(polldate),channel,format,supplier,salepoint,client,category,nitpdv""".format(month, year))
row7 = cursor7.fetchone()
resp3_yes = []
while row7 is not None:
    resp3_yes.append({
        'year': row7[0],
        'month': row7[1],
        'week': row7[2],
        'channel': row7[3],
        'format': row7[4],
        'client': row7[5],
        'category': row7[6],
        'salepoint': row7[7],
        'supplier': row7[8],
        'nitpdv': row7[9],
        'countyes': row7[10],
        'countspent': row7[11],
        'countuncoded': row7[12],
    })
    row7 = cursor7.fetchone()

print("inicio insert t3")

for yes3 in resp3_yes:

    t1 = float(yes3['countyes'])
    t3 = float(yes3['countspent'])
    t2 = float(yes3['countuncoded'])

    total = t1 + t2 + t3
    if total:
        p1 = (t1*100)/total
        p3 = (t3*100)/total
        p2 = (t2*100)/total
    else:
        p1 = 0.0
        p2 = 0.0
        p3 = 0.0

    # print(yes3['countryname'],yes3['year'],yes3['month'],yes3['month'],yes3['week'],yes3['channel'],yes3['format'],yes3['client'],yes3['category'],yes3['salepoint'],yes3['supplier'],yes3['nitpdv'],t1,t2,t3,p1,p2,p3)

    sql = "INSERT INTO `osasalepoint_ecuador_data` (`year`, `month`, `week`, `channel`, `typology`, `client`, `category`, `salepoint`, `supplier`, `codclient`, `osa`, `uncoded`, `spent`, `countosa`, `countuncoded`, `countspent`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}',{10},{11},{12},{13},{14},{15})".format(
        yes3['year'], yes3['month'], yes3['week'], yes3['channel'], yes3['format'], yes3['client'], yes3['category'], yes3['salepoint'], yes3['supplier'], yes3['nitpdv'], t1, t2, t3, p1, p2, p3)
    cur.execute(sql)
    dbGlobal.commit()

print("fin insert t3")

# TABLE 4
cursor10 = dbGlobal.cursor()
cursor10.execute(
    """select YEAR(polldate) AS year,month(polldate) as month,week(polldate) as week,channel,format,client,category,gtin,supplier,nitpdv,product,sum(yes),sum(spent),sum(uncoded) from tbls_reportsdata where kpi IN ('PRECIO','INNOVACIONES','SURTIDO','CUMPLIMIENTO PROMOCION') and YEAR(polldate) = {1} and countryname in ('ECUADOR') and month(polldate) = '{0}' GROUP BY week(polldate),channel,format,supplier,gtin,client,category,nitpdv """.format(month, year))
row10 = cursor10.fetchone()
resp4_yes = []
while row10 is not None:
    resp4_yes.append({
        'year': row10[0],
        'month': row10[1],
        'week': row10[2],
        'channel': row10[3],
        'format': row10[4],
        'client': row10[5],
        'category': row10[6],
        'gtin': row10[7],
        'supplier': row10[8],
        'nitpdv': row10[9],
        'product': row10[10],
        'countyes': row10[11],
        'countspent': row10[12],
        'countuncoded': row10[13],
    })
    row10 = cursor10.fetchone()

print("inicio insert t4")
t1 = 0.0
t2 = 0.0
t3 = 0.0
for yes4 in resp4_yes:

    if yes4['countyes']:
        t1 = float(yes4['countyes'])
    if yes4['countspent']:
        t3 = float(yes4['countspent'])
    if yes4['countuncoded']:
        t2 = float(yes4['countuncoded'])

    total = t1 + t2 + t3
    if total:
        p1 = (t1*100)/total
        p3 = (t3*100)/total
        p2 = (t2*100)/total
    else:
        p1 = 0.0
        p2 = 0.0
        p3 = 0.0

    # print(yes4['countryname'],yes4['year'],yes4['month'],yes4['month'],yes4['week'],yes4['channel'],yes4['format'],yes4['client'],yes4['category'],yes4['gtin'],yes4['supplier'],yes4['nitpdv'],yes4['product'],t1,t2,t3,p1,p2,p3)

    sql = "INSERT INTO `osasku_ecuador_data` (`year`, `month`, `week`, `channel`, `typology`, `client`, `category`, `sku`, `supplier`, `codclient`, `product`, `osa`, `uncoded`, `spent`, `countosa`, `countuncoded`, `countspent`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}',{11},{12},{13},{14},{15},{16})".format(
        yes4['year'], yes4['month'], yes4['week'], yes4['channel'], yes4['format'], yes4['client'], yes4['category'], yes4['gtin'], yes4['supplier'], yes4['nitpdv'], yes4['product'], t1, t2, t3, p1, p2, p3)
    cur.execute(sql)
    dbGlobal.commit()

print("fin insert t4")
