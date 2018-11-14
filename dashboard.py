import datetime
import re
import numpy as np
import statistics
import sys
from datetime import datetime, timedelta
import calendar
import psycopg2
import pymysql

dbGlobal = pymysql.connect(host="localhost", user="erfectsto27", passwd="ser27bert27", db="unilever_erfectstore_27")
dbColombia = pymysql.connect(host="localhost", user="mercadeo11", passwd="merca89uni", db="unilever_erfectstore")
dbPostgres = psycopg2.connect(database="darepslive", user="postgres", password="QU9tvvKM9kL6u5T")
curPostgres = dbPostgres.cursor()

today_end = "%s-%s-%s" % (today.year, today.month, today.day)
today_start = "%s-%s-01" % (today.year, today.month)
today = datetime.now()
if today.month < 10:
    month = "0%s" % (today.month)
    year = "%s" % (today.year)
    day = "%s" % (today.day)
else:
    month = "%s" % (today.month)
    year = "%s" % (today.year)
    day = "%s" % (today.day)


if day == '1':
    if month == '01':
        month = '12'
        year1 = int(year) - 1

        if year1:
            year = "%s" % (year1)

        cursor = dbPostgres.cursor()
        cursor.execute(
            "DELETE FROM dashboard_data WHERE month = '{0}' and  year = '{1}' ".format(month, year))
        print('eliminando registros del mes ', month, ' annio ', year)

    else:
        month1 = int(month) - 1
        if month1 < 10:
            month = "0%s" % (month1)
        else:
            month = "%s" % (month1)

        cursor = dbPostgres.cursor()
        cursor.execute(
            "DELETE FROM dashboard_data WHERE month = '{0}' and  year = '{1}' ".format(month, year))
        print('eliminando registros del mes ', month, ' annio ', year)


else:
    cursor = dbPostgres.cursor()
    cursor.execute(
        "DELETE FROM dashboard_data WHERE month = '{0}' and  year = '{1}' ".format(month, year))
    print('eliminando registros del mes ', month, ' annio ', year)


# # 9 yes/(yes+spent+uncoded)
# cursor = dbGlobal.cursor()
# cursor.execute("""SELECT countryname,sum(yes), sum(spent), sum(uncoded), channel, district FROM tbls_reportsdata WHERE  polldate BETWEEN '{0}' AND '{1}'  AND kpi in ('PRECIO', 'SURTIDO', 'INNOVACIONES', 'CUMPLIMIENTO PROMOCION') AND supplier = 'UNILEVER' GROUP BY district, channel, countryname ORDER BY `tbls_reportsdata`.`district` ASC """.format(
#     today_start, today_end))
# row = cursor.fetchone()
# response = []
# while row is not None:
#     response.append({
#         'countryname': row[0],
#         'si': row[1],
#         'no': row[2],
#         'no_codificado': row[3],
#         'canal': row[4],
#         'distrito': row[5],
        
#     })
#     row = cursor.fetchone()
# for res in response:
#     try:
#         porcentaje = float(res['si'])/(float(res['si']) + float(res['no'])+float(res['no_codificado']))
#     except:
#         porcentaje = 0.0

#     sql = "INSERT INTO dashboard_data (idreport,date_strat,date_end,channel,district,percentage,country) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}')".format( '9', today_start,today_end,  res['canal'], res['distrito'],porcentaje,res['countryname'])
#     curPostgres.execute(sql)
#     dbPostgres.commit()


# #3 
# cursor = dbColombia.cursor()
# cursor.execute("""SELECT channel, count(consulttype), consulttype, district FROM tbls_routescomplience WHERE IF(consulttype=1, chargedate, visitdate) BETWEEN '{0}' AND '{1}' and (reason in (SELECT id FROM tbls_novisitsreason WHERE active=1) OR reason=0) and masterid in (SELECT max(masterid)from tbls_routescomplience WHERE district='ANTIOQUIA' AND IF(consulttype=1, chargedate, visitdate) BETWEEN '{0}' AND '{1}' and (reason in (SELECT id FROM tbls_novisitsreason WHERE active=1) OR reason=0) group by channel, consulttype) group by consulttype, channel, district ORDER BY `tbls_routescomplience`.`channel` ASC""".format(today_start,today_end))
# row = cursor.fetchone()
# response = []
# while row is not None:
#     response.append({
#         'channel': row[0],
#         'counter': row[1],
#         'type': row[2],
#         'district': row[3],
#     })
#     row = cursor.fetchone()

# cursor = dbColombia.cursor()
# cursor.execute("""SELECT DISTINCT(channel) FROM tbls_routescomplience  WHERE IF(consulttype=1, chargedate, visitdate) BETWEEN '{0}' AND '{1}' """.format(
#     today_start, today_end))
# row = cursor.fetchone()
# response1 = []
# while row is not None:
#     response1.append({
#         'channel': row[0],
#     })
#     row = cursor.fetchone()

# cursor = dbColombia.cursor()
# cursor.execute("""SELECT DISTINCT(district) FROM tbls_routescomplience  WHERE IF(consulttype=1, chargedate, visitdate) BETWEEN '{0}' AND '{1}' """.format(
#     today_start, today_end))
# row = cursor.fetchone()
# response2 = []
# while row is not None:
#     response2.append({
#         'district': row[0],
#     })
#     row = cursor.fetchone()


# for ch in response1:
#     for dis in response2:
#         tipo1 = 0.0
#         tipo2 = 0.0
#         tipo3 = 0.0
#         for res in response:
#             if res['district'] == dis['district'] and ch['channel'] == res['channel']:
#                 if str(res['type']) == '1':
#                     tipo1 = float(res['counter'])
#                 if str(res['type']) == '2':
#                     tipo2 = float(res['counter'])
#                 if str(res['type']) == '3':
#                     tipo3 = float(res['counter'])
#         if tipo1 != 0.0:
#             try:
#                 porcentaje = ((tipo2+tipo3)*100)/tipo1
#             except:
#                 porcentaje = 0.0
#             sql = "INSERT INTO dashboard_data (idreport,date_strat,date_end,channel,district,percentage,country) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}')".format(
#                 '3', today_start, today_end,  res['canal'], res['distrito'], porcentaje,'COLOMBIA')
#             curPostgres.execute(sql)
#             dbPostgres.commit()

# #3 cam y ecuador


# cursor = dbGlobal.cursor()
# cursor.execute("""SELECT channel, count(consulttype), consulttype, district, country FROM tbls_routescompliancereportdata WHERE IF(consulttype=1, chargedate, visitdate) BETWEEN '{0}' AND '{1}' and (reason in (SELECT id FROM tbls_novisitsreason WHERE status=1) OR reason=0) group by consulttype, channel, district ORDER BY `tbls_routescompliancereportdata`.`district`, channel ASC""".format(
#     today_start, today_end))
# row = cursor.fetchone()
# response = []
# while row is not None:
#     response.append({
#         'channel': row[0],
#         'counter': row[1],
#         'type': row[2],
#         'district': row[3],
#         'country': row[4],
#     })
#     row = cursor.fetchone()

# cursor = dbGlobal.cursor()
# cursor.execute("""SELECT DISTINCT(channel) FROM tbls_routescompliancereportdata  WHERE IF(consulttype=1, chargedate, visitdate) BETWEEN '{0}' AND '{1}' """.format(
#     today_start, today_end))
# row = cursor.fetchone()
# response1 = []
# while row is not None:
#     response1.append({
#         'channel': row[0],
#     })
#     row = cursor.fetchone()

# cursor = dbGlobal.cursor()
# cursor.execute("""SELECT DISTINCT(district) FROM tbls_routescompliancereportdata  WHERE IF(consulttype=1, chargedate, visitdate) BETWEEN '{0}' AND '{1}' """.format(
#     today_start, today_end))
# row = cursor.fetchone()
# response2 = []
# while row is not None:
#     response2.append({
#         'district': row[0],
#     })
#     row = cursor.fetchone()


# for ch in response1:
#     for dis in response2:
#         tipo1 = 0.0
#         tipo2 = 0.0
#         tipo3 = 0.0
#         canal = ''
#         distrito = ''
#         country = ''
#         for res in response:
#             if res['district'] == dis['district'] and ch['channel'] == res['channel']:
#                 if str(res['type']) == '1':
#                     tipo1 = float(res['counter'])
#                 if str(res['type']) == '2':
#                     tipo2 = float(res['counter'])
#                 if str(res['type']) == '3':
#                     tipo3 = float(res['counter'])
#                 canal = res['canal']
#                 distrito = res['distrito']
#                 country = res['country']
#         if tipo1 != 0.0:
#             try:
#                 porcentaje = ((tipo2+tipo3)*100)/tipo1
#             except:
#                 porcentaje = 0.0
#             sql = "INSERT INTO dashboard_data (idreport,date_strat,date_end,channel,district,percentage,country) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}')".format(
#                 '3', today_start, today_end,  canal, distrito, porcentaje, country)
#             curPostgres.execute(sql)
#             dbPostgres.commit()




# #2 
# cursor = dbGlobal.cursor()
# cursor.execute("""select sum(spacing), district, supplier, channel, category, countryname from (SELECT MAX(CAST(tm.spacing AS SIGNED)) AS spacing, tm.trademark, tm.category, tm.district, tm.countryname, tm.supplier, tm.channel
#     FROM tbls_reportsdata tm
#     WHERE
#     tm.supplier IN('UNILEVER', 'MERCADO')
#     AND tm.spacing != ''
#     AND tm.polldate BETWEEN '{0}' AND '{1}'
#     AND tm.trademark NOT IN('MULTIMARCA', 'MULTICATEGORIA', 'SW', 'FRUCO_DELI', 'DRESSINGS_BASEMAYONESA')
#     GROUP BY tm.trademark, tm.district, tm.channel, tm.countryname ORDER BY category) t1
# group by district, supplier, channel, category, countryname
# """.format(
#     today_start, today_end))
# row = cursor.fetchone()
# response = []
# while row is not None:
#     response.append({
#         'suma': row[0],
#         'district': row[1],
#         'supplier': row[2],
#         'channel': row[3],
#         'category': row[4],
#         'countryname': row[5],
#     })
#     row = cursor.fetchone()

# cursor = dbGlobal.cursor()
# cursor.execute("""SELECT DISTINCT(tm.channel)
#     FROM tbls_reportsdata tm
#     WHERE
#     tm.supplier IN('UNILEVER', 'MERCADO')
#     AND tm.spacing != ''
#     AND tm.polldate BETWEEN '{0}' AND '{1}'
#     AND tm.trademark NOT IN('MULTIMARCA', 'MULTICATEGORIA', 'SW', 'FRUCO_DELI', 'DRESSINGS_BASEMAYONESA')
#     GROUP BY tm.trademark, tm.district, tm.channel, tm.countryname ORDER BY category
# """.format(
#     today_start, today_end))
# row = cursor.fetchone()
# response1 = []
# while row is not None:
#     response1.append({
#         'channel': row[0],
#     })
#     row = cursor.fetchone()

# cursor = dbGlobal.cursor()
# cursor.execute("""SELECT DISTINCT(tm.district)
#     FROM tbls_reportsdata tm
#     WHERE
#     tm.supplier IN('UNILEVER', 'MERCADO')
#     AND tm.spacing != ''
#     AND tm.polldate BETWEEN '{0}' AND '{1}'
#     AND tm.trademark NOT IN('MULTIMARCA', 'MULTICATEGORIA', 'SW', 'FRUCO_DELI', 'DRESSINGS_BASEMAYONESA')
#     GROUP BY tm.trademark, tm.district, tm.channel, tm.countryname ORDER BY category
# """.format(
#     today_start, today_end))
# row = cursor.fetchone()
# response2 = []
# while row is not None:
#     response2.append({
#         'district': row[0],
#     })
#     row = cursor.fetchone()
# cursor = dbGlobal.cursor()
# cursor.execute("""SELECT DISTINCT(tm.category)
#     FROM tbls_reportsdata tm
#     WHERE
#     tm.supplier IN('UNILEVER', 'MERCADO')
#     AND tm.spacing != ''
#     AND tm.polldate BETWEEN '{0}' AND '{1}'
#     AND tm.trademark NOT IN('MULTIMARCA', 'MULTICATEGORIA', 'SW', 'FRUCO_DELI', 'DRESSINGS_BASEMAYONESA')
#     GROUP BY tm.trademark, tm.district, tm.channel, tm.countryname ORDER BY category
# """.format(
#     today_start, today_end))
# row = cursor.fetchone()
# response3 = []
# while row is not None:
#     response3.append({
#         'category': row[0],
#     })
#     row = cursor.fetchone()

# for res3 in response3:
#     for res2 in response2:
#         for res1 in response1:
#             unilever = 0.0
#             mercado = 0.0
#             country = ''
#             for res in response:
#                 if res['district'] == res2['district'] and res['channel'] == res1['channel'] and res['category'] == res3['category']:
#                     if str(res['supplier']) == 'UNILEVER':
#                         unilever = float(res['suma'])
#                     if str(res['supplier']) == 'MERCADO':
#                         mercado = float(res['suma'])
#                     country = res['country']

#             if unilever != 0.0 and mercado != 0.0:
#                 try:
#                     porcentaje = (unilever*100)/mercado
#                 except:
#                     porcentaje = 0.0
#                 sql = "INSERT INTO dashboard_data (idreport,date_strat,date_end,channel,district,percentage,country,category) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}')".format(
#                     '2', today_start, today_end,  res1['channel'], res2['district'], porcentaje, country, res3['category'])
#                 curPostgres.execute(sql)
#                 dbPostgres.commit()


#7


districts = ['ANTIOQUIA', 'COSTA', 'CUNDINAMARCA',
    'EJECENTRO', 'ORIENTE', 'PACIFICO']
for district in districts:
    cursor = dbGlobal.cursor()
    cursor.execute("""SELECT * FROM tbls_reportsdata WHERE district = '{0}' AND polldate
     BETWEEN '{1}' AND '{2}' AND supplier = 'UNILEVER' AND kpi
     IN ('PRECIO','INNOVACIONES') AND price <> '' AND NOT price LIKE '.%' AND NOT price Like '%.'
     AND client <> 'CASH AND CARRY' """.format(district, today_start, today_end))
    responseSKU = cursor.fetchall()

    UniqGLN = list(set([(x['gtin'], x['channel'])
                        for x in responseSKU if x['channel'] != 'FARMACIAS']))
    #print(UniqGLN)

    curPriceList = cursor.execute("""SELECT * FROM tbls_pricelist WHERE country = 'COLOMBIA' AND
     provider = 'UNILEVER' AND status = 1""")
    responsePriceList = curPriceList.fetchall()

    percMT = []
    percTT = []
    for gln, channel in UniqGLN:
        if channel == 'TT':
            med = statistics.median([float(y['price']) for y in responseSKU if y['gtin'] == str(
                gln) and (y['channel'] == channel or y['channel'] == 'FARMACIAS')])
        else:
            med = statistics.median([float(y['price']) for y in responseSKU if y['gtin'] == str(
                gln) and y['channel'] == channel])

        formatos = list(set([x['client'] for x in responsePriceList if x['ean'] == str(
            gln) and x['channel'] == channel]))

        cont1MT = 0
        cont0MT = 0
        cont1TT = 0
        cont0TT = 0
        for formato in formatos:
            #             print(formato)
            minimum = float([x['min'] for x in responsePriceList if x['client']
                             == formato and x['ean'] == str(gln)][0])
#             print(minimum)
            maximum = float([x['max'] for x in responsePriceList if x['client']
                             == formato and x['ean'] == str(gln)][0])
#             print(maximum)
            if minimum <= med <= maximum:
                if channel == 'MT':
                    cont1MT += 1
                else:
                    cont1TT += 1
            else:
                if channel == 'MT':
                    cont0MT += 1
                else:
                    cont0TT += 1
        try:
            skuPercMT = cont1MT/(cont1MT + cont0MT)
            percMT.append(skuPercMT)
        except:
            pass

        try:
            skuPercTT = cont1TT/(cont1TT + cont0TT)
            percTT.append(skuPercTT)
        except:
            pass

    try:
        avgindexMT = round(sum(percMT)/len(percMT)*100)
    except:
        avgindexMT = 0
    try:
        avgindexTT = round(sum(percTT)/len(percTT)*100)
    except:
        avgindexTT = 0


    sql = "INSERT INTO dashboard_data (idreport,date_strat,date_end,channel,district,percentage,country) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}')".format(
        '7', today_start, today_end,  'MT', district, avgindexMT, 'COLOMBIA')
    curPostgres.execute(sql)
    dbPostgres.commit()
    sql = "INSERT INTO dashboard_data (idreport,date_strat,date_end,channel,district,percentage,country) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}')".format(
        '7', today_start, today_end,  'TT', district, avgindexTT, 'COLOMBIA')
    curPostgres.execute(sql)
    dbPostgres.commit()
    
     
