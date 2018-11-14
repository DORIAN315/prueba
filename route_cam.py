########### route

import urllib
import datetime
import re
import numpy as np
import statistics
import sys
from datetime import datetime, timedelta
import calendar
import pymysql
# import psycopg2

# db2 = psycopg2.connect(host="localhost",database="darepslive", user="postgres", password="Vh8Z6jjC757U72k9" ,sslmode='require')
dbGlobal = pymysql.connect(host="localhost", user="erfectsto27",
                           passwd="ser27bert27", db="unilever_erfectstore_27")
# dbColombia = pymysql.connect(host="localhost", user="mercadeo11", passwd="merca89uni", db="unilever_erfectstore")
cur = dbGlobal.cursor()

today = datetime.now()
if today.month < 10:
    month = "0%s" % (today.month)
    year = "%s" % (today.year)
    day = "%s" % (today.day)
else:
    month = "%s" % (today.month)
    year = "%s" % (today.year)
    day = "%s" % (today.day)

# month = '09'
# year = '2018'
# day = '18'
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
            "DELETE FROM `routescompliance_cam_data` WHERE mes = {0} and  `annio` = {1} ".format(month, year))
        dbGlobal.commit()
        cursor.execute(
            "DELETE FROM  `routescompliance_reason_cam_data` WHERE mes = {0} and  `annio` = {1} ".format(month, year))
        dbGlobal.commit()
        cursor.execute(
            "DELETE FROM `routescompliance_detail_cam_data` WHERE mes = {0} and  `annio` = {1} ".format(month, year))
        dbGlobal.commit()
        print('eliminando registros del mes ', month, ' annio ', year)


else:
    cursor = dbGlobal.cursor()
    cursor.execute(
        "DELETE FROM `routescompliance_cam_data` WHERE mes = {0} and  `annio` = {1} ".format(month, year))
    dbGlobal.commit()
    cursor.execute(
        "DELETE FROM  `routescompliance_reason_cam_data` WHERE mes = {0} and  `annio` = {1} ".format(month, year))
    dbGlobal.commit()
    cursor.execute(
        "DELETE FROM `routescompliance_detail_cam_data` WHERE mes = {0} and  `annio` = {1} ".format(month, year))
    dbGlobal.commit()
    print('eliminando registros del mes ', month, ' annio ', year)

print('ingresando  registros del mes ', month, ' annio ', year)
cursor = dbGlobal.cursor()
sql = """SELECT userid, username, gln, salespoint, channel, district, reason, reasondescription, visitdate, visittime, format, chargedate, chargetime, device, consulttype, visityear, visitmonth, MONTH(chargedate) as mes, YEAR(chargedate) as annio, country FROM `tbls_routescompliancereportdata` where consulttype = 1 and month(chargedate) = {0} and year(chargedate) = {1} AND  country not in ('ECUADOR','COLOMBIA')  AND chargedate = (SELECT MAX(chargedate) FROM `tbls_routescompliancereportdata` where consulttype = 1 and month(chargedate) = {0} and year(chargedate) = 2018 AND country not in ('ECUADOR','COLOMBIA') ) AND MONTH(visitbyrutero) = {0}
    group by userid ,format """.format(month, year)
print(sql)
cursor.execute(sql)
row = cursor.fetchone()

response = []
while row is not None:
    response.append({
        'userid': row[0],
        'username': row[1],
        'gln': row[2],
        'salespoint': row[3],
        'channel': row[4],
        'district': row[5],
        'reason': row[6],
        'reasondescription': row[7],
        'visitdate': row[8],
        'visittime': row[9],
        'format': row[10],
        'chargedate': row[11],
        'chargetime': row[12],
        'device': row[13],
        'consulttype': row[14],
        'visityear': row[15],
        'visitmonth': row[16],
        'mes': row[17],
        'annio': row[18],
        'country': row[19],

    })
    row = cursor.fetchone()

cursor2 = dbGlobal.cursor()
cursor2.execute(
    """SELECT userid,format,country, count(username) as visitreals FROM `tbls_routescompliancereportdata` where consulttype = 1 and month(chargedate) = {0} and year(chargedate) = {1} AND  country not in ('ECUADOR','COLOMBIA')and chargedate = (SELECT max(chargedate) FROM `tbls_routescompliancereportdata` where consulttype = 1 and month(chargedate) = {0} and year(chargedate) = {1} AND  country not in ('ECUADOR','COLOMBIA')) AND MONTH(visitbyrutero) = {0} group by userid,format""".format(month, year))
row2 = cursor2.fetchone()
response2 = []
while row2 is not None:
    response2.append({
        'userid': row2[0],
        'format': row2[1],
        'country': row2[2],
        'visitreals': row2[3],

    })
    row2 = cursor2.fetchone()
# 4 consulta

cursor3 = dbGlobal.cursor()
cursor3.execute(
    """SELECT userid,count(userid) as visit,format,country FROM `tbls_routescompliancereportdata` where consulttype = 2 and month(visitdate) = {0} and year(visitdate) = {1} AND  country not in ('ECUADOR','COLOMBIA') and chargedate = (SELECT max(chargedate) FROM `tbls_routescompliancereportdata` where consulttype = 2 and month(visitdate) = {0} and year(visitdate) = {1} AND  country not in ('ECUADOR','COLOMBIA')) AND MONTH(visitbyrutero) = {0} group by userid,format""".format(month, year))
row3 = cursor3.fetchone()
response3 = []
while row3 is not None:
    response3.append({
        'userid': row3[0],
        'visit': row3[1],
        'format': row3[2],
        'country': row3[3],
    })
    row3 = cursor3.fetchone()

# 5 consulta

cursor4 = dbGlobal.cursor()
cursor4.execute(
    """ SELECT * FROM `tbls_novisitsreason` where  country not in ('ECUADOR','COLOMBIA') """)
row4 = cursor4.fetchone()
response4 = []
while row4 is not None:
    response4.append({
        'id': row4[1],
        'description': row4[2],
        'active': row4[3],
        'country': row4[4],

    })
    row4 = cursor4.fetchone()

# 5 consulta

cursor5 = dbGlobal.cursor()
cursor5.execute(
    """SELECT userid,reason,format,country FROM `tbls_routescompliancereportdata` where consulttype = 3 and month(visitdate) = {0} and year(visitdate) ={1} AND  country not in ('ECUADOR','COLOMBIA')  and chargedate = (SELECT max(chargedate) FROM `tbls_routescompliancereportdata` where consulttype = 3 and month(visitdate) = {0} and year(visitdate) ={1} AND  country not in ('ECUADOR','COLOMBIA') ) AND MONTH(visitbyrutero) = {0}""".format(month, year))
row5 = cursor5.fetchone()
response5 = []
while row5 is not None:
    response5.append({
        'userid': row5[0],
        'reason': row5[1],
        'format': row5[2],
        'country': row5[3],
    })
    row5 = cursor5.fetchone()


# ####################################################
# #  raoutescompliancedata
# ####################################################
noAfectan = 0
Afectan = 0
for data in response:
    for data2 in response2:
        # print(" ok ")
        if data['userid'] == data2['userid'] and data['format'] == data2['format'] and data['country'] == data2['country']:
            noAfectan = 0
            Afectan = 0
            total = 0
            porcentaje = 0
            visit = ''

            for data3 in response3:

                print("hola")
                if data['userid'] == data3['userid'] and data['format'] == data3['format'] and data['country'] == data3['country']:

                    for data4 in response5:
                        if data['userid'] == data4['userid'] and data['format'] == data4['format'] and data['country'] == data4['country']:
                            for noVisita in response4:
                                if data4['reason'] == noVisita['id'] and noVisita['country'] == data4['country']:
                                    if noVisita['active'] == 1:
                                        noAfectan += 1
                                    else:
                                        Afectan += 1

                    subt = float(noAfectan) + float(data3['visit'])
                    total = subt*100/float(data2['visitreals'])
                    porcentaje = float(data3['visit']) * \
                        100/float(data2['visitreals'])
                    visit = data3['visit']
                    # print(data['channel'],data['userid'],data['username'],data['district'],data2['visitreals'],data3['visit'],porcentaje,noAfectan,Afectan,total,data['mes'],data['annio'])
            print("INSERTANDO TABLA 1")
            sql = "INSERT INTO `routescompliance_cam_data` ( `canal`, `cedula`, `nombre`, `distrito`, `formato`,`ciudad` ,`vis_programadas`, `vis_reales`, `porc_cumple`, `novisita_afectan`, `novisita_no_afecta`, `porc_cumplimiento`, `mes`, `annio`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}',{6},'{7}',{8},{9},{10},{11},'{12}','{13}')".format(
                data['channel'], data['userid'], data['username'], data['district'], data['format'], data['country'], data2['visitreals'], visit, porcentaje, noAfectan, Afectan, total, data['mes'], data['annio'])
            cur.execute(sql)
            dbGlobal.commit()
            noAfectan = 0
            Afectan = 0


# ###################################################
# #   routescompliancereasondata
# ###################################################
print("termina fase 1")
cursor6 = dbGlobal.cursor()
cursor6.execute("""SELECT id,
                        canal,
                        cedula,
                        nombre,
                        distrito,
                        vis_programadas,
                        vis_reales,
                        porc_cumple,
                        novisita_afectan,
                        novisita_no_afecta,
                        porc_cumplimiento,
                        mes,
                        annio,
                        formato,
                        ciudad
                        FROM `routescompliance_cam_data` """)
row6 = cursor6.fetchone()
resumen = []
while row6 is not None:
    resumen.append({
        'id': row6[0],
        'canal': row6[1],
        'userid': row6[2],
        'nombre': row6[3],
        'distrito': row6[4],
        'vis_programadas': row6[5],
        'visitas': row6[6],
        'porc_cumple': row6[7],
        'afectan': row6[8],
        'noafectan': row6[9],
        'porc_cumplimiento': row6[10],
        'mes': row6[11],
        'annio': row6[12],
        'formato': row6[13],
        'ciudad': row6[14],
    })
    row6 = cursor6.fetchone()

cursor7 = dbGlobal.cursor()
cursor7.execute(
    """SELECT userid,username,reason,count(reason) as num_reason, MONTH(visitdate) as mes ,YEAR(visitdate) as annio,format,country FROM `tbls_routescompliancereportdata` where consulttype = 3 and month(visitdate) = {0} and year(visitdate) = {1} AND  country not in ('ECUADOR','COLOMBIA') and chargedate = (SELECT max(chargedate) FROM `tbls_routescompliancereportdata` where consulttype = 3 and month(visitdate) = {0} and year(visitdate) = {1} AND  country not in ('ECUADOR','COLOMBIA')) group by reason,userid,format ORDER BY `tbls_routescompliancereportdata`.`userid` ASC """.format(month, year))
row7 = cursor7.fetchone()
counter = []
while row7 is not None:
    counter.append({
        'userid': row7[0],
        'username': row7[1],
        'reason': row7[2],
        'num_reason': row7[3],
        'mes': row7[4],
        'annio': row7[5],
        'format': row7[6],
        'ciudad': row7[7],
    })
    row7 = cursor7.fetchone()


for data in resumen:
    for counters in counter:
        if data['userid'] == counters['userid'] and data['mes'] == str(counters['mes']) and data['annio'] == str(counters['annio']) and data['formato'] == counters['format'] and data['ciudad'] == counters['ciudad']:
            print("paso")
            for x in response4:
                if counters['reason'] == x['id'] and counters['ciudad'] == x['country']:
                    if x['active'] == 0:
                        print("INSERTANDO TABLA 2")
                        sql = "INSERT INTO `routescompliance_reason_cam_data` ( `id_routes_compliance_data`, `cedula`,`formato`,`ciudad`, `id_causal`, `total_causal`, `mes`, `annio`, `afecta_cumplimiento`) VALUES ({0},'{1}','{2}','{3}',{4},'{5}','{6}',{7},{8})".format(
                            data['id'], data['userid'], data['formato'], data['ciudad'], counters['reason'], counters['num_reason'], counters['mes'], counters['annio'], 0)
                        cur.execute(sql)
                        dbGlobal.commit()
                        # print(data['id'],data['userid'],counters['reason'],counters['num_reason'],counters['annio'],counters['mes'],'0')
                    else:
                        print("INSERTANDO TABLA 2")
                        sql = "INSERT INTO `routescompliance_reason_cam_data` ( `id_routes_compliance_data`, `cedula`,`formato`,`ciudad`, `id_causal`, `total_causal`, `mes`, `annio`, `afecta_cumplimiento`) VALUES ({0},'{1}','{2}','{3}',{4},'{5}','{6}',{7},{8})".format(
                            data['id'], data['userid'], data['formato'], data['ciudad'], counters['reason'], counters['num_reason'], counters['mes'], counters['annio'], 1)
                        cur.execute(sql)
                        dbGlobal.commit()
                        print(data['id'], data['userid'], counters['reason'],
                              counters['num_reason'], counters['annio'], counters['mes'], '1')


# ################################
# #  routescompliancedetaildata
# ###############################
# print("termina fase 2")


cursor9 = dbGlobal.cursor()
cursor9.execute(""" SELECT intervalDescription,starttime,endtime,country,state FROM `tbls_workinghours` where active = 1 AND  country not in ('ECUADOR','COLOMBIA') """)
row9 = cursor9.fetchone()
rango = []
while row9 is not None:
    rango.append({
        'descripcion': row9[0],
        'horainicio': row9[1],
        'horafin': row9[2],
        'country': row9[3],
        'state': row9[4],
    })
    row9 = cursor9.fetchone()


print("iniciando")
cursor8 = dbGlobal.cursor()
cursor8.execute("""SELECT   userid,
                        username,
                        gln,
                        salespoint,
                        channel,
                        district,
                        reason,
                        reasondescription,
                        visitdate,
                        visittime,
                        format,
                        chargedate,
                        chargetime,
                        device,
                        consulttype,
                        visityear,
                        visitdate,
                        MONTH(visitdate)  as mes,
                        YEAR(visitdate) as annio,
                        WEEK(visitdate, 5) - WEEK(DATE_SUB(visitdate, INTERVAL DAYOFMONTH(visitdate) - 1 DAY), 5) + 1,
                        country
                        FROM `tbls_routescompliancereportdata` where month(visitdate) = {0} and year(visitdate) = {1} AND  country not in ('ECUADOR','COLOMBIA') """.format(month, year))
row8 = cursor8.fetchone()
routes = []
while row8 is not None:
    routes.append({
        'userid': row8[0],
        'username': row8[1],
        'gln': row8[2],
        'salespoint': row8[3],
        'channel': row8[4],
        'district': row8[5],
        'reason': row8[6],
        'reasondescription': row8[7],
        'visitdate': row8[8],
        'visittime': row8[9],
        'format': row8[10],
        'chargedate': row8[11],
        'chargetime': row8[12],
        'device': row8[13],
        'consulttype': row8[14],
        'visityear': row8[15],
        'visitmonth': row8[16],
        'mes': row8[17],
        'annio': row8[18],
        'semana': row8[19],
        'country': row8[20],
    })
    row8 = cursor8.fetchone()

for res in resumen:
    timerango = ''
    TIPO = None
    rango1 = 0

    for rou in routes:

        if res['userid'] == rou['userid'] and res['annio'] == str(rou['annio']) and res['mes'] == str(rou['mes']) and res['ciudad'] == rou['country'] and res['formato'] == rou['format']:
            for rang in rango:
                date = datetime.strptime(str(rou['visittime']), '%H:%M:%S')
                start = datetime.strptime(str(rang['horainicio']), '%H:%M:%S')
                end = datetime.strptime(str(rang['horafin']), '%H:%M:%S')
                if date >= start and date <= end:

                    timerango = rang['descripcion']
                    rango1 = rang['state']

            if rango1 == 0:
                timerango = 'De 23:00 a 01:00'
                rango1 = '1'

            if rou['consulttype'] == 2:
                TIPO = "VISITA"
            else:
                TIPO = "NO VISITA"

            # print(res['id'],rou['channel'],rou['userid'],rou['username'],rou['district'],rou['salespoint'],rou['gln'],rou['reasondescription'],rou['visitdate'],rou['visittime'],rou['device'],rango,description,TIPO,rou['semana'])
            print("INSERTANDO TABLA 3")
            sql = "INSERT INTO `routescompliance_detail_cam_data` (`id_routescompliance_data`, `canal`, `nombre`, `distrito`,`formato`,`ciudad`, `pdv`, `gln`, `reasondescription`, `fecha`, `hora`, `dispositivo`, `rango`, `time_rango`, `tipo`, `semana`,`mes`, `annio`) VALUES ({0},'{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}',{15},'{16}','{17}')".format(
                res['id'], rou['channel'], rou['username'], rou['district'], res['formato'], res['ciudad'], rou['salespoint'], rou['gln'], rou['reasondescription'], rou['visitdate'], rou['visittime'], rou['device'], rango1, timerango, TIPO, rou['semana'], res['mes'], res['annio'])
            cur.execute(sql)
            dbGlobal.commit()

print("termina fase 3")
