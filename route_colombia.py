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
dbColombia = pymysql.connect(
    host="localhost", user="mercadeo11", passwd="merca89uni", db="unilever_erfectstore")
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


# if day == '1':
#     if month == '01':
#         month = '12'
#         year1 = int(year) - 1

#         if year1:
#             year = "%s" % (year1)
#     else:
#         month1 = int(month) - 1
#         if month1 < 10:
#             month = "0%s" % (month1)
#         else:
#             month = "%s" % (month1)

#         cursor = dbGlobal.cursor()
#         cursor.execute(
#             "DELETE FROM `routescompliance_colombia_data` WHERE mes` = {0} and  `annio` = {1} ".format(month, year))
#         cursor.execute(
#             "DELETE FROM  `routescompliance_reason_colombia_data` WHERE mes` = {0} and  `annio` = {1} ".format(month, year))
#         cursor.execute(
#             "DELETE FROM `routescompliance_detail_colombia_data` WHERE mes` = {0} and  `annio` = {1} ".format(month, year))
#         print('eliminando registros del mes ', month, ' annio ', year)


# else:
#     cursor = dbGlobal.cursor()
#     cursor.execute(
#         "DELETE FROM `routescompliance_colombia_data` WHERE mes` = {0} and  `annio` = {1} ".format(month, year))
#     cursor.execute(
#         "DELETE FROM  `routescompliance_reason_colombia_data` WHERE mes` = {0} and  `annio` = {1} ".format(month, year))
#     cursor.execute(
#         "DELETE FROM `routescompliance_detail_colombia_data` WHERE mes` = {0} and  `annio` = {1} ".format(month, year))
#     print('eliminando registros del mes ', month, ' annio ', year)

month = '08'
year = '2018'
# month = '08'
# 1 consulta
# cursor = dbColombia.cursor()
# cursor.execute("""SELECT   userid,
#                         username,
#                         gln,
#                         salespoint,
#                         channel,
#                         district,
#                         reason,
#                         reasondescription,
#                         visitdate,
#                         visittime,
#                         masterid,
#                         chargedate,
#                         chargetime,
#                         device,
#                         consulttype,
#                         visityear,
#                         visitmonth,
#                         MONTH(chargedate)  as mes,
#                         YEAR(chargedate) as annio
#                         FROM `tbls_routescomplience` where consulttype = 1 and month(chargedate) = {0} and year(chargedate) = {1} group by userid """.format(month, year))
# row = cursor.fetchone()
# response = []
# while row is not None:
#     response.append({
#         'userid': row[0],
#         'username': row[1],
#         'gln': row[2],
#         'salespoint': row[3],
#         'channel': row[4],
#         'district': row[5],
#         'reason': row[6],
#         'reasondescription': row[7],
#         'visitdate': row[8],
#         'visittime': row[9],
#         'masterid': row[10],
#         'chargedate': row[11],
#         'chargetime': row[12],
#         'device': row[13],
#         'consulttype': row[14],
#         'visityear': row[15],
#         'visitmonth': row[16],
#         'mes': row[17],
#         'annio': row[18],

#     })
#     row = cursor.fetchone()

#2
cursor1 = dbColombia.cursor()
cursor1.execute("""SELECT max(masterid) as maximo,
            userid 
            FROM `tbls_routescomplience` 
            where consulttype = 1 and month(chargedate) = {0} and year(chargedate) = {1}  group by userid""".format(month, year))
row1 = cursor1.fetchone()
response1 = []
while row1 is not None:
    response1.append({
        'maximo': row1[0],
        'userid': row1[1],
    })
    row1 = cursor1.fetchone()
# # 3 consulta
# cursor2 = dbColombia.cursor()
# cursor2.execute("""SELECT userid,
#         count(username) as visitreals, masterid
#         FROM `tbls_routescomplience`
#         where consulttype = 1 and month(chargedate) = {0} and year(chargedate) = {1}
#         group by userid""".format(month, year))
# row2 = cursor2.fetchone()
# response2 = []
# while row2 is not None:
#     response2.append({
#         'userid': row2[0],
#         'visitreals': row2[1],
#         'masterid': row2[2],
#     })
#     row2 = cursor2.fetchone()
# # 4 consulta

# cursor3 = dbColombia.cursor()
# cursor3.execute("""SELECT  userid,count(userid) as visit
#             FROM `tbls_routescomplience`
#             where consulttype = 2 and month(visitdate) = {0} and year(visitdate) = {1}
#             group by userid""".format(month, year))
# row3 = cursor3.fetchone()
# response3 = []
# while row3 is not None:
#     response3.append({
#         'userid': row3[0],
#         'visit': row3[1],
#     })
#     row3 = cursor3.fetchone()

# # 5 consulta

# cursor4 = dbColombia.cursor()
# cursor4.execute("""SELECT * FROM `tbls_novisitsreason`""")
# row4 = cursor4.fetchone()
# response4 = []
# while row4 is not None:
#     response4.append({
#         'id': row4[0],
#         'description': row4[1],
#         'active': row4[2],
#     })
#     row4 = cursor4.fetchone()

# # 5 consulta

# cursor5 = dbColombia.cursor()
# cursor5.execute(
#     """SELECT userid,reason FROM `tbls_routescomplience` where consulttype = 3 and month(visitdate) = {0} and year(visitdate) = {1} """.format(month, year))
# row5 = cursor5.fetchone()
# response5 = []
# while row5 is not None:
#     response5.append({
#         'userid': row5[0],
#         'reason': row5[1],
#     })
#     row5 = cursor5.fetchone()


####################################################
#  raoutescompliancedata
####################################################
# noAfectan = 0
# Afectan = 0
# for data in response:
#     for maxid in response1:
#         if data['userid'] == maxid['userid']:
#             cursor2 = dbColombia.cursor()
#             cursor2.execute("""SELECT userid,
#                     count(username) as visitreals, masterid
#                     FROM `tbls_routescomplience`
#                     where consulttype = 1 and month(chargedate) = {0} and year(chargedate) = {1}  and masterid = {2}
#                     group by userid""".format(month, year, maxid['maximo']))
#             row2 = cursor2.fetchone()
#             response2 = []
#             while row2 is not None:
#                 response2.append({
#                     'userid': row2[0],
#                     'visitreals': row2[1],
#                     'masterid': row2[2],
#                 })
#                 row2 = cursor2.fetchone()
#             for data2 in response2:
#                 if maxid['userid'] == data2['userid']:
#                     for data3 in response3:
#                         if data3['userid'] == data['userid']:

#                             for data4 in response5:
#                                 if data4['userid'] == data['userid']:
#                                     for noVisita in response4:
#                                         if data4['reason'] == noVisita['id']:
#                                             if noVisita['active'] == 1:
#                                                 noAfectan += 1
#                                             else:
#                                                 Afectan += 1

#                             subt = float(noAfectan) + float(data3['visit'])
#                             total = subt*100/float(data2['visitreals'])
#                             porcentaje = float(
#                                 data3['visit'])*100/float(data2['visitreals'])
#                             # print(data['channel'],data['userid'],data['username'],data['district'],data2['visitreals'],data3['visit'],porcentaje,noAfectan,Afectan,total,data['mes'],data['annio'])
#                             print("INSERTANDO TABLA 1")
#                             sql = "INSERT INTO `routescompliance_colombia_data` ( `canal`, `cedula`, `nombre`, `distrito`, `vis_programadas`, `vis_reales`, `porc_cumple`, `novisita_afectan`, `novisita_no_afecta`, `porc_cumplimiento`, `mes`, `annio`) VALUES ('{0}','{1}','{2}','{3}',{4},'{5}',{6},{7},{8},{9},'{10}','{11}')".format(
#                                 data['channel'], data['userid'], data['username'], data['district'], data2['visitreals'], data3['visit'], porcentaje, noAfectan, Afectan, total, data['mes'], data['annio'])
#                             cur.execute(sql)
#                             dbGlobal.commit()
#                             noAfectan = 0
#                             Afectan = 0


# ###################################################
# #   routescompliancereasondata
# ###################################################
print("termina fase 1")
print("hola")
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
                        annio
                        FROM `routescompliance_colombia_data` """)
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
    })
    row6 = cursor6.fetchone()

# cursor7 = dbColombia.cursor()
# cursor7.execute(
#     """SELECT userid,username,reason,count(reason) as num_reason, MONTH(visitdate) as mes ,YEAR(visitdate) as annio FROM `tbls_routescomplience` where consulttype = 3 and month(visitdate) = {0} and year(visitdate) = {1} group by reason,userid """.format(month, year))
# row7 = cursor7.fetchone()
# counter = []
# while row7 is not None:
#     counter.append({
#         'userid': row7[0],
#         'username': row7[1],
#         'reason': row7[2],
#         'num_reason': row7[3],
#         'mes': row7[4],
#         'annio': row7[5],
#     })
#     row7 = cursor7.fetchone()


# for data in resumen:
#     for counters in counter:
#         if data['userid'] == counters['userid'] and data['mes'] == str(counters['mes']) and data['annio'] == str(counters['annio']):
#             for x in response4:
#                 if counters['reason'] == x['id']:
#                     if x['active'] == 0:
#                         # print("INSERTANDO TABLA 2")
#                         sql = "INSERT INTO `routescompliance_reason_colombia_data` ( `id_routes_compliance_data`, `cedula`, `id_causal`, `total_causal`, `mes`, `annio`, `afecta_cumplimiento`) VALUES ({0},'{1}',{2},'{3}','{4}',{5},{6})".format(
#                             data['id'], data['userid'], counters['reason'], counters['num_reason'], counters['mes'], counters['annio'], 0)
#                         cur.execute(sql)
#                         dbGlobal.commit()
#                         # print(data['id'],data['userid'],counters['reason'],counters['num_reason'],counters['annio'],counters['mes'],'0')
#                     else:
#                         # print("INSERTANDO TABLA 2")
#                         sql = "INSERT INTO `routescompliance_reason_colombia_data` ( `id_routes_compliance_data`, `cedula`, `id_causal`, `total_causal`, `mes`, `annio`, `afecta_cumplimiento`) VALUES ({0},'{1}',{2},'{3}','{4}',{5},{6})".format(
#                             data['id'], data['userid'], counters['reason'], counters['num_reason'], counters['mes'], counters['annio'], 1)
#                         cur.execute(sql)
#                         dbGlobal.commit()
#                         print(data['id'],data['userid'],counters['reason'],counters['num_reason'],counters['annio'],counters['mes'],'1')


################################
#  routescompliancedetaildata
###############################
print("termina fase 2")


cursor9 = dbColombia.cursor()
cursor9.execute(
    """SELECT Descripcion, HoraInicio,HoraFin,EstadoLaboral FROM `horario_laboral`""")
row9 = cursor9.fetchone()
rango = []
while row9 is not None:
    rango.append({
        'descripcion': row9[0],
        'horainicio': row9[1],
        'horafin': row9[2],
        'estado': row9[3],
    })
    row9 = cursor9.fetchone()

timerango = ''
TIPO = None
rango1 = 0
print("iniciando")
for res in resumen:
    for maxid in response1:
        # print("fase 3", maxid['maximo'])
        if res['userid'] == maxid['userid']:
            print("fase 4")
            cursor8 = dbColombia.cursor()
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
                                    masterid,
                                    chargedate,
                                    chargetime,
                                    device,
                                    consulttype,
                                    visityear,
                                    visitmonth,
                                    MONTH(visitdate)  as mes,
                                    YEAR(visitdate) as annio,
                                    WEEK(visitdate, 5) - WEEK(DATE_SUB(visitdate, INTERVAL DAYOFMONTH(visitdate) - 1 DAY), 5) + 1
                                    FROM `tbls_routescomplience` where month(visitdate) = {0} and year(visitdate) = {1} and masterid = {2}""".format(month, year, maxid['maximo']))
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
                    'masterid': row8[10],
                    'chargedate': row8[11],
                    'chargetime': row8[12],
                    'device': row8[13],
                    'consulttype': row8[14],
                    'visityear': row8[15],
                    'visitmonth': row8[16],
                    'mes': row8[17],
                    'annio': row8[18],
                    'semana': row8[19],
                })
                row8 = cursor8.fetchone()
            for rou in routes:
                print(res['userid'], rou['userid'], res['annio'],
                      rou['annio'], res['mes'], rou['mes'])
                print(type(res['userid']), type(rou['userid']), type(res['annio']), type(
                    rou['annio']), type(res['mes']), type(rou['mes']))
                if res['userid'] == rou['userid'] and res['annio'] == str(rou['annio']) and res['mes'] == str(rou['mes']):
                    for rang in rango:
                        date = datetime.strptime(
                            str(rou['visittime']), '%H:%M:%S')
                        start = datetime.strptime(
                            str(rang['horainicio']), '%H:%M:%S')
                        end = datetime.strptime(
                            str(rang['horafin']), '%H:%M:%S')
                        if date >= start and date <= end:

                            timerango = rang['descripcion']
                            rango1 = rang['estado']

                    if rango1 == 0:
                        timerango = 'De 23:00 a 01:00'
                        rango1 = '1'

                    if rou['consulttype'] == 2:
                        TIPO = "VISITA"
                    else:
                        TIPO = "NO VISITA"

                    # print(res['id'],rou['channel'],rou['userid'],rou['username'],rou['district'],rou['salespoint'],rou['gln'],rou['reasondescription'],rou['visitdate'],rou['visittime'],rou['device'],rango,description,TIPO,rou['semana'])
                    print("INSERTANDO TABLA 3")
                    sql = "INSERT INTO `routescompliance_detail_colombia_data` (`id_routescompliance_data`, `canal`, `nombre`, `distrito`, `pdv`, `gln`, `reasondescription`, `fecha`, `hora`, `dispositivo`, `rango`, `time_rango`, `tipo`, `semana`,`mes`, `annio`) VALUES ({0},'{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}',{13},'{14}','{15}')".format(
                        res['id'], rou['channel'], rou['username'], rou['district'], rou['salespoint'], rou['gln'], rou['reasondescription'], rou['visitdate'], rou['visittime'], rou['device'], rango1, timerango, TIPO, rou['semana'], res['mes'], res['annio'])
                    cur.execute(sql)
                    dbGlobal.commit()

print("termina fase 3")
