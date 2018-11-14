
# -*- coding: utf-8 -*
import datetime
import re
import numpy as np
import statistics
import sys
from datetime import datetime, timedelta
import calendar

import pymysql


dbGlobal = pymysql.connect(host="localhost", user="erfectsto27", passwd="ser27bert27", db="unilever_erfectstore_27")
dbColombia = pymysql.connect(host="localhost", user="mercadeo11", passwd="merca89uni", db="unilever_erfectstore")
## OSA

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
            "DELETE FROM `bigmoments_colombia_data` WHERE month = {0} and  `year` = {1} ".format(month, year))
        print('eliminando registros del mes ', month, ' annio ', year)


else:
    cursor = dbGlobal.cursor()
    cursor.execute(
        "DELETE FROM `bigmoments_colombia_data` WHERE month = {0} and  `year` = {1} ".format(month, year)) 
    print('eliminando registros del mes ', month, ' annio ', year)


cursor =dbColombia.cursor()
cursor.execute("""select WEEK(OSAR.datepoll),COUNT(OSAR.uncoded),typology,district,channel,clientone,client,category,make,year,month from tbls_osareportdata OSAR where OSAR.year in (2018) and MONTH(OSAR.datepoll) in (7) and OSAR.provedor='INNOVACIONES' and OSAR.kpi ='INNOVACIONES' AND uncoded = 'NA' group by WEEK(OSAR.datepoll),typology,district,channel,clientone,client,category,make order by WEEK(OSAR.datepoll),typology,district,channel,category,make asc""")
row = cursor.fetchone() 
response = []
while row is not None:
    response.append({
        'semana':row[0],
        'contador':row[1],
        'tipologia':row[2],
        'distrito':row[3],
        'canal':row[4],
        'clienteone':row[5],
        'cliente':row[6],
        'categoria':row[7],
        'marca':row[8],
        'annio':row[9],
        'mes':row[10],
    })
    row = cursor.fetchone()


# ESTE ME DA VALOR 3
cursor1 =dbColombia.cursor()
cursor1.execute("""select WEEK(OSAR.datepoll),SUM(OSAR.yes) AS SI ,sum(OSAR.no) AS NO ,OSAR.typology,district,channel,clientone,client,category,make from tbls_osareportdata OSAR where OSAR.year in (2018) and MONTH(OSAR.datepoll) in (7) and OSAR.provedor='INNOVACIONES' and OSAR.kpi ='INNOVACIONES' group by WEEK(OSAR.datepoll),typology,district,channel,clientone,client,category,make order by WEEK(OSAR.datepoll),typology,district,channel,category,make asc """)
row1 = cursor1.fetchone() 
response1 = []
while row1 is not None:
    response1.append({
        'semana':row1[0],
        'si':row1[1],
        'no':row1[2],
        'tipologia':row1[3],
        'distrito':row1[4],
        'canal':row1[5],
        'clienteone':row1[6],
        'cliente':row1[7],
        'categoria':row1[8],
        'marca':row1[9],
        
    })
    row1 = cursor1.fetchone()
print("INICIA OSA")
for res in response:
    for res1 in response1:
        if res1['semana'] == res['semana'] and res1['tipologia'] == res['tipologia'] and res1['distrito'] == res['distrito'] and res1['canal'] == res['canal'] and res1['clienteone'] == res['clienteone'] and res1['cliente'] == res['cliente'] and res1['categoria'] == res['categoria'] and res1['marca'] == res['marca']:
            count = 0
            if res['contador']:
                count = res['contador']
            valor = float(res1['si']+res1['no']+count)
            porcentaje = float(res1['si']*100)/valor
            print('4',res['annio'],res['mes'],res['semana'],res['canal'],res['distrito'],res['tipologia'],res['clienteone'],res['cliente'],res['categoria'],res['marca'],porcentaje)

            #INSERT
            sql = "INSERT INTO `bigmoments_colombia_data` (`idreport`,`year`,`month`,`channel`,`district`,`format`,`clientone`,`client`,`category`,`trademark`,`week`,`percentage`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}',{10},{11})".format('4',res['annio'],res['mes'],res['canal'],res['distrito'],res['tipologia'],res['clienteone'],res['cliente'],res['categoria'],res['marca'],res['semana'],porcentaje)
            cur.execute(sql)
            dbGlobal.commit()

print("TERMINA OSA")

##########################  sos 
# lstmes =dbColombia.cursor()
# lstmes.execute("""select month(curdate())""")
# ltmes = lstmes.fetchone() 
# mesActual = []
# lstSemana = []
# while ltmes is not None:
#     mesActual.append({
#         'mes':ltmes[0],  
#     })
#     ltmes = lstmes.fetchone()
mesActual= ['07']

for mesActual1 in mesActual:
    
#semana
    cursor7 =dbColombia.cursor()
    cursor7.execute("""select DISTINCT(week(fechas))as semana from (select * from (select adddate('1970-01-01',t4.i*10000 + t3.i*1000 + t2.i*100 + t1.i*10 + t0.i) fechas from (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t0, (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t1, (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t2, (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t3, (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t4) v where fechas between '2018-{0}-01' and LAST_DAY('2018-{0}-01')) t2""".format(mesiterar))
    row7 = cursor7.fetchone() 
    while row7 is not None:
        lstSemana.append({
            'semana':row7[0],  
        })
        row7 = cursor7.fetchone()


# marcas innovacion
cursor8 =dbColombia.cursor()
cursor8.execute("""select C.idcategory , C.darepslivecode as darepslivecodeCategoria , M.idmake , M.darepslivecode as darepslivecodeMarca , M.iscategory from tbls_makes M inner join tbls_categories C on M.idcategory=C.idcategory where M.idmake in (select SF.idmake from tbls_sosformula SF where SF.idsubreport=(select SR.idmodulereports from tbls_subreports SR where SR.description='sos_report_innovation'))""")
row8 = cursor8.fetchone() 
lstMarcaInnovation = []
while row8 is not None:
    lstMarcaInnovation.append({
        'idcategoria':row8[0],  
        'categoria':row8[1],
        'idmake':row8[2],
        'make':row8[3],
        'iscategory':row8[4],
    })
    row8 = cursor8.fetchone()

porM_uni = 0.0
porM_mar = 0.0
porm_uni = 0.0
porm_mak = 0.0
make_uni = None
make_mar = None
print("INICIA SOS")
for semana in lstSemana:
    for marcainnovation in lstMarcaInnovation:
        # obtener Id (mercado y unilever)
        cursor9 =dbColombia.cursor()
        cursor9.execute("""SELECT idmakeunilever,idmakemarket,idaction,description,idmake FROM `tbls_sosformula` WHERE `idmake` = {0}""".format(marcainnovation['idmake']))
        row9 = cursor9.fetchone() 
        lstformula = []
        while row9 is not None:
            lstformula.append({
                'idmakeunilever':row9[0],  
                'idmakemarket':row9[1],
                'idaction':row9[2],
                'description':row9[3],
                'idmake':row9[4],
            })
            row9 = cursor9.fetchone()

        for formula in lstformula:
            # marca unilever
            cursor11 =dbColombia.cursor()
            cursor11.execute("""SELECT M.idmake, M.make, M.darepslivecode as Mdarepslivecode,C.darepslivecode as Cdarepslivecode,M.percentage,M.minimumpercentage FROM tbls_sosmakesunilever MU INNER JOIN tbls_sosmakesunileverdescription MUD ON MU.idmakeunilever = MUD.idmakeunilever INNER JOIN tbls_makes M ON M.idmake = MUD.idmark INNER JOIN tbls_categories C ON M.idcategory=C.idcategory where MU.idmakeunilever = {0}""".format(formula['idmakeunilever']))
            row11 = cursor11.fetchone() 
            lstmakeuni = []
            while row11 is not None:
                lstmakeuni.append({
                    'idmake':row11[0],  
                    'make':row11[1],
                    'makeuni':row11[2],
                    'cateuni':row11[3],
                    'percentage':row11[4],
                    'minimumpercentage':row11[5],
                })
                row11 = cursor11.fetchone()
            
            # marca mercado
            cursor12 =dbColombia.cursor()
            cursor12.execute("""SELECT M.idmake ,M.make , M.darepslivecode as Mdarepslivecode ,C.darepslivecode as Cdarepslivecode,M.percentage,M.minimumpercentage FROM tbls_sosmakemarket MM INNER JOIN tbls_sosmakemarketdescription MMD ON MM.idsosmakemarket = MMD.idmakemarket INNER JOIN tbls_makes M ON M.idmake = MMD.idmake INNER JOIN tbls_categories C ON M.idcategory=C.idcategory where MM.idsosmakemarket = {0}""".format(formula['idmakemarket']))
            row12 = cursor12.fetchone() 
            lstmakemarker = []
            while row12 is not None:
                lstmakemarker.append({
                    'idmake':row12[0],  
                    'make':row12[1],
                    'makeuni':row12[2],
                    'cateuni':row12[3],
                    'percentage':row12[4],
                    'minimumpercentage':row12[5],
                })
                row12 = cursor12.fetchone()
            ### mercado y unilever
            
            for makeuni in lstmakeuni:
                make_uni = makeuni['make']
                porM_uni = makeuni['percentage']
                porm_uni = makeuni['minimumpercentage']

            for makemarker in lstmakemarker:
                make_mar = makemarker['make']
                porM_mar = makemarker['percentage']
                porm_mar = makemarker['minimumpercentage']
            

            # VARIABLES PARA CONSULTA 
            # CONSULTA PORCENTAGE
            # ordenar semana año
            cursor13 = dbColombia.cursor()
            cursor13.execute("""select sum(porcentaje)/count(porcentaje),sum(porcentaje),count(porcentaje),gln,year,month,category,make,channel,distrito,tipopunto,clientone,cliente from (select suma,suma1,CONVERT((suma*100/suma1), DECIMAL(4,2))as porcentaje, gln,year,month,category,make,channel,distrito,tipopunto,clientone,cliente from (SELECT * FROM (select max(CAST( OS.cms AS SIGNED )) as suma , OS.gln AS gln,year,month,category,make,channel,distrito,tipopunto,clientone,cliente from tbls_sosreportdata OS where OS.year in (2018) and MONTH(OS.fecha_encuesta) in (7) AND OS.make='{0}' and OS.category='{1}' and OS.proveedor='INNOVACIONES' group by OS.gln,year,month,category,make) T1 INNER JOIN (select max(CAST(  OS.cms AS SIGNED )) as suma1 , OS.gln AS gln1,year as year1,month as month1,category as category1,make as make1 from tbls_sosreportdata OS where OS.year in (2018) and MONTH(OS.fecha_encuesta) in (7) AND OS.make= '{2}' and OS.category='{1}' and OS.proveedor='INNOVACIONES' group by OS.gln,year,month,category,make) T2 on T1.gln = T2.gln1  inner join (select OS.gln as gln3,year year3,month month3,category category3,make make3 from tbls_sosreportdata OS where OS.year=2018 and OS.month in (7) and OS.proveedor='INNOVACIONES' group by OS.gln,year,month,category,make) T3 ON  T3.gln3 = T1.gln GROUP BY gln,year,month,category,make) T5) T6  where porcentaje <= {3} and porcentaje >= {4} and suma1 <> 0 GROUP BY gln,year,month,category,make order by gln desc""".format(make_uni,marcainnovation['categoria'],make_mar,porM_uni,porm_uni))
            row13 = cursor13.fetchone() 
            porc_total = []
            while row13 is not None:
                porc_total.append({
                    'porcentaje_final':row13[0],  
                    'suma':row13[1],
                    'contador':row13[2],
                    'gln':row13[3],  
                    'annio':row13[4],
                    'mes':row13[5],
                    'categoria':row13[6],  
                    'marca':row13[7],
                    'canal':row13[8],
                    'distrito':row13[9],  
                    'formato':row13[10],
                    'clienteone':row13[11],
                    'cliente':row13[12],  
                })
                row13 = cursor13.fetchone()
            
            print("KE TE PASA WEEEE !!!")
            for fin in porc_total:
                print('2',fin['porcentaje_final'],fin['suma'],fin['contador'],fin['gln'],fin['annio'],fin['mes'],fin['categoria'],fin['marca'],fin['canal'],fin['distrito'],fin['formato'],fin['clienteone'],fin['cliente'],semana['semana'])

                # INSERT
                sql = "INSERT INTO `bigmoments_colombia_data` (`idreport`,`year`,`month`,`channel`,`district`,`format`,`clientone`,`client`,`category`,`trademark`,`week`,`percentage`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}',{10},{11})".format('2',fin['annio'],fin['mes'],fin['canal'],fin['distrito'],fin['formato'],fin['clienteone'],fin['cliente'],fin['categoria'],fin['marca'],semana['semana'],fin['porcentaje_final'])
                cur.execute(sql)
                dbGlobal.commit()



print("TERMINA SOS")
# ########################## PART 4  Detallado de exhibición por formato


cursor4 =dbColombia.cursor()
cursor4.execute("""select year, month,channel,district,typology,clientone,client,category,make,sum(medicionreal),sum(objective),(sum(medicionreal)*100)/sum(objective) from (SELECT year, month,channel,district,typology,clientone,client,category,make,max(`real`)as medicionreal ,objective,gln from tbls_detaildatainovation where year=2018 and month in (07) group by year, month,channel,district,typology,clientone,client,category,make,gln) as table1 group by year, month,channel,district,typology,clientone,client,category,make,gln""")
row4 = cursor4.fetchone() 
detallado = []
while row4 is not None:
    detallado.append({
        'annio':row4[0],
        'mes':row4[1],
        'canal':row4[2],
        'distrito':row4[3],
        'tipologia':row4[4],
        'clienteone':row4[5],
        'cliente':row4[6],
        'categoria':row4[7],
        'marca':row4[8],
        'medicionreal':row4[9],
        'objective':row4[10],
        'porcentage':row4[11],   
    })
    row4 = cursor4.fetchone()
print("INICIA DETALLADO")
for detail in detallado:
    print('5',detail['annio'],detail['mes'],detail['canal'],detail['distrito'],detail['tipologia'],detail['clienteone'],detail['cliente'],detail['categoria'],detail['marca'],detail['medicionreal'],detail['objective'],detail['porcentage'])
    sql = "INSERT INTO `bigmoments_colombia_data` (`idreport`,`year`,`month`,`channel`,`district`,`format`,`clientone`,`client`,`category`,`trademark`,`real`,`objetive`,`fullfilment`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}',{12})".format('5',detail['annio'],detail['mes'],detail['canal'],detail['distrito'],detail['tipologia'],detail['clienteone'],detail['cliente'],detail['categoria'],detail['marca'],detail['medicionreal'],detail['objective'],detail['porcentage'])
    cur.execute(sql)
    dbGlobal.commit()


print("TERMINA DETALLADO")
# ########################### Price

cursor5 =dbColombia.cursor()
cursor5.execute("""select tbl.Promedio ,tbl.idean ,remove_accents(tbl.dupla) as  dupla ,tbl.descriptionproduct ,tipologia,year,month,channel, district,clientone,client,category,make
                from (
                SELECT AVG(CAST(  P.value AS SIGNED )) as Promedio
                ,(select D.iddupla
                    from tbls_duplas D
                    where
                        D.ean= P.ean
                        and D.estado=1
                        and D.tipologia
                            like CONCAT('%',P.typology,'%')) as idean
                ,(select D.dupla
                    from tbls_duplas D
                    where D.ean= P.ean
                    and D.estado=1
                    and D.tipologia
                        like CONCAT('%',P.typology,'%'))as dupla
                ,remove_accents(P.descriptionproduct) as descriptionproduct
                    ,P.typology as tipologia,year,month,channel,district,clientone,client,category,make
                                from tbls_pricereportdata P
                where P.provedor='INNOVACIONES'
                AND CAST( P.value AS SIGNED ) <> 0
                AND P.year=2018
                and P.month in (7)
                group by P.ean,year,month,channel,district,clientone,client,category,make) tbl
                """)
row5 = cursor5.fetchone() 
price1 = []
while row5 is not None:
    price1.append({
        'promedio':row5[0],
        'idean':row5[1],
        'dupla':row5[2],
        'description':row5[3],
        'tipologia':row5[4],
        'annio':row5[5],
        'mes':row5[6],
        'canal':row5[7],
        'distrito':row5[8],
        'clienteone':row5[9],
        'cliente':row5[10],
        'categoria':row5[11],
        'marca':row5[12],
    })
    row5 = cursor5.fetchone()

############ RETORNA 2 VALOR
cursor6 =dbColombia.cursor()
cursor6.execute("""SELECT AVG(CAST(  P.value AS SIGNED )) as Promedio,P.typology, P.descriptionproduct,year,month,channel,district,clientone,client,category,make
            from tbls_pricereportdata P
            where P.provedor='INNOVACIONES'
            AND CAST( P.value AS SIGNED ) <>0
            AND P.year=2018
            and P.month in (7)
            group by P.descriptionproduct,typology,year,month,channel,district,clientone,client,category,make""")
row6 = cursor6.fetchone() 
price2 = []
while row6 is not None:
    price2.append({
        'promedio':row6[0],
        'tipologia':row6[1],
        'description':row6[2],
        'annio':row6[3],
        'mes':row6[4],
        'canal':row6[5],
        'distrito':row6[6],
        'clienteone':row6[7],
        'cliente':row6[8],
        'categoria':row6[9],
        'marca':row6[10],
    })
    row6 = cursor6.fetchone()
print("INICIA PRICE")
for p1 in price1:
    if p1['dupla'] is not None and p1['idean'] is not None:
        prom1 = 0.0
        prom2 = 0.0
        bandera = True
        for p2 in price2:
            if p1['dupla'] == p2['description'] and p1['tipologia'] == p2['tipologia'] and p1['annio'] == p2['annio'] and p1['mes'] == p2['mes'] and p1['canal'] == p2['canal']  and p1['distrito'] == p2['distrito'] and p1['clienteone'] == p2['clienteone'] and p1['cliente'] == p2['cliente'] and p1['categoria'] == p2['categoria'] and p1['marca'] == p2['marca']:

                
                promedio = float(p1['promedio']*100)/float(p2['promedio'])
                bandera = False
                print('7',p1['annio'],p1['mes'],p1['canal'],p1['distrito'],p1['tipologia'],p1['clienteone'],p1['cliente'],p1['categoria'],p1['marca'],p1['description'],p1['promedio'],p1['dupla'],p2['promedio'],promedio)

                #INSERT 
                sql = "INSERT INTO `bigmoments_colombia_data` (`idreport`,`year`,`month`,`channel`,`district`,`format`,`clientone`,`client`,`category`,`trademark`,`percentage`,`sku`,`price_sku`,`dupla`,`price_dupla`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}',{10},'{11}',{12},'{13}',{14})".format('7',p1['annio'],p1['mes'],p1['canal'],p1['distrito'],p1['tipologia'],p1['clienteone'],p1['cliente'],p1['categoria'],p1['marca'],promedio,p1['description'],p1['promedio'],p1['dupla'],p2['promedio'])
                cur.execute(sql)
                dbColombia.commit()


        if bandera:
            print('7',p1['annio'],p1['mes'],p1['canal'],p1['distrito'],p1['tipologia'],p1['clienteone'],p1['cliente'],p1['categoria'],p1['marca'],p1['description'],p1['promedio'],p1['dupla'],prom1,prom2)
            #INSERT 
            sql = "INSERT INTO `bigmoments_colombia_data` (`idreport`,`year`,`month`,`channel`,`district`,`format`,`clientone`,`client`,`category`,`trademark`,`percentage`,`sku`,`price_sku`,`dupla`,`price_dupla`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}',{10},'{11}',{12},'{13}',{14})".format('7',p1['annio'],p1['mes'],p1['canal'],p1['distrito'],p1['tipologia'],p1['clienteone'],p1['cliente'],p1['categoria'],p1['marca'],prom2,p1['description'],p1['promedio'],p1['dupla'],prom1)
            cur.execute(sql)
            dbGlobal.commit()


print("TERMINA PRICE")
