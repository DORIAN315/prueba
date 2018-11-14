########## sos
import datetime
import re
import numpy as np
import statistics
import sys
from datetime import datetime, timedelta
import calendar

import pymysql
# import psycopg2

db = pymysql.connect(host="localhost", user="erfectsto27", passwd="ser27bert27", db="unilever_erfectstore_27")
meses = ['09']
# db = psycopg2.connect(host="localhost",database="darepslive", user="postgres", password="Vh8Z6jjC757U72k9" ,sslmode='require')
for lismes in meses:
    today=datetime.now()
    if today.month < 10:
        date_start = "%s-0%s-01" % (today.year, today.month)
        date_end =  "%s-0%s-%s" % (today.year, today.month, today.day)
        if  today.day < 10:
            date_end =  "%s-0%s-0%s" % (today.year, today.month, today.day)
    else:    
        date_start = "%s-%s-01" % (today.year, today.month)
        date_end =  "%s-%s-%s" % (today.year, today.month, today.day)
        if  today.day < 10:
            date_end =  "%s-%s-0%s" % (today.year, today.month, today.day)

    # [ESPACIOS X MARCAS X GLN X CATEGORIA]
    cursor3 =db.cursor()
    cursor3.execute("""SELECT MAX( CAST( tm.spacing AS SIGNED ) ) AS spacing
            , tm.trademark
            , tm.category
            , tm.district
            , tm.gln
            , tm.supplier
            , tm.channel
            , tm.salepoint
            , tm.clientone
            , tm.client
            , tm.countryname
            , tm.supplier
            , tm.polldate
            FROM tbls_reportsdata tm                
            WHERE
            tm.supplier IN ('UNILEVER', 'MERCADO')
            AND tm.spacing != ''
            and tm.trademark != ''
            
            AND tm.countryname = 'COLOMBIA'
            AND tm.polldate BETWEEN  '2018-{0}-01' AND '2018-{0}-31'
            AND tm.trademark NOT IN ('MULTIMARCA', 'MULTICATEGORIA')
            GROUP BY tm.trademark, tm.gln, tm.category  order by tm.gln asc""".format(lismes))
    row3 = cursor3.fetchone() 
    response = []
    while row3 is not None:
        response.append({
            'spacing':row3[0],
            'trademark': row3[1],
            'category' : row3[2],
            'district' : row3[3],
            'gln' : row3[4],
            'supplier' : row3[5],
            'channel': row3[6],
            'salepoint': row3[7],
            'clientone': row3[8],
            'client': row3[9],
            'countryname': row3[10],
            'polldate' :row3[11],
        })
        row3 = cursor3.fetchone()
    
    cursor3 =db.cursor()
    cursor3.execute("""SELECT MAX( CAST( tm.spacing AS SIGNED ) ) AS spacing
            , tm.trademark
            , tm.category
            , tm.district
            , tm.gln
            , tm.supplier
            , tm.channel
            , tm.salepoint
            , tm.clientone
            , tm.client
            , tm.countryname
            , tm.supplier
            FROM tbls_reportsdata tm                
            WHERE
            tm.supplier IN ('UNILEVER', 'MERCADO')
            AND tm.spacing != ''
            and tm.trademark != ''
            AND tm.countryname = 'COLOMBIA'
            AND tm.polldate BETWEEN  '2018-{0}-01' AND '2018-{0}-31'
            AND tm.trademark NOT IN ('MULTIMARCA', 'MULTICATEGORIA', 'SW', 'FRUCO_DELI', 'DRESSINGS_BASEMAYONESA')
            GROUP BY tm.trademark, tm.gln, tm.category  order by tm.gln asc""".format(lismes))
    row3 = cursor3.fetchone() 
    response2 = []
    while row3 is not None:
        response2.append({
            'spacing':row3[0],
            'trademark': row3[1],
            'category' : row3[2],
            'district' : row3[3],
            'gln' : row3[4],
            'supplier' : row3[5],
            'channel': row3[6],
            'salepoint': row3[7],
            'clientone': row3[8],
            'client': row3[9],
            'countryname': row3[10],
        })
        row3 = cursor3.fetchone()


    #  [MARCAS] idcountry 1 = colombia
    cursor2 =db.cursor()
    cursor2.execute('''SELECT * FROM tbls_makes where idcountry = '1' ''')
    row2 = cursor2.fetchone() 
    marcas = []
    while row2 is not None:
        marcas.append({
            'idmake':row2[0],
            'make': row2[1],
            'darepslivecode' : row2[4],
            'percentage' : row2[7],
            'minimumpercentage' : row2[8],
            'idcountry' : row2[9]
        })
        row2 = cursor2.fetchone()

    #[FORMULA]
    cursor1 =db.cursor()
    cursor1.execute('''SELECT * FROM `tbls_sosformula` where idcountry = '1' ''')
    row1 = cursor1.fetchone() 
    formula = []
    while row1 is not None:
        formula.append({
            'idosaformula':row1[0],
            'idmakeunilever': row1[1],
            'idmakemarket' : row1[2],
            'idaction' : row1[3],
            'description' : row1[4],
            'idsubreport' : row1[5],
            'idmake' : row1[6],
            'idcountry' : row1[7]
        })
        row1 = cursor1.fetchone()


    #[SOSMAKEMARKEDESCRIPTION]
    cursor4 =db.cursor()
    cursor4.execute('''SELECT * FROM tbls_sosmakemarketdescription ''')
    row4 = cursor4.fetchone() 
    mercadoDesc = []
    while row4 is not None:
        mercadoDesc.append({
            'idsosmakemarketdescription':row4[0],
            'idmake': row4[1],
            'idmakemarket' : row4[2],
            'idcountry' : row4[3],
        })
        row4 = cursor4.fetchone()

    #[SOSMAKEUNILEVERDESCRIPTION]
    cursor5 =db.cursor()
    cursor5.execute('''SELECT * FROM tbls_sosmakesunileverdescription ''')
    row5 = cursor5.fetchone() 
    unileverDesc = []
    while row5 is not None:
        unileverDesc.append({
            'idsosmakesunileverdescription':row5[0],
            'idmark': row5[1],
            'idmakeunilever' : row5[2],
            'idcountry' : row5[3],
        })
        row5 = cursor5.fetchone()

    print("teoria relatividad")

    # # PUNTOVENTA
    # cursor6 =db.cursor()
    # cursor6.execute('''select DISTINCT(gln) from tbls_reportsdata order by gln asc''')
    # row6 = cursor6.fetchone() 
    # puntoVenta = []
    # while row6 is not None:
    #     puntoVenta.append({
    #         'gln':row6[0],
    #     })
    #     row6 = cursor6.fetchone()


    cursor7 =db.cursor()
    cursor7.execute('''SELECT SUM(`porcentaje`) total_porcentage, COUNT(`porcentaje`) contador_porcentage, trademark, gln, annio
        FROM `sos_colombia_data` GROUP BY `annio`,`category`,`trademark` ''')
    row7 = cursor7.fetchone() 
    ytd_list = []
    while row7 is not None:
        ytd_list.append({
            'total_porcentage':row7[0],
            'contador_porcentage':row7[1],
            'trademark':row7[2],
            'gln':row7[3],
            'annio':row7[4],
        })
        row7 = cursor7.fetchone()


    unilever = 0.0
    markas = 0.0
    eliminar = 0


    for resp in response:
        for mark in marcas:
            if resp['trademark'] == mark['darepslivecode']:
                minporce = float(mark['minimumpercentage'])
                maxporce = float(mark['percentage'])
                action = 0
                for formu in formula:
                    if formu['idmake'] == mark['idmake']:
                        action = formu['idaction']
                        entrega = formu['description']
                        unilever = float(resp['spacing'])
                        for sosmak in mercadoDesc:
                            if sosmak['idmakemarket'] == formu['idmakemarket']:
                                for res1 in marcas:
                                    if sosmak['idmake'] == res1['idmake']:
                                        for date in response2:
                                            if date['trademark'] == res1['darepslivecode'] and date['category'] == resp['category'] and date['gln'] == resp['gln']:
                                                
                                                markas = float(date['spacing'])
                                                if action == 2 and unilever != 0.0 and markas != 0.0  :
                                                    porcentaje = (unilever/markas)*100
                                                    
                                                    
                                                    cont = 0
                                                    fecha = resp['polldate']
                                                    for y in ytd_list:
                                                        if y['gln'] == resp['gln'] and y['trademark'] == resp['trademark'] and y['annio'] == fecha[:4]:
                                                            sub_ytd = y['total_porcentage'] + porcentaje
                                                            contador = y['contador_porcentage'] + 1
                                                            ytd = sub_ytd/contador
                                                            cont += 1
                                                    
                                                    if cont == 0 :
                                                        ytd = porcentaje
                                                            
                                                    cur =db.cursor()
                                                    date1 = datetime.now()
                                                    porcentaje1 = float("{0:.2f}".format(porcentaje))
                                                    ytd1 = float("{0:.2f}".format(ytd))
                                                    unilever1 = float(unilever)

                                                    if minporce <= porcentaje1 and maxporce >= porcentaje1:
                                                        if date1.strftime("%d") == '01' and porcentaje1 >= mark['minimumpercentage'] and porcentaje <= mark['percentage']:

                                                            sql = "INSERT INTO `sos_colombia_data` (`month`, `annio`, `country`, `district`, `channel`, `gln`, `salespoint`, `clientone`, `client`, `category`, `trademark`, `porcentaje`, `ytd`, `spacing_mercado`, `spacing_unilever`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}',{11},{12},{13},{14})".format(
                                                                lismes, '2018', resp['countryname'], resp['district'], resp['channel'], resp['gln'], resp['salepoint'], resp['clientone'], resp['client'], resp['category'], entrega, porcentaje1, ytd1, unilever1, markas)
                                                            cur.execute(sql)
                                                            db.commit()
                                                        
                                                        else:
                                                            if eliminar == 0:
                                                                cur.execute(
                                                                    "DELETE FROM `sos_colombia_data` WHERE `sos_colombia_data`.`month` = {0} and  `sos_colombia_data`.`annio` = {1} ".format(lismes, '2018'))
                                                                db.commit()
                                                                eliminar += 1
                                                            
                                                            
                                                            sql = "INSERT INTO `sos_colombia_data` (`month`, `annio`, `country`, `district`, `channel`, `gln`, `salespoint`, `clientone`, `client`, `category`, `trademark`, `porcentaje`, `ytd`, `spacing_mercado`, `spacing_unilever`) VALUES ('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}',{11},{12},{13},{14})".format(
                                                                lismes, '2018', resp['countryname'], resp['district'], resp['channel'], resp['gln'], resp['salepoint'], resp['clientone'], resp['client'], resp['category'], entrega, porcentaje1, ytd1, unilever1, markas)
                                                            cur.execute(
                                                                sql)
                                                            db.commit()

                                                        # print("INSERTAMOS")
                                                    unilever = 0.0
                                                    markas = 0.0

    print("termina mes ", lismes)
