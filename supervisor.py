from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine, Float, DateTime, update, Date, Time, exc
from sqlalchemy.sql import select
from sqlalchemy.sql import text as sa_text
import time
import os

def dentroRango(rmax,rmin,num):
    #Si esta dentro del rango retorna 0
    resp = 0
    if (num > rmax):
        #Si sobrepasa el rango devuelve 1
        resp = 1
    if (num < rmin):
        #Si baja mas del rango devuelve -1
        resp = -1
    return resp

def buscarId(matriz, identificador):
    for valores in matriz:
        if valores.id == identificador:
            return valores
    return False


metadata = MetaData()
#Estructura de la tabla todo
todo = Table('todo', metadata,
             Column('id', Integer, primary_key=True),
             Column('temperatura', Float()),
             Column('humedad', Float()),
             Column('canal1', Float()),
             Column('canal2', Float()),
             Column('canal3', Float()),
             Column('canal4', Float()),
             Column('tempGabinete', Float()),
             Column('hora', Time()),
             Column('fecha', Date()),
             )

#Estructura de la tabla configuracion             
configuracion = Table('configuracion', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('tipo', Integer),
                      Column('frec', Integer),
                      Column('potmax', Float()),
                      Column('potmin', Integer()),
                      Column('tempmax', Integer()),
                      Column('tempmin', Integer()),
                      Column('checkbox', String(15)),
                      Column('ip', String(15))
                      )

#Estructura de la talbla ahora
ahora = Table('ahora', metadata,
              Column('id', Integer, primary_key=True),
              Column('temperatura', Float()),
              Column('humedad', Float()),
              Column('canal1', Float()),
              Column('canal2', Float()),
              Column('canal3', Float()),
              Column('canal4', Float()),
              Column('tempGabinete', Float()),
              Column('hora', Time()),
              )

#Estructura de la talbla alarma
alarmas = Table('alarmas', metadata,
                Column('id', Integer, primary_key = True),
                Column('codigo', String(10)),
                Column('descripcion', String(250)),
                Column('hora_inicial', Time()),
                Column('fec_inicial', Date()),
                Column('estado', String(10))
                )

estado_conexion = Table('estado_conexion', metadata,
                Column('id', Integer, primary_key = True),
                Column('estado', String(20)),
                Column('CCM', String(20))
                )

#Estructura de la tabla todo
temporal = Table('temporal', metadata,
             Column('id', Integer, primary_key=True),
             Column('temperatura', Float()),
             Column('humedad', Float()),
             Column('canal1', Float()),
             Column('canal2', Float()),
             Column('canal3', Float()),
             Column('canal4', Float()),
             Column('tempGabinete', Float()),
             Column('hora', Time()),
             Column('fecha', Date()),
             )

while True:
    #Se crea un objeto con la conexion a la base de datos de MariaDB
    engine = create_engine('mysql+pymysql://javi:javiersolis12@10.0.0.20/Tuti')
    connection = engine.connect()
    #Seleccionamos los datos de la tabla configurar
    query_init = select([configuracion])
    resultado = connection.execute(query_init).fetchone()
    #Direccion de la base de datos para la conexion de SQLAlchemy
    url = 'mysql+pymysql://javi:javiersolis12@' + resultado.ip + '/tuti'
    #Si exite conexion con la CCM
    if (resultado.checkbox == "con CCM"):
        try:
            engine2 = create_engine(url)
            connection2 = engine2.connect()
            query_conexion_CCM = update(estado_conexion).where(estado_conexion.c.id==1).values(CCM = 'con conexion')
            connection.execute(query_conexion_CCM)
        except exc.SQLAlchemyError:
            query_conexion_CCM = update(estado_conexion).where(estado_conexion.c.id==1).values(CCM = 'sin conexion')
            connection.execute(query_conexion_CCM)
    metadata.create_all(engine)
    #Seleccionara la unica entrada en la tabla Configuracion
    query = select([configuracion])
    confi_actual = connection.execute(query).fetchone()
    #Seleccionar la primera entrada de la tabla ahora
    query_aux = select([ahora])
    datos_actuales = connection.execute(query_aux).fetchone()
    #Selecciona la primera entrada de la tabla estado_conexion
    query_estado_conexion_select = select([estado_conexion])
    resultado_query_estado_conexion_select = connection.execute(query_estado_conexion_select).fetchone()
    #Selecciona todos los datos de la tabla alarmas
    query_alarmas_select = select([alarmas])
    res_query_alarmas_select = connection.execute(query_alarmas_select).fetchall()
    #Busca en el conjunto de datos si existe el id
    estado_conexion_SMR = buscarId(res_query_alarmas_select,7)
    estado_conexion_DHT = buscarId(res_query_alarmas_select,1)
    #Condición, se guardara la informacion únicamente cada x tiempo, según el dato frecuencia dentro de la tabla configuracion
    if ( int(time.strftime("%M")) % int(confi_actual.frec) == 0 and int(time.strftime("%S")) == 00 and estado_conexion_SMR.estado == 'inactivo' and estado_conexion_DHT.estado == 'inactivo'):
        aux = todo.insert().values(temperatura = datos_actuales.temperatura,
                            humedad = datos_actuales.humedad,
                            canal1 = datos_actuales.canal1,
                            canal2 = datos_actuales.canal2,
                            canal3 = datos_actuales.canal3,
                            canal4 = datos_actuales.canal4,
                            tempGabinete = datos_actuales.tempGabinete,
                            hora = time.strftime("%H:%M:%S"),
                            fecha = time.strftime("%Y/%m/%d"))
        connection.execute(aux)
        if (resultado.checkbox == "con CCM" and resultado_query_estado_conexion_select.CCM == 'con conexion'):
            query_temporal_select = select([temporal])
            resp_query_temporal_select = connection.execute(query_temporal_select).fetchall()
            if (len(resp_query_temporal_select) > 0):
                for valor in resp_query_temporal_select:
                    aux_temporal = todo.insert().values(temperatura = valor.temperatura,
                                                    humedad = valor.humedad,
                                                    canal1 = valor.canal1,
                                                    canal2 = valor.canal2,
                                                    canal3 = valor.canal3,
                                                    canal4 = valor.canal4,
                                                    tempGabinete = datos_actuales.tempGabinete,
                                                    hora = valor.hora,
                                                    fecha = valor.fecha)                
                    connection2.execute(aux_temporal)
                connection.execute(sa_text('''TRUNCATE TABLE temporal''').execution_options(autocommit=True))
            connection2.execute(aux)
        if (resultado.checkbox == "con CCM" and resultado_query_estado_conexion_select.CCM == 'sin conexion'):
            aux = temporal.insert().values(temperatura = datos_actuales.temperatura,
                            humedad = datos_actuales.humedad,
                            canal1 = datos_actuales.canal1,
                            canal2 = datos_actuales.canal2,
                            canal3 = datos_actuales.canal3,
                            canal4 = datos_actuales.canal4,
                            tempGabinete = datos_actuales.tempGabinete,
                            hora = time.strftime("%H:%M:%S"),
                            fecha = time.strftime("%Y-%m-%d"))
            connection.execute(aux)
        time.sleep(1)

    #Evento programado cada 5 segundos
    if ( int(time.strftime("%S")) % 5 == 0):
        rango_potencia = dentroRango(confi_actual.potmax, confi_actual.potmin, datos_actuales.canal1)
        rango_temperatura = dentroRango(confi_actual.tempmax, confi_actual.tempmin, datos_actuales.temperatura)
        #Si esta dentro del rango poner inactivo los dos errores 0xE1002 & 0xE1003
        if (rango_potencia == 0):
            query_alarmas_update = update(alarmas).where(alarmas.c.id==2).values(estado = 'inactivo')
            connection.execute(query_alarmas_update)
            query_alarmas_update1 = update(alarmas).where(alarmas.c.id==3).values(estado = 'inactivo')
            connection.execute(query_alarmas_update1)
        #Si el rango de Potencia supera el umbral cambia el valor de estado a activo
        if (rango_potencia == 1):
            query_alarmas_update3 = update(alarmas).where(alarmas.c.id==2).values(
                                                                                    estado = 'activo',
                                                                                    hora_inicial = time.strftime("%H:%M:%S"),
                                                                                    fec_inicial = time.strftime("%Y-%m-%d"))
            connection.execute(query_alarmas_update3)
        #Si el rango de potencia es inferior cambia de estado activo
        if (rango_potencia == -1):
            query_alarmas_update4 = update(alarmas).where(alarmas.c.id==3).values(
                                                                                    estado = 'activo',
                                                                                    hora_inicial = time.strftime("%H:%M:%S"),
                                                                                    fec_inicial = time.strftime("%Y-%m-%d"))
            connection.execute(query_alarmas_update4)

        #Si conexion a la SMR falla
        if (resultado_query_estado_conexion_select.estado == 'sin conexion'):
            query_update_alarma_SMR = update(alarmas).where(alarmas.c.id==7).values(
                                                                                    estado = 'activo',
                                                                                    hora_inicial = time.strftime("%H:%M:%S"),
                                                                                    fec_inicial = time.strftime("%Y-%m-%d"))
            connection.execute(query_update_alarma_SMR)

        #Si la conexion SMR esta Online
        if (resultado_query_estado_conexion_select.estado == 'con conexion'):
            query_update_alarma_SMR = update(alarmas).where(alarmas.c.id==7).values(estado = 'inactivo')
            connection.execute(query_update_alarma_SMR)

        #Si la conexion hacia la CCM falla
        if (resultado_query_estado_conexion_select.CCM == 'sin conexion' and resultado.checkbox == "con CCM"):
            query_update_alarma_CCM = update(alarmas).where(alarmas.c.id==8).values(
                                                                                    estado = 'activo',
                                                                                    hora_inicial = time.strftime("%H:%M:%S"),
                                                                                    fec_inicial = time.strftime("%Y-%m-%d"))
            connection.execute(query_update_alarma_CCM)  

        #Si la conexion con el CCM se mantiene   
        if (resultado_query_estado_conexion_select.CCM == 'con conexion' and resultado.checkbox == "con CCM"):
            query_update_alarma_CCM = update(alarmas).where(alarmas.c.id==8).values(estado = 'inactivo')
            connection.execute(query_update_alarma_CCM)      
        
        #Si la conexion a la CCM esta Online
        if (resultado.checkbox == "con CCM" and resultado_query_estado_conexion_select.CCM == 'con conexion'):
            aux_ahora = update(ahora).where(ahora.c.id==1).values(
                                temperatura = datos_actuales.temperatura,
                                humedad = datos_actuales.humedad,
                                canal1 = datos_actuales.canal1,
                                canal2 = datos_actuales.canal2,
                                canal3 = datos_actuales.canal3,
                                canal4 = datos_actuales.canal4,
                                tempGabinete = datos_actuales.tempGabinete,
                                hora = time.strftime("%H:%M:%S"))
            connection2.execute(aux_ahora)
            
        #Si el sensor DHT11 falla
        if (datos_actuales.temperatura == 1000 or datos_actuales.humedad == 1000):
            query_update_alarma_DHT = update(alarmas).where(alarmas.c.id==1).values(
                                                                                    estado = 'activo',
                                                                                    hora_inicial = time.strftime("%H:%M:%S"),
                                                                                    fec_inicial = time.strftime("%Y-%m-%d"))
            connection.execute(query_update_alarma_DHT)
        else:
            query_update_alarma_DHT = update(alarmas).where(alarmas.c.id==1).values(
                                                                                    estado = 'inactivo',
                                                                                    hora_inicial = time.strftime("%H:%M:%S"),
                                                                                    fec_inicial = time.strftime("%Y-%m-%d"))
            connection.execute(query_update_alarma_DHT)

        #Fallo temperatura Gabinete
        if (datos_actuales.tempGabinete >= confi_actual.tempmax):
            query_update_alarma_tempmax = update(alarmas).where(alarmas.c.id==5).values(
                                                                                    estado = 'activo',
                                                                                    hora_inicial = time.strftime("%H:%M:%S"),
                                                                                    fec_inicial = time.strftime("%Y-%m-%d"))
            connection.execute(query_update_alarma_tempmax)

        if (datos_actuales.tempGabinete <= confi_actual.tempmin):
            query_update_alarma_tempmin = update(alarmas).where(alarmas.c.id==6).values(
                                                                                    estado = 'activo',
                                                                                    hora_inicial = time.strftime("%H:%M:%S"),
                                                                                    fec_inicial = time.strftime("%Y-%m-%d"))
            connection.execute(query_update_alarma_tempmin) 

        if (datos_actuales.tempGabinete > confi_actual.tempmin and datos_actuales.tempGabinete < confi_actual.tempmax):
            query_update_alarma_aux = update(alarmas).where(alarmas.c.id==5).values(estado = 'inactivo',)
            connection.execute(query_update_alarma_aux)
            query_update_alarma_aux = update(alarmas).where(alarmas.c.id==6).values(estado = 'inactivo',)
            connection.execute(query_update_alarma_aux)    

        time.sleep(0.4)
    connection.close()
    if (resultado_query_estado_conexion_select.CCM == 'con conexion'):
        connection2.close()
    time.sleep(0.5)