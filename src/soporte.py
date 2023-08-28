import requests
import pandas as pd
import numpy as np
import mysql.connector
from geopy.geocoders import Nominatim

class Extraccion: 

    def __init__(self):
        pass
    
    def invocar_API(self,pais):
        self.pais = pais
        url = "http://universities.hipolabs.com/search"
        paises = {'country': pais}
        response = requests.get(url=url, params=paises)
        cod = response.status_code
        razon = response.reason
        if cod == 200:
            print('La peticion se ha realizado correctamente, se ha devuelto el código de estado:',cod,' y como razón del código de estado: ',razon)
        elif cod == 402:
            print('No se ha podido autorizar usario, se ha devuelto el código de estado:', cod,' y como razón del código de estado: ',razon)
        elif cod == 404:
            print('Algo ha salido mal, el recurso no se ha encontrado,se ha devuelto el código de estado:', cod,' y como razón del código de estado: ',razon)
        else:
            print('Algo inesperado ha ocurrido, se ha devuelto el código de estado:', cod,' y como razón del código de estado: ',razon)
        df = pd.json_normalize(response.json())
        return df

    def unir_df(self,df1,df2,df3):
         df = pd.concat([df1, df2, df3], axis = 0)
         return df
    
    def limpieza(self,df,states):

        columnas =[]
        for col in df.columns:
            col1 = col.replace("-","_")
            columnas.append(col1)

        print(columnas)
        df.columns=columnas
        df.drop(['domains'], axis=1, inplace= True)
        df = df.explode('web_pages')
        print("Examinamos si tenemos nombres duplicados")
        print(df[df.duplicated(subset=['name'])])
        df = df.drop_duplicates(subset=['name'])
        df['state_province'] = df['state_province'].map(states)
        return df
    
    def nulos_nan(self,df):
        df = df.fillna(value=np.nan)
        print("Comprobamos si nos reconoce los valores nulos que hemos cambiado")
        print(df.isnull().sum())
        return df
    def unknown(self, df):
        df['state_province']=df['state_province'].replace(np.nan,'Unknown')
        print("Comprobamos que no queda ningún nulo")
        print(df.isnull().sum())
        return df
    def ubicaciones(self,df):
        print('Creamos el dataframe con las localidades')
        statesc = pd.Series(df['state_province'].unique(), name="state_province")
        locations = statesc.to_frame()
        print(locations)
        geolocator = Nominatim(user_agent="specify_your_state")
        locations["latitud"] = locations["state_province"].apply(lambda x: geolocator.geocode(x).latitude)
        locations["longitud"] = locations["state_province"].apply(lambda x: geolocator.geocode(x).longitude)
        print("Comprobamos el df final con la latitud y la longitud")
        print(locations)
        return locations

    def crear_bbdd_ejercicio(self,nombre_bbdd):
        mydb = mysql.connector.connect(host="localhost", user="root", password="AlumnaAdalab",
        auth_plugin = 'mysql_native_password')
        print("Conexion realizada con éxito")

        mycursor = mydb.cursor()

        try:
            mycursor.execute(f'CREATE DATABASE IF NOT EXISTS {nombre_bbdd}')
            print(mycursor)
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg) 

    def crear_insertar_tabla(self,nombre_bbdd, contraseña, query):
    
        cnx = mysql.connector.connect(user='root', password=f"{contraseña}",
                                        host='127.0.0.1', database=f"{nombre_bbdd}",  
                                        auth_plugin = 'mysql_native_password')
        mycursor = cnx.cursor()
        
        try: 
            mycursor.execute(query)
            cnx.commit() 
        except mysql.connector.Error as err:
            print(err)
            print("Error Code:", err.errno)
            print("SQLSTATE", err.sqlstate)
            print("Message", err.msg)
