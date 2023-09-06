import src.soporte as sp
import src.soporte_variables as var
import requests
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
pd.options.display.max_columns = None

etl = sp.Extraccion()

dfc = etl.invocar_API('Canada')
dfa = etl.invocar_API('Argentina')
dfeeuu = etl.invocar_API('United States')

df = etl.unir_df(dfc,dfa,dfeeuu)
df = etl.limpieza(df, var.states)
df = etl.nulos_nan(df)
df = etl.unknown(df)
locations = etl.ubicaciones(df)
print("Unimos los dataframes para tener ya el definitivo")
df_final = df.merge(locations, how='inner', on='state_province')
print(df_final.columns)
#Llevar esta linea al método ubicación
print(df_final)
print('Una vez listo nuestro dataframe creamos la BBDD')
db = etl.crear_bbdd_ejercicio('univ', 'AlumnaAdalab')
print('Una vez creada la BBDD definimos las tablas')
#querys de tablas a soporte variables
tabla_países = '''
                CREATE TABLE IF NOT EXISTS `univ`.`paises`
                (idestado INT NOT NULL AUTO_INCREMENT,
                 nombre_pais VARCHAR(45) NOT NULL,
                 nombre_provincia VARCHAR(45) NOT NULL,
                 latitud FLOAT NOT NULL,
                 longitud FLOAT NOT NULL,
                 PRIMARY KEY (`idestado`))
                 ENGINE = InnoDB;'''

tabla_universidades = '''
                CREATE TABLE IF NOT EXISTS `univ`.`universidades`
                ( iduniversidades INT NOT NULL AUTO_INCREMENT,
                 nombre_universidad VARCHAR(45) NOT NULL,
                 pagina_web VARCHAR(45) NOT NULL,
                 paises_idestado INT NOT NULL,
                 PRIMARY KEY (`iduniversidades`),
                 CONSTRAINT `fk_idestado`
                    FOREIGN KEY (`paises_idestado`)
                    REFERENCES `univ`.`paises`(`idestado`))
                ENGINE = InnoDB;'''

print('Y las insertamos en nuestra BBDD')
etl.crear_insertar_tabla("univ","AlumnaAdalab", tabla_países)
etl.crear_insertar_tabla("univ","AlumnaAdalab", tabla_universidades)
print('Por último procedemos a la carga de datos')
#Carga tabla paises
for indice, fila in df_final.iterrows():

    query_provincia = f"""
                INSERT INTO univ.paises (nombre_pais, nombre_provincia,latitud, longitud ) 
                VALUES ( "{fila["country"]}", "{fila["state_province"]}", "{fila['latitud']}", "{fila['longitud']}");
                """
    provincias = etl.check_provincias()

    if len(provincias) == 0 or fila['state_province'] not in provincias[0]:
        etl.crear_insertar_tabla('univ', 'AlumnaAdalab', query_provincia)

    else:
        print(f"{fila['state_province']} ya esta en nuestra BBDD")
    #Carga tabla universidades
    for indice, fila in df_final.iterrows():
        idestado = etl.sacar_id_estado(fila['state_province'], 'AlumnaAdalab')
        if idestado == None:
            pass
        else:
            query_universidades = f"""
                    INSERT INTO universidades (nombre_universidad, pagina_web,paises_idestado) 
                    VALUES ( "{fila["name"]}", "{fila["web_pages"]}", {idestado},);
                    """
            etl.crear_insertar_tabla('univ', 'AlumnaAdalab', query_universidades)
