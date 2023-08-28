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
print(locations)
print("Unimos los dataframes para tener ya el definitivo")
df.merge(locations, how='inner', on='state_province')
print(df)
print('Una vez listo nuestro dataframe creamos la BBDD')
db = etl.crear_bbdd_ejercicio('universidades')
print('Una vez creada la BBDD definimos las tablas')
tabla_países = '''
                CREATE TABLE IF NOT EXISTS `univ`.`paises`
                ( idestado INT NOT NULL AUTO_INCREMENT,
                 nombre_pais VARCHAR(45) NOT NULL,
                 nombre_provincia VARCHAR(45) NOT NULL,
                 latitud FLOAT NOT NULL,
                 longitud FLOAT NOT NULL,
                 PRIMARY KEY (`idestado`))'''

tabla_universidades = '''
                CREATE TABLE IF NOT EXISTS `univ`.`universidades`
                ( iduniversidades INT NOT NULL AUTO_INCREMENT,
                 nombre_universidad VARCHAR(45) NOT NULL,
                 pagina_web VARCHAR(45) NOT NULL,
                 paises_idestado INT NOT NULL,
                 PRIMARY KEY (`iduniversidades`))
                 CONSTRAINT `fk_idestado`
                                FOREIGN KEY (`paises_idestado`)
                                REFERENCES `univ`.`paises`(`idestado`)'''

print('Y las insertamos en nuestra BBDD')
etl.crear_insertar_tabla("clima","AlumnaAdalab", tabla_países)
etl.crear_insertar_tabla("clima","AlumnaAdalab", tabla_universidades)