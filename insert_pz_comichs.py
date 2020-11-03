# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 21:00:03 2019
@author: solis
insert data in table ipa2 from a csv file
"""
#import csv
import pandas as pd
from db_connection import con_get
import littleLogging as logging


class InsertPzComisaria():

    column_names = {'miteco': None, 'fecha': None, 'pnp': None,
                    'situacion': None, 'instalado': None, 'tuboguia': None,
                    'proyecto': None}


    def __init__(self, fi: str, col_names: dict, red: str, sep: str=';'):
        """
        args:
            fi: path to csv file
            col_names: column names in fi; all these names must exist
            sep: column separatos in fi
        """
        self.fi = fi
        self.col_names = InsertPzComisaria.column_names
        for k in self.col_names.keys():
            self.col_names[k] = col_names[k]
        self.red = red
        self.sep = sep
        self.checked_data_in_tables = False
        self.checked_booleans = False
        self.data = pd.read_csv(self.fi, sep=self.sep)
        self.con = con_get('postgres', 'ipa')


    def check_data_in_tables(self) ->None:
        """
        chequea que los contenidos de determinadas columnas están en tablas
            codificadas
        """

        # columna red control
        items = [self.red,]
        s0 = "select red from ipas.red_control where red = %s"
        not_found = self.__data_in_table(items, s0, 'ipas.red_control.red')
        if not_found:
            return False

        # columna códigos miteco
        mitecos = set(self.data[self.col_names['miteco']])
        s0 = """select cod from ipas.ipa1_red_control
        where cod_red = %s and red = """ + f"'{self.red}'"
        not_found = \
        self.__data_in_table(mitecos, s0, 'ipas.ipa1_red_control.cod_red')
        if not_found:
            return False

        # columna situacion
        s0 = "select cod from ipas.fsituaci where cod = %s"
        situacion = set(self.data[self.col_names['situacion']].str.lower())
        not_found = self.__data_in_table(situacion, s0,
                                         'ipas.fsituaci.cod')
        if not_found:
            return False

        # columna proyecto
        s0 = "select cod from ipas.fproyect where cod = %s"
        situacion = set(self.data[self.col_names['proyecto']])
        not_found = self.__data_in_table(situacion, s0,
                                         'ipas.fproject.cod')
        if not_found:
            return False

        self.checked_data_in_tables = True
        return True


    def check_booleans(self):
        """
        chequea columnas lógicas (bbol) en csv file
        """

        # columna instalado
        items = set(self.data[self.col_names['instalado']])
        not_found = self.__are_booleans(items, 'csv column instalado')
        if not_found:
            return False

        # columna tuboguia
        items = set(self.data[self.col_names['tuboguia']])
        not_found = self.__are_booleans(items, 'csv column tuboguia')
        if not_found:
            return False

        self.checked_booleans = True
        return True


    def print_min_max(self, columns: list):
        """
        imprime los valores mínimo y máximo en columns
        """
        for column1 in columns:
            items = self.data[self.col_names[column1]]
            print(f'Rango en {column1}: {min(items)} - {max(items)}')


    def __data_in_table(self, items: list, s0: str, table_column: str) ->list:
        """
        check column content
        args:
            items: elements to check
            s0: select
            table_column: schema.table.column in witch items must exists
        """
        cur = self.con.cursor()
        not_found = []
        for item in items:
            cur.execute(s0,(item,))
            row = cur.fetchone()[0]
            if row is None:
                not_found.append(item)
        if not_found:
            print(f'No existen {len(not_found):n}/{len(items):n)}' +
            f'en la tabla {table_column}')
            for item in not_found:
                print(item)
        else:
            print(f'Todos los contenidos están en {table_column}')
        return not_found


    def __are_booleans(self, items: list, table_column: str) ->list:
        """
        chequea columnas lógicas (bbol) en csv file
        """

        not_found = [item for item in items
                     if item.lower() not in ('y', 's', 'n', 1, 0, None)]
        if not_found:
            print(f'Hay {len(not_found):n}/{len(items):n)}' +
            f'no booleanos en {table_column}')
            for item in not_found:
                print(item)
        else:
            print(f'Todos los contenidos en {table_column} son booleanos')
        return not_found


    def upsert(self, update: bool=True):
        """
        inserta fila a fila
            si existe ya la fila con la clave primaria que se desea insertar
            actualiza sus contenidos en el caso de que update == True, en caso
            contrario no hace nada
        """
        s0 = """select cod from ipas.ipa1_red_control
        where cod_red = %s and red = """ + f"'{self.red}'"

        insert = \
        """
        insert into ipas.ipa2 (cod, fecha, situacion, instalado,
                               tuboguia, proyecto, pnp_original)
        values (%s, %s, %s, %s, %s, %s, %s);
        """
        update = \
        """
        update ipas.ipa2
        set situacion, instalado, tuboguia, proyecto, pnp_original
        where cod=%s and fecha=%s
        """

        if not self.checked_data_in_tables:
            ok = self.check_data_in_tables()
            if not ok:
                return

        if not self.checked_booleans:
            ok = self.check_booleans()
            if not ok:
                return

        cur = self.con.cursor()
        for index, row in self.data.iterrows():
            cur.execute(s0, (row['miteco'],))
            cod = cur.fetchone()[0]
            if cod is not None:
                logging.append(f"{row['miteco']} not found", toSreen=False)
                continue
            try:
                cur.execute(insert, (cod, row['fecha'], row['situacion'],
                                     row['instalado'], row['tuboguia'],
                                     row['proyecto'], row['pnp'] ))
                logging.append(f"{row['miteco']} inserted", toSreen=False)
            except:
                if not update:
                    continue






