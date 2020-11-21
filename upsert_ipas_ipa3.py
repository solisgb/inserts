# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 21:00:03 2019
@author: solis
insert or update data in table ipa2. from a csv file
"""
import pandas as pd
import traceback
from db_connection import con_get
import littleLogging as logging


class Upsert_ipas_ipa3():

    column_names = {'cod': None, 'fecha': None, 'caudal_ls': None,
                    'error': None, 'situacion': None, 'proyecto': None,
                    'medidor': None}


    def __init__(self, fi: str, col_names: dict, sep: str=';',
                 sheet_name: str=None, converters: dict={},
                 lowercase: tuple=()):
        """
        args:
            fi: path to csv, xls or xlsx file
            col_names: diccionario en el que las claves son los nombres de
                las columnas en la tabla en que se van a importar los datos y
                los contenidos son los correspondientes nombres de las
                columnas en fi
            si fi tiene extensión csv
            sep: column separatos in fi (sólo para csv)
            si fi tiene extensión Excel
            sheet_name: nombre de la hoja en fi
            converters: al leer un fichero xls o xlsx deduce el tipo de los
                contenidos de las columnas; si se desea forzar un tipo utilice
                converters, que es un diccionario cuya clave es el nombre de
                la columna en el fichero xls o xlsx que se desea contertir
                el tipo y el contenido es el tipo de python al que se desea
                convertir
            lowercase: column names in sheet_name to transform to lowercase
        """
        from os.path import splitext
        self.fi = fi
        self.col_names = Upsert_ipas_ipa3.column_names.copy()
        for k in self.col_names.keys():
            self.col_names[k] = col_names[k]
        self.sep = sep
        self.checked_data_in_tables = False
        if splitext(fi)[1] == '.csv':
            self.data = pd.read_csv(self.fi, sep=self.sep)
        else:
            self.data = pd.read_excel(fi, sheet_name=sheet_name,
                                      converters=converters)
            if lowercase is not None:
                for col in lowercase:
                    self.data[col] = self.data[col].str.lower()
            self.data = self.data.where(pd.notnull(self.data), None)
        self.con = con_get('postgres', 'ipa')


    @property
    def cod(self):
        return self.col_names['cod']


    @property
    def fecha(self):
        return self.col_names['fecha']


    @property
    def caudal_ls(self):
        return self.col_names['caudal_ls']


    @property
    def error(self):
        return self.col_names['error']


    @property
    def situacion(self):
        return self.col_names['situacion']


    @property
    def proyecto(self):
        return self.col_names['proyecto']


    @property
    def medidor(self):
        return self.col_names['medidor']


    def check_data_in_tables(self) ->bool:
        """
        chequea que los contenidos de determinadas columnas están en tablas
            codificadas
        """

        # columna códigos en toma
        to_search = set(self.data[self.cod])

        s0 = """select cod from ipas.ipa1 where cod=%s"""
        not_found = \
        self.__data_in_table(to_search, s0, 'ipas.ipa1')
        if not_found:
            return False

        # columna situacion
        s0 = "select cod from ipas.ipa3_situacion where cod = %s"
        to_search = set(self.data[self.situacion])
        not_found = self.__data_in_table(to_search, s0,
                                         'ipas.ipa3_situacion.cod')
        if not_found:
            return False

        # columna medidor
        s0 = "select codigo from ipas.medidor where codigo = %s"
        to_search = set(self.data[self.medidor])
        not_found = self.__data_in_table(to_search, s0,
                                         'ipas.medidor.codigo')
        if not_found:
            return False

        # columna proyecto
        s0 = "select cod from ipas.proyectos where cod = %s"
        to_search = set(self.data[self.proyecto])
        not_found = self.__data_in_table(to_search, s0,
                                         'public.proyectos.cod')
        if not_found:
            return False

        self.checked_data_in_tables = True
        return True


    def print_min_max(self, columns: list):
        """
        imprime los valores mínimo y máximo en columns
        args:
            nombre de las columns en self.fi en las que se desea mostrar
            los valores mínimo y máximo
        """
        for column1 in columns:
            items = self.data[column1]
            print(f'Rango en {column1}: {min(items)} - {max(items)}')


    def __data_in_table(self, items: list, s0: str, table_column: str) ->list:
        """
        executes select s0 for each element in items
        args:
            items: elements to search
            s0: select qith one parameter
            table_column: schema.table.column in witch items must exists
        """
        cur = self.con.cursor()
        not_found = []
        for item in items:
            cur.execute(s0,(item,))
            row = cur.fetchone()
            if row is None:
                not_found.append(item)
        if len(not_found) > 0:
            print(f'No existen {len(not_found):n}/{len(items):n} ' +
            f'en la tabla {table_column}')
            for item in not_found:
                print(item)
        else:
            print(f'Todos los contenidos están en {table_column}')
        return not_found


    def upsert(self, update: bool=True, check: bool=True) ->None:
        """
        args:
            update: si ==True hace un update cuando la fila ya existe
            check: si True chekea los datos antes de insert-update
        inserta fila a fila
            si existe ya la fila con la clave primaria que se desea insertar
            actualiza sus contenidos en el caso de que update == True, en caso
            contrario no hace nada
        la operación se hace fila a fila y se contabiliza el número de
            insertados y el de actualizados, por lo que no se utiliza la
            sentencia upsert de postgres
        """
        s0 = "select cod from ipas.ipa1 where cod=%s"
        s1 = 'select cod from ipas.ipa3 where cod=%s and fecha=%s'
        insert = \
        """
        insert into ipas.ipa3 (cod, fecha, caudal_ls, error, situacion,
                               proyecto, medidor)
        values (%s, %s, %s, %s, %s, %s, %s);
        """
        update = \
        """
        update ipas.ipa3
        set caudal_ls=%s, error=%s, situacion=%s, proyecto=%s, medidor=%s
        where cod=%s and fecha=%s
        """

        if check and not self.checked_data_in_tables:
            ok = self.check_data_in_tables()
            if not ok:
                return

        cur = self.con.cursor()
        nrows = self.data.shape[0] - 1
        not_found = inserted = updated = 0
        for index, row in self.data.iterrows():
            print(f'{index}/{nrows}')
            cur.execute(s0, (row[self.cod],))
            row1 = cur.fetchone()
            if row1 is None:
                logging.append(f"{index} {row[self.cod]} not found", False)
                not_found += 1
                continue
            cur.execute(s1, (row[self.cod], row[self.fecha]))
            row1 = cur.fetchone()

            if row1 is None:
                try:
                    cur.execute(insert,
                                (row[self.cod], row[self.fecha],
                                 row[self.caudal_ls], row[self.error],
                                 row[self.situacion],
                                 row[self.proyecto], row[self.medidor]))
                    logging.append(f"{index:n} {row[self.cod]} " \
                                   f"{row[self.fecha]} inserted", False)
                    inserted += 1
                except:
                    msg = traceback.format_exc()
                    logging.append(f"{index:n} {row[self.cod]} " \
                                   f"{row[self.fecha]} error " +\
                                   f'inserting\n{msg}')
                    return
            else:
                if not update:
                    logging.append(f"{index:n} {row[self.cod]} " \
                                   f"{row[self.fecha]} not updated", False)
                    continue
                try:
                    cur.execute(update,
                                (row[self.caudal_ls], row[self.error],
                                 row[self.situacion], row[self.proyecto],
                                 row[self.medidor],
                                 row[self.cod], row[self.fecha]))
                    logging.append(f"{index:n} {row[self.cod]} " \
                                   f"{row['fecha']} updated", False)
                    updated += 1
                except:
                    msg = traceback.format_exc()
                    logging.append(f"{index:n} {row[self.cod]} " \
                                   f"{row[self.fecha]} error " +\
                                   f'updating\n{msg}')
                    return
        self.con.commit()
        print(f'cod not found: {not_found:n}')
        print(f'inserted: {inserted:n}')
        print(f'updated: {updated}')
