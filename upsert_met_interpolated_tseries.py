# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 21:00:03 2019
@author: solis
upsert data in table met.interpolated_tseries
"""
from os.path import splitext
import pandas as pd
import traceback

from db_connection import con_get
import littleLogging as logging


class Upsert_met_interpolated_tseries():

    column_names = {'fid': None, 'variable': None, 'fecha': None,
                    'value': None, 'metodo': None}


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
        self.fi = fi
        self.col_names = Upsert_met_interpolated_tseries.column_names.copy()
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
    def fid(self):
        return self.col_names['fid']


    @property
    def variable(self):
        return self.col_names['variable']


    @property
    def fecha(self):
        return self.col_names['fecha']


    @property
    def value(self):
        return self.col_names['value']


    @property
    def metodo(self):
        return self.col_names['metodo']


    def check_data_in_tables(self) ->None:
        """
        chequea que los contenidos de las columnas en el fichero con los
            datos a importar para comprobar que el valor es un código válido
            en la columna de la tabla donde se va a insertar, que tiene una
            restricción de clave foránea
        Las columnas a chequear se especifican en columns_2_test, ({},..), en
            que cada {} tiene 3 claves:
                col_data: nombre de la columna a chequear en self.fi
                table: nombre de la tabla donde se va a insertar
                col: nombre de la columna de la tabla
        """

        columns_2_test = \
        (
         {'col_data': self.fid,
          'table': 'met.interpolation_points',
          'col': 'fid'},
         {'col_data': self.variable,
          'table': 'met.interpolation_variable',
          'col': 'fid'},
         {'col_data': self.metodo,
          'table': 'met.interpolation_metodo',
          'col': 'fid'},
        )

        for item in columns_2_test:
            select = f"select {item['col']} from {item['table']}" \
            f" where {item['col']}=%s;"
            to_search = set(self.data[item['col_data']])
            text = f"{item['table']}.{item['col']}"
            not_found = \
            self.__data_in_table(to_search, select, text)
            if not_found:
                return False

        self.checked_data_in_tables = True
        return True


    def print_min_max(self, columns: list):
        """
        imprime los valores mínimo y máximo en columns
        """
        for column1 in columns:
            col = self.data[column1]
            print(f'Rango en {column1}: {min(col)} - {max(col)}')


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
            row = cur.fetchone()
            if row is None:
                not_found.append(item)
        if not_found:
            print(f'No existen {len(not_found):n}/{len(items):n}' +
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
        s0 = "select fid from met.interpolation_points where fid = %s"
        s1 = \
        """
        select fid
        from met.interpolated_tseries
        where fid=%s and variable=%s and fecha=%s"""
        insert = \
        """
        insert into met.interpolated_tseries
            (fid, variable, fecha, value, metodo)
        values (%s, %s, %s, %s, %s);
        """
        update = \
        """
        update met.interpolated_tseries
        set value=%s, metodo=%s
        where fid=%s and variable=%s and fecha=%s
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
            cur.execute(s0, (row[self.fid],))
            row1 = cur.fetchone()
            if row1 is None:
                logging.append(f"{index} {row[self.fid]} not found")
                not_found += 1
                continue
            cur.execute(s1, (row[self.fid], row[self.variable],
                             row[self.fecha]))
            row1 = cur.fetchone()

            msg_key_columns = f"{index:n} {row[self.fid]} " \
            f"{row[self.variable]} {row[self.value]}"
            if row1 is None:
                try:
                    cur.execute(insert,
                                (row[self.fid], row[self.variable],
                                 row[self.fecha],
                                 row[self.value], row[self.metodo]))
                    logging.append(f"{msg_key_columns} inserted", False)
                    inserted += 1
                except:
                    msg = traceback.format_exc()
                    logging.append(f"{msg_key_columns} error inserting " \
                                   f'\n{msg}')
                    return
            else:
                if not update:
                    logging.append(f"{msg_key_columns} not updated", False)
                    continue
                try:
                    cur.execute(update,
                                (row[self.value], row[self.metodo],
                                 row[self.fid], row[self.variable],
                                 row[self.fecha]))
                    logging.append(f"{msg_key_columns} updated", False)
                    updated += 1
                except:
                    msg = traceback.format_exc()
                    logging.append(f"{msg_key_columns} error updating " \
                                   f'\n{msg}')
                    return
        self.con.commit()
        print(f'cod not found: {not_found:n}')
        print(f'inserted: {inserted:n}')
        print(f'updated: {updated}')
