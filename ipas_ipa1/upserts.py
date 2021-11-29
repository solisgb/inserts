# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 21:00:03 2019
@author: solis

inserts or update table with values in an xls or xlsm file
"""
from os.path import splitext
import pandas as pd
import traceback
import xml.etree.ElementTree as ET

from db_connection import con_get
import littleLogging as logging


class Upsert():

    xml_org = 'upserts.xml'

    def __init__(self, db: str, schema: str, table: str,
                 fi: str, sheet_name: str, converters: dict={},
                 lowercase: tuple=(), check_data: bool=True):
        """
        args:
            db: database
            schema: schema
            table: table
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
        tree = ET.parse(self.xml_org)
        root = tree.getroot()
        db_element = self.__read_atrib_element(root, 'db', 'name', db)
        self.db = db
        self.table = self.__read_atrib_element(db_element, 'table', 'name',
                                               f'{schema}.{table}')

        self.fi = fi
        self.data = pd.read_excel(fi, sheet_name=sheet_name,
                                  converters=converters)

        if check_data:
            self.__check_fks()

        if lowercase is not None:
            for col in lowercase:
                self.data[col] = self.data[col].str.lower()
        self.data = self.data.where(pd.notnull(self.data), None)
        self.con = con_get('postgres', 'ipa')
        self.cur = self.con.cursor()


    def __read_atrib_element(self, root, element_name: str, attrib_name: str,
                             attrib_value: str):
        """
        root: element
        """
        efound = None
        for element in root.findall(element_name):
            if element.get(attrib_name) == attrib_value:
                efound = element
                break
        if not efound:
            raise ValueError(f'Element {element_name} attribute {attrib_name}'\
                             ' {attrib_name} not found in {self.xml_org}')
        return efound


    def __check_df_column_names(self):
        """
        column names in dataframes must exists as column names in table
            to be upserted
        """
        cols_df = [col1 for col1 in self.data.columns]
        cols_table = [col1[0] for col1 in self.__table_column_get()]
        not_found = []
        for col1 in cols_df:
            if col1 not in cols_table:
                not_found.append(col1)
        return not_found


    def __table_column_get(self) ->list:
        """
        returns a list with the name and type columns in the table to be
            upserted
        """
        s1 = \
        f"""
        select column_name, data_type
        from information_schema.columns
        where table_catalog={self.db} and table_schema ={self.schema}
            and table_name = {self.table}
        order by ordinal_position
        ;
        """
        self.cur.execute(s1)
        columns = [row for row in self.cur.fetchall()]
        return columns


    def __check_fks(self):
        """
        tests if foreign key columns values if file are in foregn keys
        """
        fk_errors = 0
        tcols = self.__table_column_get()
        names_table = [col1 for col1 in tcols]
        names_df = [col1 for col1 in self.data.columns]
        efks = self.table.findall('fk')
        for element in efks:
            # query column names
            q_cols = element.find('col').text.split(',')
            q_cols = [col1.strip() for col1 in q_cols]
            # check if q_cols are in names_df
            n = 0
            for col1 in q_cols:
                if col1 in names_df:
                    n += 1
            if len(names_df) != n:
                continue

            # foreign table column names
            fk_cols = element.find('fk_col').text.split(',')
            fk_cols = [col1.strip() for col1 in fk_cols]

            table = element.find('fk_table').text
            cols_in_select = ','.join(fk_cols)
            cols_in_where = [f'{col1}=%s' for col1 in fk_cols]
            cols_in_where = ','.join(cols_in_where)
            s1 = f'select {cols_in_select} from {table} where {cols_in_where}'
            if len(q_cols) == 1:
                values = set(df1['a'].values)
            else:
                values = []
                for i in range(len(self.data.index)):
                    r = tuple([self.data[col1].values[i] for col1 in q_cols])
                    values.append(r)
                values = tuple(set(values))
            not_found = 0
            for row in values:
                self.cur.execute(s1, row)
                r = self.cur.fetchone()
                if not r:
                    not_found += 1
                    logging.append(f'{row} not in {table}', False)
            tname = self.table.get('name')
            print(f'tabla {tname}: {not_found:n}/{len(values):n} not found')
            if not_found > 0:
                fk_errors + = 1
        if fk_errors > 0:
            raise ValueError(f'Errores de foreign keys in {fk_errors} ' \
                             'relaciones')


    def __column_names_from_element(self, root, element_name, cols_in_excelfi):
        """
        reads element col in each fk element; split content using comma as
            separator; checks in each column name is present in column names
            in excel file with the data; if all columns in foreign key are
            present returns a list with the column names, else return a
            o length list
        """
        col_names = root.find(element_name).text.split(',')
        col_names = [row.strip() for row in col_names]
        n = 0
        for cname in col_names:
            if cname in cols_in_excelfi:
                n += 1
        if len(col_names) != n:
            return []
        else:
            return col_names


    def check_foreign_keys(self) ->None:
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
         {'col_data': self.indic,
          'table': 'met.pexistencias',
          'col': 'indic'},
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
        s0 = "select indic from met.pexistencias where indic = %s"
        s1 = 'select indic from met.pmes where indic=%s and fecha=%s'
        insert = \
        """
        insert into met.pmes (indic, fecha, prec)
        values (%s, %s, %s);
        """
        update = \
        """
        update met.pmes
        set prec=%s
        where indic=%s and fecha=%s
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
            cur.execute(s0, (row[self.indic],))
            row1 = cur.fetchone()
            if row1 is None:
                logging.append(f"{index} {row[self.indic]} not found")
                not_found += 1
                continue
            fecha1 = self.last_date_of_the_month(row[self.year],
                                                 row[self.month])
            cur.execute(s1, (row[self.indic], fecha1))
            row1 = cur.fetchone()

            if row1 is None:
                try:
                    cur.execute(insert,
                                (row[self.indic], fecha1, row[self.prec]))
                    logging.append(f"{index:n} {row[self.indic]} " \
                                   f"{fecha1} inserted", False)
                    inserted += 1
                except:
                    msg = traceback.format_exc()
                    logging.append(f"{index:n} {row[self.indic]} " \
                                   f"{fecha1} error " +\
                                   f'inserting\n{msg}')
                    return
            else:
                if not update:
                    logging.append(f"{index:n} {row[self.indic]} " \
                                   f"{fecha1} not updated", False)
                    continue
                try:
                    cur.execute(update,
                                (row[self.prec],
                                 row[self.indic], fecha1))
                    logging.append(f"{index:n} {row[self.indic]} " \
                                   f"{fecha1} updated", False)
                    updated += 1
                except:
                    msg = traceback.format_exc()
                    logging.append(f"{index:n} {row[self.indic]} " \
                                   f"{fecha1} error " +\
                                   f'updating\n{msg}')
                    return
        self.con.commit()
        print(f'cod not found: {not_found:n}')
        print(f'inserted: {inserted:n}')
        print(f'updated: {updated}')


    def __foreign_keys_get(self) ->dict:
        """
        each table has its own columns to test, stored in cols_2 _test dict
        """
        elements = self.table.findall('fk')
        fks = []
        for element in elements:
            fk1 = {}
            fk1['col'] = None
            fk1['required'] = None
            fk1['fk_table'] = None
            fk1['fk_col'] = None
            fks.append(fk1)
        return fks
