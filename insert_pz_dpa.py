# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 21:00:03 2019
@author: solis
insert data in table ipa2 from a csv file
"""
import pandas as pd
import traceback
from db_connection import con_get
import littleLogging as logging


class Insert_pz_dpa():

    column_names = {'c_hoja': None, 'c_oct': None, 'c_toma': None,
                    'fecha': None, 'situacion': None, 'mide': None,
                    'pnp': None, 'tr': None, 'cod_proyecto': None}


    def __init__(self, fi: str, col_names: dict, sep: str=';',
                 sheet_name: str=None, converters: dict={}):
        """
        args:
            fi: path to csv file
            col_names: column names in fi; all these names must exist
            sep: column separatos in fi
        """
        from os.path import splitext
        self.fi = fi
        self.col_names = Insert_pz_dpa.column_names.copy()
        for k in self.col_names.keys():
            self.col_names[k] = col_names[k]
        self.sep = sep
        self.checked_data_in_tables = False
        self.checked_booleans = False
        if splitext(fi)[1] == '.csv':
            df = pd.read_csv(self.fi, sep=self.sep)
        else:
            df = pd.read_excel(fi, sheet_name=sheet_name,
                                      converters=converters)
        self.data = df.where(pd.notnull(df), None)
        self.con = con_get('postgres', 'bdaserver')


    @property
    def c_hoja(self):
        return self.col_names['c_hoja']


    @property
    def c_oct(self):
        return self.col_names['c_oct']


    @property
    def c_toma(self):
        return self.col_names['c_toma']


    @property
    def fecha(self):
        return self.col_names['fecha']


    @property
    def situacion(self):
        return self.col_names['situacion']


    @property
    def mide(self):
        return self.col_names['mide']


    @property
    def pnp(self):
        return self.col_names['pnp']


    @property
    def tr(self):
        return self.col_names['tr']


    @property
    def cod_proyecto(self):
        return self.col_names['cod_proyecto']


    def check_data_in_tables(self) ->bool:
        """
        chequea que los contenidos de determinadas columnas están en tablas
            codificadas
        """

        # columna códigos en toma
        pkeys = [(hoja1, oct1, toma1, 'B') for hoja1, oct1, toma1 in zip(
                self.data[self.col_names['c_hoja']],
                self.data[self.col_names['c_oct']],
                self.data[self.col_names['c_toma']]
                )]
        pkeys = tuple(set(pkeys))
        s0 = """select concat(c_hoja, c_oct, c_toma) from ipa1
        where c_hoja=%s and c_oct=%s and c_toma=%s and tipo=%s"""
        not_found = \
        self.__data_in_table(pkeys, s0, 'ipa1')
        if not_found:
            return False

        # columna situacion
        s0 = "select c from ipa_situaci where c = %s"
        situacion = set(self.data[self.col_names['situacion']].str.upper())
        not_found = self.__data_in_table(situacion, s0,
                                         'public.ipa_situaci.c')
        if not_found:
            return False

        # columna mide
        s0 = "select c from ipa_mide where c = %s"
        situacion = set(self.data[self.col_names['mide']])
        not_found = self.__data_in_table(situacion, s0,
                                         'public.ipa_mide.c')
        if not_found:
            return False

        # columna cod_proyecto
        s0 = "select cod_proyecto from proyectos where cod_proyecto = %s"
        situacion = set(self.data[self.col_names['cod_proyecto']])
        not_found = self.__data_in_table(situacion, s0,
                                         'public.proyectos.cod_proyecto')
        if not_found:
            return False

        self.checked_data_in_tables = True
        return True


    def check_booleans(self) ->bool:
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
            if isinstance(item, tuple):
                cur.execute(s0, item)
            else:
                cur.execute(s0,(item,))
            row = cur.fetchone()
            if row is None:
                not_found.append(item)
            else:
                row = row[0]
        if not_found:
            print(f'No existen {len(not_found):n}/{len(items):n} valores ' +
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
        s0 = \
        """
        select c_hoja
        from public.ipa1
        where c_hoja=%s and c_oct=%s and c_toma=%s
        """
        s1 = \
        """
        select c_hoja
        from public.ipa2
        where c_hoja=%s and c_oct=%s and c_toma=%s and fecha=%s"""
        insert = \
        """
        insert into public.ipa2 (c_hoja, c_oct, c_toma, fecha, situacion,
                               mide, pnp, tr, cod_proyecto)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        update = \
        """
        update public.ipa2
        set situacion=%s, mide=%s, pnp=%s, tr=%s, cod_proyecto=%s
        where c_hoja=%s and c_oct=%s and c_toma=%s and fecha=%s
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
            cur.execute(s0,
                        (row[self.c_hoja], row[self.c_oct], row[self.c_toma]))
            row1 = cur.fetchone()
            if row1 is None:
                logging.append(f"{index} {row[self.c_hoja]}{row[self.c_oct]}" \
                               f"{row[self.c_toma]} not found in public.ipa1"
                               , False)
                not_found += 1
                continue

            cur.execute(s1, (row[self.c_hoja], row[self.c_oct],
                             row[self.c_toma], row[self.fecha]))
            row1 = cur.fetchone()

            if row1 is None:
                try:
                    cur.execute(insert, (row[self.c_hoja], row[self.c_oct],
                                         row[self.c_toma], row[self.fecha],
                                         row[self.situacion],
                                         row[self.mide], row[self.pnp],
                                         row[self.tr], row[self.cod_proyecto]))
                    logging.append(f"{index} {row[self.c_hoja]}" \
                                   f"{row[self.c_oct]}{row[self.c_toma]} " \
                                   f"{row[self.fecha]} inserted", False)
                    inserted += 1
                except:
                    msg = traceback.format_exc()
                    logging.append(f"{index} {row[self.c_hoja]}" \
                                   f"{row[self.c_oct]}{row[self.c_toma]} " \
                                   f"{row[self.fecha]} error inserting\n{msg}",
                                   False)
                    return msg
            else:
                if not update:
                    logging.append(f"{index} {row[self.c_hoja]}" \
                                   f"{row[self.c_oct]}{row[self.c_toma]} " \
                                   f"{row[self.fecha]} not updated", False)
                    continue
                try:
                    cur.execute(update,
                                (row[self.situacion], row[self.mide],
                                 row[self.pnp], row[self.tr],
                                 row[self.cod_proyecto], row[self.c_hoja],
                                 row[self.c_oct], row[self.c_toma],
                                 row[self.fecha]))
                    logging.append(f"{index} {row[self.c_hoja]}" \
                                   f"{row[self.c_oct]}{row[self.c_toma]} " \
                                   f"{row[self.fecha]} updated", False)
                    updated += 1
                except:
                    msg = traceback.format_exc()
                    logging.append(f"{index} {row[self.c_hoja]}" \
                                   f"{row[self.c_oct]}{row[self.c_toma]} " \
                                   f"{row[self.fecha]} error " \
                                   f"updating\n{msg}", False)
                    return msg
        self.con.commit()
        print(f'c_hoja c_oct c_ipa not found: {not_found:n}')
        print(f'inserted: {inserted:n}')
        print(f'updated: {updated}')
        return 'terminado'

