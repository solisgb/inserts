# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 21:00:03 2019
@author: solis
insert data in table ipa2 from a csv file
"""

import littleLogging as logging
from os.path import join
import sqlite3
from sqlite3 import Error

column_names = ('Estación', 'FechaToma', 'Param Cod', 'Param Nom',
                'Unidades', 'Valor', 'X_ETRS89', 'Y_ETRS89', 'Cod Masa',
                'Nombre Masa', 'Municipio', 'Provincia', 'UH Geo',
                'UH Geo Nombre', 'Acuifero', 'PROFUNDIDAD')

def create_tables(dbname: str):
    com1 = """
    create table if not exists puntos(
        fid text primary key,
        xetrs89 real,
        yetrs89 real,
        id_mas text,
        mas text,
        tm text,
        prov text,
        id_uh text,
        uh text,
        acu text,
        prof real
    )
    """
    com2 = """
    create table if not exists analisis(
        fid text,
        fecha text,
        param text,
        valor real,
        uds text,
        primary key (fid, fecha, param)
    )
    """
    com3 = """
    create table if not exists param_q(
        fid text primary key,
        param text
    )
    """
    com4 = """
    create table if not exists masub_q(
        fid text primary key,
        masub text
    )
    """
    try:
        con = sqlite3.connect(dbname)
        cur = con.cursor()
        cur.execute(com1)
        cur.execute(com2)
        cur.execute(com3)
        cur.execute(com4)

    except Error:
        raise ValueError(Error)
    finally:
        con.close()


def insert(csvfiles: list, csvpath: str, db: str,
           sep: str =';',
           update_puntos: bool = True,
           exception:bool = True,
           insert_update: bool = True) -> None:
    """

    Parameters
    ----------
    csvfiles : list
        lista de ficheros csv o txt
    csvpath : str
        directorio de los ficheros csvfile
    db : str
        nombre y dirección de la db sqlite; si no existe la crea
    sep : text
        separador de columnas en los ficheros txt
    update_puntos : bool
        si True actualiza las filas de las tablas
    exception:
        si True lanza un raise si encuentra una tabla con un campo requerido
        con contenido null
    insert_update:
        si True realiza un insert/update de las tablas a partir de csvfiles
    Returns
        None
    """

    select1 = """
    select *
    from puntos
    where idchs = ?
    """

    def to_float(col: str, file: str, line: int, col_name: str,
                 required: bool = True):
        if col.lower() == 'null':
            msg = f'file {file}, line {i:n} {col_name} is null'
            if required:
                raise ValueError(msg)
            logging.append(msg)
            return ''
        else:
            return float(col[j])


    def to_str(col: str, file: str, line: int, col_name: str,
               required: bool = True):
        if col.lower() == 'null':
            msg = f'file {file}, line {i:n} {col_name} is null'
            if required:
                raise ValueError(msg)
            logging.append(msg)
            return ''
        else:
            return col[j]

    try:
        con = sqlite3.connect(db)
        cur = con.cursor()
        for file in csvfiles:
            print(file)
            with open(join(csvpath, file), 'r', encoding='Latin-1') as fi:
                for i, row in enumerate(fi):
                    if i == 0:
                        continue
                col = row.split(sep)
                for j in range(col):
                    if j == 0:
                        col[j] = to_str(col[j], file, i, 'fid analisis')
                    elif j == 1:
                        col[j] = to_str(col[j], file, i, 'fecha')
                    elif j == 2:
                        col[j] = to_str(col[j], file, i, 'fid param')
                    elif j == 3:
                        col[j] = to_str(col[j], file, i, 'param')
                    elif j == 4:
                        col[j] = to_str(col[j], file, i, 'uds', False)
                    elif j == 5:
                        col[j] = to_str(col[j], file, i, 'valor')
                    elif j == 6:
                        col[j] = to_float(col[j], file, i, 'xetrs89', False)
                    elif j == 7:
                        col[j] = to_float(col[j], file, i, 'yetrs89', False)
                    elif j == 8:
                        col[j] = to_str(col[j], file, i, 'id_mas')
                    elif j == 9:
                        col[j] = to_str(col[j], file, i, 'mas name', False)
                    elif j == 10:
                        col[j] = to_str(col[j], file, i, 'tm', False)
                    elif j == 11:
                        col[j] = to_str(col[j], file, i, 'prov', False)
                    elif j == 12:
                        col[j] = to_str(col[j], file, i, 'id_uh', False)
                    elif j == 13:
                        col[j] = to_str(col[j], file, i, 'uh name', False)
                    elif j == 14:
                        col[j] = to_str(col[j], file, i, 'acu', False)
                    elif j == 15:
                        col[j] = to_float(col[j], file, i, 'prof', False)

    except Error:
        raise ValueError(Error)
    finally:
        con.close()



def __check_column_names(row: str, file: str):
    cols = row.split(';')
    for col1, col2 in zip(cols, column_names):
        if col1 != col2:
            raise ValueError(f'File {file}\nLa columna {col1} no está ' +
                             'definida o está en distinto orden')



