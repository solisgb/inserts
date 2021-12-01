# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 21:00:03 2019
@author: solis
insert data in table ipa2 from a csv file
"""

import csv
from os.path import join
import numpy as np
import sqlite3
from sqlite3 import Error
import traceback

import littleLogging as logging

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
        tm text,
        prov text,
        id_uh text,
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
    create table if not exists param(
        fid text primary key,
        name text
    )
    """
    com4 = """
    create table if not exists masub(
        fid text primary key,
        name text
    )
    """
    com5 = """
    create table if not exists uh(
        fid text primary key,
        name text
    )
    """
    try:
        con = sqlite3.connect(dbname)
        cur = con.cursor()
        commands = (com1, com2, com3, com4, com5)
        for command in commands:
            cur.execute(command)

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

    select_puntos = """
    select *
    from puntos
    where fid = ?
    """

    insert_puntos = """
    insert into puntos(fid, xetrs89, yetrs89, id_mas, tm, prov,
        id_uh, acu, prof)
    values (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    select_masub = """
    select *
    from masub
    where fid = ?
    """

    insert_masub = """
    insert into masub(fid, name)
    values (?, ?)
    """

    select_param = """
    select *
    from param
    where fid = ?
    """

    insert_param = """
    insert into param(fid, name)
    values (?, ?)
    """

    select_uh = """
    select *
    from uh
    where fid = ?
    """

    insert_uh = """
    insert into uh(fid, name)
    values (?, ?)
    """

    select_analisis = """
    select *
    from analisis
    where fid = ? and fecha = ? and param = ?
    """

    insert_analisis = """
    insert into analisis(fid, fecha, param, valor, uds)
    values (?, ?, ?, ?, ?)
    """

    def to_float(col: str, file: str, line: int, col_name: str,
                 exception: bool, cols_with_null_values: list,
                 required: bool = True):

        if col.lower() == 'null':
            if col_name not in cols_with_null_values.keys():
                cols_with_null_values[col_name] = 1
            else:
                cols_with_null_values[col_name] += 1

            msg = f'file {file}, line {i:n} {col_name} is null'
            logging.append(msg, toScreen=False)
            if required and exception:
                raise ValueError(msg)
            return ''
        else:
            return float(col.replace(',', '.'))


    def to_str(col: str, file: str, line: int, col_name: str,
               exception: bool, cols_with_null_values: list,
               required: bool = True):

        if col.lower() == 'null':
            if col_name not in cols_with_null_values.keys():
                cols_with_null_values[col_name] = 1
            else:
                cols_with_null_values[col_name] += 1

            msg = f'file {file}, line {i:n} {col_name} is null'
            logging.append(msg, toScreen=False)
            if required and exception:
                raise ValueError(msg)
            return ''
        else:
            return col

    cols_with_null_values = {}

    try:
        connected = False
        con = sqlite3.connect(db)
        connected = True
        cur = con.cursor()
        for file in csvfiles:
            print(file)
            with open(join(csvpath, file), 'r', encoding='utf-8') as fi:
                csv_reader = csv.reader(fi, delimiter=sep)
                for i, row in enumerate(csv_reader):
                    if i == 0:
                        continue
                    col = row
                    for j in range(len(col)):
                        if j == 0:
                            col[j] = to_str(col[j], file, i, 'fid analisis', exception, cols_with_null_values)
                        elif j == 1:
                            col[j] = to_str(col[j], file, i, 'fecha', exception, cols_with_null_values)
                        elif j == 2:
                            col[j] = to_str(col[j], file, i, 'fid param', exception, cols_with_null_values)
                        elif j == 3:
                            col[j] = to_str(col[j], file, i, 'param', exception, cols_with_null_values)
                        elif j == 4:
                            col[j] = to_str(col[j], file, i, 'uds', exception, cols_with_null_values, False)
                        elif j == 5:
                            col[j] = to_float(col[j], file, i, 'valor', exception, cols_with_null_values)
                        elif j == 6:
                            col[j] = to_float(col[j], file, i, 'xetrs89', exception, cols_with_null_values)
                        elif j == 7:
                            col[j] = to_float(col[j], file, i, 'yetrs89', exception, cols_with_null_values)
                        elif j == 8:
                            col[j] = to_str(col[j], file, i, 'id_mas', exception, cols_with_null_values)
                        elif j == 9:
                            col[j] = to_str(col[j], file, i, 'mas name', exception, cols_with_null_values, False)
                        elif j == 10:
                            col[j] = to_str(col[j], file, i, 'tm', exception, cols_with_null_values, False)
                        elif j == 11:
                            col[j] = to_str(col[j], file, i, 'prov', exception, cols_with_null_values, False)
                        elif j == 12:
                            col[j] = to_str(col[j], file, i, 'id_uh', exception, cols_with_null_values)
                        elif j == 13:
                            col[j] = to_str(col[j], file, i, 'uh name', exception, cols_with_null_values, False)
                        elif j == 14:
                            col[j] = to_str(col[j], file, i, 'acu', exception, cols_with_null_values, False)
                        elif j == 15:
                            col[j] = to_float(col[j], file, i, 'prof', exception, cols_with_null_values, False)

                    if not insert_update:
                        continue

                    cur.execute(select_puntos, (col[0],))
                    if cur.fetchone() is None:
                        cur.execute(insert_puntos, (col[0], col[6], col[7],
                                                    col[8], col[10], col[11],
                                                    col[12], col[14], col[15]))

                    cur.execute(select_masub, (col[8],))
                    if cur.fetchone() is None:
                        cur.execute(insert_masub, (col[8], col[9]))

                    cur.execute(select_param, (col[2],))
                    if cur.fetchone() is None:
                        cur.execute(insert_param, (col[2], col[3]))

                    cur.execute(select_uh, (col[12],))
                    if cur.fetchone() is None:
                        cur.execute(insert_uh, (col[12], col[13]))

                    cur.execute(select_analisis, (col[0], col[1], col[2]))
                    if cur.fetchone() is None:
                       cur.execute(insert_analisis, (col[0], col[1], col[2],
                                                     col[5], col[4]))

        print('\ncols with null values')
        print('column, null values number')
        for key, value in cols_with_null_values.items():
            print(f'{key}, {value}')

    except Error:
        raise ValueError(Error)
    except ValueError:
        msg = traceback.format_exc()
        logging.append(f'ValueError exception\n{msg}')
    finally:
        if connected:
            con.commit()
            con.close()


def ooutliers(dbname: str):
    """
    IQR diferencia entre el tercer y el primer cuartil
    [(Q1-1.5 IQR), (Q3+1.5 IQR)]

    Parameters
    ----------
    dbname : str
        DESCRIPTION.

    Raises
    ------
    ValueError
        DESCRIPTION.

    Returns
    -------
    None.

    """

    select_puntos = """
    select fid
    from puntos
    order by fid;
    """

    select_params = """
    select fid
    from param
    order by fid
    ;
    """

    select_analisis = """
    select valor
    from analisis
    where fid=? and param = ?
    order by valor;
    """

    try:
        con = sqlite3.connect(dbname)
        cur = con.cursor()

        cur.execute(select_params)
        params = cur.fetchall()

        cur.execute(select_puntos)
        puntos = cur.fetchall()

        perc = np.empty(2, np.float32)

        for punto1 in puntos:
            print(punto1)
            for param1 in params:
                cur.execute(select_analisis, (punto1, param1))
                values = cur.fetchall()
                values = np.asarray(values, dtype=np.float32)
                perc = np.percentile(values, [25, 75])




    except Error:
        raise ValueError(Error)
    finally:
        con.close()

