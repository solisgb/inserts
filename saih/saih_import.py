# -*- coding: utf-8 -*-
"""
Created on Sun May  1 11:47:47 2022

@author: solis

se importan los datos a una tabla desde ficheros csv descargados de
la web de la chs. La tabla puede ser: la tabla saih.tsd, saih.tsh u otra tabla
con la misma estructura y primary key que las anteriores

según el intervalo temporal con el que se descargan los
datos, la discriminación temporal es distinta, datos diarios en formato
%y-%m-%d 00:00:00 o dato horario %y-%m-%d H:00:00 o dato minutario

Es importante que en una
misma operación de carga de datos no se mezclen en el mismo directorio ficheros
csv coo datos de diferente discriminación temporal: diarios, horarios

"""
import csv
from datetime import datetime, date
import glob
from pathlib import Path
import psycopg2
from os.path import join
import traceback

import littleLogging as logging


class Saih_import():

    implemented_vars = ('day', 'hour')

    def __init__(self, path, table, pkey, tstep, pattern='*.csv'):
        """
        Insert the data in csv files in the tsd or tsh table.

        Parameters
        ----------
        path : str
            directory of csv files.
        table : str
            Table name.
        pkey : str
            Primary key name.
        tstep : str
            tim step: must be in implemented_vars.
        pattern : str, optional
            The default is '*.csv'.

        Returns
        -------
        None.

        """
        if tstep not in self.implemented_vars:
            msg = f'step {self.setp} is not valid'
            logging.append(msg)
            raise ValueError(msg)

        self.file_names = self.__file_names_get(path, pattern)

        self.path = path
        self.pattern = join(path, pattern)
        self.insert = \
            f"""
            insert into {table} values(%s, %s, %s, %s)
            on conflict on constraint {pkey}
            do nothing
            """
        self.upsert = \
            f"""
            insert into {table} values(%s, %s, %s, %s)
            on conflict on constraint {pkey}
            do update set v = excluded.v
            """
        self.table = table
        self.step = tstep


    @staticmethod
    def __connect():
        user = input('User: ')
        passw = input('Password: ')
        db = input('DB: ')
        con = psycopg2.connect(database=db, user=user, password=passw)
        return con


    def __count_rows(self, cur):
        cur.execute(f'select count(*) from {self.table}')
        row = cur.fetchone()
        return row[0]


    @staticmethod
    def __file_names_get(path, pattern):
        file_names = [name for name in glob.glob(join(path, pattern))]
        if not file_names:
            msg = f'No files in {join(path, pattern)}'
            logging.append(msg)
            raise ValueError(msg)
        return file_names


    def __check_time_step(self, strdate, fi, line):
        """
        Cheks if strdate is a valid datetime type acording self.step

        Parameters
        ----------
        strdate : str
            Date or datetime type as str.
        fi : str
            File if strdate.
        line : int
            line in fi.

        Raises
        ------
        ValueError
            Not a valid strdate or self.step.

        Returns
        -------
        date or datetime
            strdate in proper type.

        """
        dt = datetime.strptime(strdate, '%Y-%m-%d %H:%M:%S')
        if self.step == 'day':
            if dt.hour > 0 or dt.minute > 0 or dt.second > 0:
                msg = f'Time step must be days in {fi}, line {line}'
                logging.append(msg)
                raise ValueError(msg)
            return date(dt.year, dt.month, dt.day)
        elif self.step == 'hour':
            if dt.minute > 0 or dt.second > 0:
                msg = f'Time step must be hours in {fi}, line {line}'
                logging.append(msg)
                raise ValueError(msg)
            return datetime(dt.year, dt.month, dt.day, dt.hour)
        else:
            msg = f'step {self.setp} is not valid'
            logging.append(msg)
            raise ValueError(msg)


    def __ask_continue(self, upsert):
        logging.append(f'Files to import: {join(self.path, self.pattern)}')
        logging.append(f'In table: {self.table}')
        logging.append(f'Upsert: {upsert}')
        ans = input('Continue?: ')
        if ans.lower() not in ('y', 's', '1'):
            logging.append('Operation aborted')
            return False
        else:
            return True


    def upsert_data_from_csv_files(self, upsert=True, file_encoding='utf8'):
        """
        Inserts or upserts data in csv files

        Parameters
        ----------
        upsert : bool, optional
            If False inserts only new data; is True update values too.
            The default is True.
        file_encoding: str, optional
            File encoding
        Raises
        ------
        ValueError
        Any exception

        Returns
        -------
        None.

        """
        if not self.__ask_continue(upsert):
            return

        try:
            con = Saih_import.__connect()
            cur = con.cursor()

            n = 0
            nr0 = self.__count_rows(cur)
            for fi in self.file_names:
                with open(fi, encoding=file_encoding) as csv_file:
                    line = -1
                    csv_reader = csv.reader(csv_file, delimiter=',')
                    for line, row in enumerate(csv_reader):
                        if line == 0:
                            id1 = row[1][0:5].lower()
                            var = row[1][5:8].lower()
                            fi_name = Path(fi).name
                            print(fi_name, id1, var)
                            continue

                        d = self.__check_time_step(row[0], fi, line)

                        try:
                            x = float(row[1])
                            if upsert:
                                cur.execute(self.upsert, (id1, d, var, x))
                            else:
                                cur.execute(self.insert, (id1, d, var, x))
                            n += 1
                        except ValueError:
                            msg = f'{fi}, line {line:d} "{row[1]}" is not a number'
                            logging.append(msg, False)
                        except Exception:
                            msg = traceback.format_exc()
                            logging.append(f'Exception\n{msg}')

            con.commit()
            nr1 = self.__count_rows(cur)
            m = nr1-nr0
            logging.append(f'Rows inserted: {m:d}')
            if upsert:
                logging.append(f'Rows updated: {n-m:d}')
            else:
                logging.append('Rows updated: 0')

        except ValueError:
            msg = traceback.format_exc()
            if 'line' in locals():
                msg = f'{fi}, line {line:d}\n{msg}'
            else:
                msg = f'{msg}'
            logging.append(f'{msg}')
        except Exception:
            msg = traceback.format_exc()
            logging.append(f'{msg}')
        finally:
            if 'con' in locals():
                con.close()



