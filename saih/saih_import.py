# -*- coding: utf-8 -*-
"""
Created on Sun May  1 11:47:47 2022

@author: solis

Data are imported into a table from csv files downloaded from the chs website.
The destination table can be: saih.tsd, saih.tsh or another
table with the same structure and primary key as the previous ones.

The date format of the downloaded data is %y-%m-%d HH:MM:ss. Depending on the
time interval selected when downloading the data, the content varies: If the
data are daily the content is %y-%m-%d 00:00:00:00; if the data are hourly
%y-%m-%d HH:00:00:00; if minute %y-%m-%d HH:MM:00

It is important that in an operation to load data into the database, the
selected directory does not have files of different temporal discrimination:
daily, hourly, hourly, etc.

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

    READ_CHUNK = 5000

    def __init__(self, path, tstep, pattern='*.csv',
                 file_encoding='utf-8-sig', detect_delim=False, delim=';'):
        """
        Insert the data in csv files in the tsd or tsh table.

        Parameters
        ----------
        path : str
            directory of csv files.
        tstep : str
            date-time step: must be: ('day', 'hour')
        pattern : str, optional
            The default is '*.csv'.
        file_encoding : str
            encoding of the files pattern in path
        detect_delim: bool
            If true, the delimiter is obtained directly from each csv file
        delim : str
            delimiter of csv file columns. If detect_delim is true, the value
            of delim is not considered

        Returns
        -------
        None.

        """
        if tstep == 'day':
            self.table = 'saih.tsd'
            self.pkey = 'tsd_pkey'
        elif tstep == 'hour':
            self.table = 'saih.tsh'
            self.pkey = 'tsh_pkey'
        else:
            msg = f'tstep {tstep} is not valid'
            logging.append(msg)
            raise ValueError(msg)

        self.tstep = tstep
        self.file_names = self.__file_names_get(path, pattern)

        self.path = path
        self.pattern = join(path, pattern)
        self.file_encoding = file_encoding
        self.delim = delim
        self.insert = \
            f"""
            insert into {self.table} values(%s, %s, %s, %s)
            on conflict on constraint {self.pkey}
            do nothing
            """
        self.upsert = \
            f"""
            insert into {self.table} values(%s, %s, %s, %s)
            on conflict on constraint {self.pkey}
            do update set v = excluded.v
            """


    @staticmethod
    def __connect():
        db = input('DB: ')
        user = input('User: ')
        passw = input('Password: ')
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
        Cheks if strdate is a valid datetime type acording self.tstep

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
            Not a valid strdate or self.tstep.

        Returns
        -------
        date or datetime
            strdate in proper type.

        """
        dt = datetime.strptime(strdate, '%Y-%m-%d %H:%M:%S')
        if self.tstep == 'day':
            if dt.hour > 0 or dt.minute > 0 or dt.second > 0:
                msg = f'Time step must be days in {fi}, line {line}'
                logging.append(msg)
                raise ValueError(msg)
            return date(dt.year, dt.month, dt.day)
        elif self.tstep == 'hour':
            if dt.minute > 0 or dt.second > 0:
                msg = f'Time step must be hours in {fi}, line {line}'
                logging.append(msg)
                raise ValueError(msg)
            return datetime(dt.year, dt.month, dt.day, dt.hour)
        else:
            msg = f'tstep {self.tstep} is not valid'
            logging.append(msg)
            raise ValueError(msg)


    def __ask_continue(self, upsert):
        logging.append(f'Files to import: {join(self.path, self.pattern)}')
        logging.append(f'Encoding of the files: {self.file_encoding}')
        logging.append(f'Delimiter of the files: {self.delim}')
        logging.append(f'Insert in table: {self.table}')
        logging.append(f'Upsert: {upsert}')
        ans = input('Continue?: ')
        if ans.lower() not in ('y', 's', '1'):
            logging.append('Operation aborted')
            return False
        else:
            return True


    def __find_delimiter(self, filename: str):
        sniffer = csv.Sniffer()
        with open(filename) as fp:
            delimiter = sniffer.sniff(fp.read(self.READ_CHUNK)).delimiter
        return delimiter


    def upsert_data_from_csv_files(self, upsert=True):
        """
        Inserts or upserts data in csv files

        Parameters
        ----------
        upsert : bool, optional
            If False inserts only new data; is True update values too.
            The default is True.

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
                with open(fi, encoding=self.file_encoding) as csv_file:
                    line = -1
                    csv_reader = csv.reader(csv_file, delimiter=self.delim)
                    for line, row in enumerate(csv_reader):
                        if line == 0:
                            id1 = row[1][0:5].lower()
                            var = row[1][5:8].lower()
                            fi_name = Path(fi).name
                            print(fi_name, id1, var)
                            continue

                        d = self.__check_time_step(row[0], fi, line)

                        try:
                            x = row[1].replace(',', '.')
                            x = float(x)
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



