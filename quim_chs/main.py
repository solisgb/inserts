# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 09:12:17 2021

@author: LUISSOLIS
"""

csvpath = r'D:\IGME20\20211124_calidad_subt_chs\data'
csvfiles = ('Export_1970_1979.csv', 'Export_1980_1989.csv',
            'Export_1990_1999.csv', 'Export_2000_2009.csv',
            'Export_2010_2019.csv', 'Export_2020_2029.csv')
dbname = r'D:\IGME20\20211124_calidad_subt_chs\data\quim_chs.db'

if __name__ == "__main__":

    try:
        llog = False
        import littleLogging as logging
        llog = True

        import traceback
        from time import time
        from upsert_quim_chs import create_tables, insert

        start = time()

        create_tables(dbname)

        insert(csvfiles, csvpath, dbname, update_puntos = False,
               exception = False, insert_update = True)

        end = time()
        print('ellapsed time ', end-start)

    except ValueError:
        msg = traceback.format_exc()
        logging.append(f'ValueError exception\n{msg}')
    except Exception:
        msg = traceback.format_exc()
        if not llog:
            print(msg)
        else:
            logging.append(f'Exception\n{msg}')
    finally:
        print('\nFin')

