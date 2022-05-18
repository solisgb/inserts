# -*- coding: utf-8 -*-
"""
Created on Sun May  1 11:46:02 2022

@author: solis
"""

# ============ parameter =============
path2files = r'H:\LSGB\data2db\saih\q01_dia'
table = 'saih.tsh'
pkey = 'tsh_pkey'
tstep = 'hour'
# =====================================


if __name__ == "__main__":

    try:
        from datetime import datetime
        from time import time
        import traceback
        import sys

        import littleLogging as logging
        import saih_import as si

        now = datetime.now()

        startTime = time()

        saih = si.Saih_import(path2files, table, pkey, tstep)
        saih.upsert_data_from_csv_files(False)

        xtime = time() - startTime
        print(f'El script tardó {xtime:0.1f} s')

    except ValueError:
        msg = traceback.format_exc()
        logging.append(f'ValueError exception\n{msg}')
    except ImportError:
        msg = traceback.format_exc()
        print (f'ImportError exception\n{msg}')
    except Exception:
        msg = traceback.format_exc()
        logging.append(f'Exception\n{msg}')
    finally:
        if 'logging' in sys.modules:
            logging.dump()
        print('\nFin')
