# -*- coding: utf-8 -*-
"""
Created on Sun May  1 11:46:02 2022

@author: solis
"""

# ============ parameters =============
path2files = r'H:\LSGB\data2db\saih\cnp_dia'
tstep = 'day'  # in ('day', 'hour')
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

        saih = si.Saih_import(path2files, tstep)
        saih.upsert_data_from_csv_files()

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
