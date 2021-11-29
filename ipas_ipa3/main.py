# -*- coding: utf-8 -*-
"""
Created on Sun Nov  1 13:08:40 2020

@author: solis
"""
import littleLogging as logging

# ===================data to complete========================
excelf = \
r'H:\off\chs\Aforos_CHS_20-21\trabajo\entregas\03_camp_202011\datos' \
r'\datos_para_db.xlsx'
sheet_name = 'aforos'
# correspondencia entre el nombre de las columnas en la tabla ipa2 -clave-
# y el nombre de las columnas en el fichero csv o excel
column_names = \
{'cod': 'COD',
 'fecha': 'FECHA',
 'caudal_ls': 'CAUDAL_LS',
 'error': 'ERROR',
 'situacion': 'SITUACION',
 'proyecto': 'PROYECTO',
 'medidor': 'MEDIDOR'
 }
# conversión de tipos al leer la Excel, es útil para leer una columna
# de números como texto
converters={'COD':str, 'PROYECTO':str, 'MEDIDOR':str}
# columnas del fichero Excel a convertir a minúsculas
cols_to_lowercase=('COD', 'SITUACION')
# =============================================================

if __name__ == "__main__":

    try:
        from datetime import datetime
        from time import time
        import traceback
        from upsert_ipas_ipa3 import Upsert_ipas_ipa3 as ups

        now = datetime.now()

        startTime = time()

        af = ups(excelf, column_names, sheet_name=sheet_name,
                 converters=converters, lowercase=cols_to_lowercase)
        pass01 = af.check_data_in_tables()
        if not pass01:
            raise ValueError('los datos no cumplen las condiciones')
        af.print_min_max(['FECHA', 'CAUDAL_LS'])
        a = af.upsert()

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
        try:
            af.con
        except NameError:
            pass
        else:
            af.con.close()
        logging.dump()
        print('\nFin')
