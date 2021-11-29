# -*- coding: utf-8 -*-
"""
Created on Sun Nov  1 13:08:40 2020

@author: solis
"""
import littleLogging as logging

# ===================rellena estos datos y ejecuta========================
excelf: str = \
r'H:\off\chs\Aforos_CHS_20-21\trabajo\entregas\02_camp_202009' \
r'\pluviometria_mensual\interpol\out\pd_idw.xlsx'
sheet_name: str = 'Hoja1'
# correspondencia entre el nombre de las columnas en la tabla ipa2 -clave-
# y el nombre de las columnas en el fichero csv o excel
column_names: dict  = \
{'fid': 'fid',
 'variable': 'variable',
 'fecha': 'fecha',
 'value': 'valor',
 'metodo': 'metodo'}
# conversión de tipos al leer la Excel, es útil para leer una columna
# de números como texto
converters: dict = {'fid': str}
# columnas del fichero Excel a convertir a minúsculas
cols_to_lowercase: tuple = ()
# columnas del fichero Excel a mostrar los valores min y max
cols_min_max: tuple = ('fecha', 'valor')
# =============================================================

if __name__ == "__main__":

    try:
        from datetime import datetime
        from time import time
        import traceback
        from upsert_met_interpolated_tseries \
        import Upsert_met_interpolated_tseries as ups

        now = datetime.now()

        startTime = time()

        data = ups(excelf, column_names, sheet_name=sheet_name,
                   converters=converters,
                   lowercase=cols_to_lowercase)
        pass01 = data.check_data_in_tables()
        if not pass01:
            raise ValueError('los datos no cumplen las condiciones')
        data.print_min_max(cols_min_max)
        data.upsert()

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
            data.con
        except NameError:
            pass
        else:
            data.con.close()
        logging.dump()
        print('\nFin')
