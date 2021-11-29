# -*- coding: utf-8 -*-
"""
Created on Sun Nov  1 13:08:40 2020

@author: solis
"""
import littleLogging as logging

# ===================data to complete========================
excelf = \
r'H:\off\chs\Aforos_CHS_20-21\trabajo\entregas\02_camp_202009' \
r'\pluviometria_mensual\todos.xlsx'
sheet_name = 'Hoja1'
# correspondencia entre el nombre de las columnas en la tabla ipa2 -clave-
# y el nombre de las columnas en el fichero csv o excel
column_names = \
{'indic': 'INDICATIVO',
 'year': 'AÑO',
 'month': 'MES',
 'nombre': 'NOMBRE',
 'z': 'ALTITUD',
 'x_utm': 'C_X',
 'y_utm': 'C_Y',
 'prov': 'NOM_PROV',
 'prec': 'PMES77'
}
# conversión de tipos al leer la Excel, es útil para leer una columna
# de números como texto
converters={}
# columnas del fichero Excel a convertir a minúsculas
cols_to_lowercase=()
# =============================================================

if __name__ == "__main__":

    try:
        from datetime import datetime
        from time import time
        import traceback
        from upsert_met_pmes import Upsert_met_pmes as ups

        now = datetime.now()

        startTime = time()

        data = ups(excelf, column_names, sheet_name=sheet_name,
                   converters=converters,
                   lowercase=cols_to_lowercase)
        pass01 = data.check_data_in_tables()
        if not pass01:
            raise ValueError('los datos no cumplen las condiciones')
        data.print_min_max(['AÑO', 'PMES77'])
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
