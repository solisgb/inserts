# -*- coding: utf-8 -*-
"""
Created on Sun Nov  1 13:08:40 2020

@author: solis
"""
import littleLogging as logging

# ===================data to complete========================
db = 'ipa'
schema = 'ipas'
table = 'ipa1'
path = r'H:\igme\tmp'
fi = r'H:\igme\tmp\ipas_ipa1.xlsx'
sheet_name = 'ipa1'


# columns a utilizar: si None todas; otro ej. "A-B, D-E" hasta la E sin la C
usecols = 'A:AL'
# columnas del fichero Excel a convertir al tipo:necesario en codigos numericos
# en columns con sólo dígitos
converters={'cod': str, 'acuifero': str}
# columnas del fichero Excel a convertir a minúsculas
cols_to_lowercase=()
# =============================================================

if __name__ == "__main__":

    try:
        from datetime import datetime
        from time import time
        import traceback
        from upserts import Upsert as ups

        now = datetime.now()

        startTime = time()

        data = ups(db, schema, table, geom_name='geom')

#        data.create_template(path)

        data.get_converters(fi, sheet_name, path)

#        pass01 = data.check_data_in_tables()
#        if not pass01:
#            raise ValueError('los datos no cumplen las condiciones')
#        data.print_min_max(['AÑO', 'PMES77'])
#        data.upsert()

        xtime = time() - startTime
        print(f'El script tardó {xtime:0.1f} s')

    except ValueError:
        msg = traceback.format_exc()
        logging.append(f'ValueError exception\n{msg}')
    except ImportError:
        msg = traceback.format_exc()
        print (f'ImportError exception\n{msg}')
    except SystemExit:
        pass
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
