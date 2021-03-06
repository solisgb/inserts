# -*- coding: utf-8 -*-
"""
Created on Sun Nov  1 13:08:40 2020

@author: solis
"""
try:
    import littleLogging as logging
except:
    import sys
    t = sys.exc_info()
    print(f'{t[0]}\n{t[1]}')
    sys.exit(1)

# ======================data to complete========================
# Read the docstrings in upsert module
db = 'ipa'
schema = 'ipas'
table = 'ipa1'
path = r'H:\igme\tmp'
fi = r'H:\igme\tmp\ipas_ipa1.xlsx'
sheet_name = 'ipa1'
# columns a utilizar: si None todas; ejs. "A-B, D-E" desde la A a la E
#   sin la C
usecols = 'A:AM'
# columnas del fichero Excel a convertir al tipo:necesario en codigos numericos
# en columns con sólo dígitos
converters = {"cod": str, "z": float, "acuifero": str, "masub": str,
              "masup": str, "cod_demar": str, "natcode": str, "codprov": str,
              "codmuni": str, "tipo": str, "nivelado": bool, "toponimia": str,
              "paraje": str, "propietario": str, "propietario1": str,
              "titular_tipo": str, "direc_pro": str, "tfno_pro": str,
              "acceso": bool, "medida": bool, "direc_cont": str,
              "tfno_cont": str, "prof": float, "energia": str, "bomba": str,
              "hp": float, "prof_fil": float, "q_ls": float, "d_mm": float,
              "contador": bool, "bes": int, "ch": int, "observacio": str,
              "proyecto": str, "fecha_act": str, "XETRS89": float,
              "YETRS89": float, "EPGS": int, "codificador": str }

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

#        data.get_converters(fi, sheet_name, path, usecols)

        data.check_data(fi, sheet_name, converters, usecols)

#        pass01 = data.check_data_in_tables()
#        if not pass01:
#            raise ValueError('los datos no cumplen las condiciones')
#        data.print_min_max(['AÑO', 'PMES77'])
#        data.upsert()

        xtime = time() - startTime
        print(f'El script tardó {xtime:0.1f} s')

    except ValueError:
        msg = traceback.format_exc()
        logging.append(msg)
    except ImportError:
        msg = traceback.format_exc()
        print (msg)
    except SystemExit:
        pass
    except Exception:
        msg = traceback.format_exc()
        logging.append(msg)
    finally:
        try:
            data.con
        except NameError:
            pass
        else:
            data.con.close()
        logging.dump()
        print('\nFin')