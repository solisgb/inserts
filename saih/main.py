# -*- coding: utf-8 -*-
"""
Created on Sun May  1 11:46:02 2022

@author: solis

Es frecuente que Los datos del saih haya que descargarlos en múltiples
ficheros, que tienen la misma estructura. Recientemente han cambiado el
contenido de los fichros csv:
    Delimitador. Ahora ';', antes ','
    Separador decimal. Ahora ',', antes '.'
Asegúrate de si tienes ficheros con distinto delimitador, detect_delim tiene
    valor true

Según el intervalo temporal seleccionado en la pág. web de la CHS, los
datos tienen intervalo temporal diario u horario, por lo que es
IMPRESCINDIBLE no mezclar en el mismo directorio de descarga datos con
discretizaciones temporales diferentes. El resto de información necesaria
se lee directamente de cada fichero

Revisa el encoding de los ficheros descargados, este
parámetro tiene valores por defecto al formato más reciente de la CHS, pero
pueden cambiar. En la actualidad file_encoding = 'utf-8-sig'
"""

# ============ parameters =============
path2files = r'H:\LSGB\data2db\saih\p01_dia'
tstep = 'day'  # in ('day', 'hour')
upsert = False
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
        saih.upsert_data_from_csv_files(upsert)

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
