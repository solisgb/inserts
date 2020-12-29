# -*- coding: utf-8 -*-
import pandas as pd
#import traceback
#import xml.etree.ElementTree as ET

from db_connection import con_get
#import littleLogging as logging


class Upsert():
    """
    imports the content of an excel file in a postgres table
    you can also import the point geometries: in this case you can provide
        to the constructor the name of the geometry column
    the first time you import data to a table, it's advisable to run the
        method create_file_template
    before to run the upsert function, you must define the converters; you
        can be help with the function get_converters; don't run the method
        upsert before you have defined a dictionary with the converters; the
        converter dict contains the translation between the column data type
        in th dataframe and the column type in the postgres table; a common
        case is when a columns of integers in the dateframe must be converted
        to str because the type in the postgres table
    """

    xml_org = 'upserts.xml'
    point_geometry_columns = ('XETRS89', 'YETRS89', 'EPGS')
    pg_create_point_arg_types = ('double precision', 'double precision',
                                 'integer')
    pd_create_point_arg_types = ('real64', 'real64', 'int64')

    def __init__(self, db: str, schema: str, table: str,
                 geom_name: str = None):
        """
        args:
            db: database
            schema: schema
            table: table
            if your table has a point geometry column you musy supply
                the column name
        """
        self.con = None
        try:
            self.db = db
            self.schema = schema
            self.table = table
            self.geom = geom_name
            self.con = con_get('postgres', db)
            self.cur = self.con.cursor()
            self.cur.execute(f'select * from {schema}.{table} limit 1')
            # self.data will be a DataFrame object when it be written the
            # excel file to be imported
            self.data = None
        except:
            if self.con is not None:
                self.con.close()
            raise ValueError('Error al instanciar Upsert')


    def create_template(self, path: str):
        """
        it create an excel file in the directory path with the table columns
            of self.table most common extensions: xlsx, xls
        """
        from os import sys
        from os.path import join
        dst = join(path, f'{self.schema}_{self.table}_template.xlsx')
        writer = pd.ExcelWriter(dst)
        columns = [names[0] for names in self.__table_column_get()]
        columns = self.__replace_geom(columns)
        df = pd.DataFrame(columns=columns)
        df.to_excel(writer, sheet_name=self.table, index=False,
                    encoding='utf-8')
        writer.save()
        print(f'{dst} has been written\n'
              'examine the file and create the convertes dict '
              'if it is necessary')
        sys.exit(0)


    def read_data(self,  fi: str, sheet_name: str, ):
        df = pd.read_excel(fi, sheet_name=sheet_name)
        return df


    def get_converters(self, fi: str, sheet_name: str, path: str, 
                       usecols: dict = None):
        """
        it reads the data in fi.sheet_name, reads the column type in
            self.table and writes the mapping between each column in self.table
            and columns in fi how are undertood by pandas
        args:
            fi: Excel file with the data to import
            sheet_name: sheet name
            path: directory in wich output will be written
        """
        from os.path import join
        from os import sys
        df = pd.read_excel(fi, sheet_name=sheet_name, engine='openpyxl',
                           usecols=usecols)
        df_types = list(df.dtypes)
        columns = self.__table_column_get()
        column_names = [col[0] for col in columns]
        column_names = self.__replace_geom(column_names)
        convert_to = ['' for col in column_names]
        pg_types = self.__point_geom_args(columns)
        df_types = [df_type for df_type in df_types]
        for i in range(len(column_names)):
            if pg_types[i] == 'character varying':
                convert_to[i] = 'str'
            elif pg_types[i] == 'double precision':
                convert_to[i] = 'float'
            elif pg_types[i] == 'boolean':
                convert_to[i] = 'bool'
            elif pg_types[i] == 'smallint':
                convert_to[i] = 'int'
            elif pg_types[i] == 'integer':
                convert_to[i] = 'int'
            elif pg_types[i] == 'timestamp with time zone':
                convert_to[i] = 'str'
            else:
                convert_to[i] = 'str'

        df_converters = pd.DataFrame({'column_name': column_names,
                                      'convert_to': convert_to,
                                      'pg_type': pg_types,
                                      'df_type': df_types})

        dst = join(path, f'{self.schema}_{self.table}_converters.xlsx')
        writer = pd.ExcelWriter(dst)
        df_converters.to_excel(writer, sheet_name=f'{self.table}_conv',
                               index=False, encoding='utf-8')
        writer.save()
        print(f'{dst} has been written\n'
              'examine the file and adjust the convertes dict')
        a = [f'"{column_names[i]}": {convert_to[i]}'
             for i in range(len(column_names))]
        b = '{' + ', '.join(a) + ' }'
        dst = join(path, f'{self.schema}_{self.table}_converters.py')
        with open(dst, 'w') as fo:
            fo.write('# -*- coding: utf-8 -*-\n')
            fo.write('# proposed converter dict\n')
            fo.write(f'converters = {b}\n')
        print(f'{dst} has been written\n'
              'examine the file and adjust the convertes dict')
        sys.exit(0)


    def __replace_geom(self, table_col_names: list):
        """
        it replaces the geometry column for the others that must be used to
            construct the geometry (arguments of st_ functions)
        args:
            table_col_names: each element is the column name
                of the table in wich data will be inserted
        """
        if self.geom is None:
            return table_col_names
        if self.geom not in table_col_names:
            raise ValueError(f'{self.geom} is not a valid column name in ',
                             f'table {self.schema}.{self.table}')
        n = table_col_names.index(self.geom)
        col1 = table_col_names[0:n]
        col2 = list(self.point_geometry_columns)
        if len(col1) == len(table_col_names):
            return col1 + col2
        else:
            return col1 + col2 + table_col_names[n+1:]


    def __point_geom_args(self, columns: list, pd_types: list = None):
        """
        it replaces the geometry column for the others that must be used to
            construct the geometry (arguments of st_ functions)
        args:
            columns: each element is a 2 element list with the name and the
                type of the table columns in wich data will be inserted
            pd_types: if present it returns pandas types, else pg types
        """
        table_col_names = [col[0] for col in columns]
        if pd_types is None:
            col_types = [col[1] for col in columns]
            arg_types = list(self.pg_create_point_arg_types)
        else:
            col_types = pd_types.copy()
            arg_types = list(self.pd_create_point_arg_types)
        if self.geom is None:
            return col_types
        if self.geom not in table_col_names:
            raise ValueError(f'{self.geom} is not a valid column name in ',
                             f'table {self.schema}.{self.table}')
        n = table_col_names.index(self.geom)
        col1 = col_types[0:n]
        col2 = arg_types
        if len(col1) == len(table_col_names):
            return col1 + col2
        else:
            return col1 + col2 + col_types[n+1:]


    def __pdtypes2pytypes(self, df_types):
        """
        it translates pandas types to python types
        """
        pytypes = []
        for df_type in df_types:
            if df_type == 'object':
                pytypes.append('str')
            elif df_type == 'int64':
                pytypes.append('integer')
            elif df_type == 'float64':
                pytypes.append('real')
            else:
                pytypes.append('str')
        return pytypes


    def __table_column_get(self) ->list:
        """
        returns a list with the name and type columns in the table to be
            upserted
        """
        s1 = \
        f"""
        select column_name, data_type
        from information_schema.columns
        where table_catalog='{self.db}' and table_schema ='{self.schema}'
            and table_name = '{self.table}'
        order by ordinal_position
        ;
        """
        self.cur.execute(s1)
        columns = [row for row in self.cur.fetchall()]
        return columns
