# -*- coding: utf-8 -*-
import pandas as pd

from db_connection import con_get
import littleLogging as logging


class Upsert():
    """
    It checks the data to be imported from an Excel file to a postgres
    database table and when all are ok you can import then using an upsert
    sentence

    The first row in the Excel file are the exact names of the columns in
    the table in which the data are going to be imported

    You can also import the point geometries: in this case you can provide
        to the constructor the name of the geometry column and the template
        file substitutes geometry column name with appropiate x, y and epsg
        names

    Usually you need to provide a dict with the converters. Some columns in
        the dataframe must have a type compatible with th postgres column
        in the table. You can be help with by using the function
        get_converters: the converter dict contains the translation between
        the column data type in the dataframe and the column type in the
        postgres table; a common case is when a columns of integers in the
        dateframe must be converted to str because the type in the postgres
        table

    Mode of use:
        1.- instance an Upsert object
        2.- Create the converter dict by yourself or by using the function
        get_converters
        3.- If it is the case, select the columns in the Excel file to be
        imported
        4.- Check if the data acomplish the rules by running the function
        check_data
        5.- Insert or update the data in the Excel file into tle table
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
            geom: point geometry column name
            con: connection to db
            cur: cursor to con
            data: dataframe object with the data to be checked and imported
            convertes: when pandas reads an Excel file it inferes the dtype
                of the column; in same cases you need it understand some
                columns un a specific type, you need concerts these column
                to the prostgers compatible type. Read the documentation in
                pandas.DataFrame.read_excel function
            usecols: columns in Excel file to be read. Read the documentation
                in pandas.DataFrame.read_excel function
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
            self.converters = None
            self.usecols = None
            self.notnullcols = None
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
        columns = self.__table_column_get(False)
        columns = self.__replace_geom(columns)
        df = pd.DataFrame(columns=columns)
        df.to_excel(writer, sheet_name=self.table, index=False,
                    encoding='utf-8', engine='openpyxl')
        writer.save()
        print(f'{dst} has been written\n'
              'examine the file and create the convertes dict '
              'if it is necessary')
        sys.exit(0)


    def read_data(self, fi: str, sheet_name: str,
                  converters: dict = None, usecols: str = None):
        df = pd.read_excel(fi, sheet_name=sheet_name,
                           converters=converters, usecols=usecols,
                           engine='openpyxl')
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


    def __table_column_get(self, data_type:bool = True) ->list:
        """
        returns a list with the name and type columns in the table to be
            upserted
        """
        if data_type:
            columns = 'column_name, data_type'
        else:
            columns = 'column_name'
        s1 = \
        f"""
        select {columns}
        from information_schema.columns
        where table_catalog='{self.db}' and table_schema ='{self.schema}'
            and table_name = '{self.table}'
        order by ordinal_position
        ;
        """
        self.cur.execute(s1)
        return [row for row in self.cur.fetchall()]


    def check_data(self, fi: str, sheet_name: str, converters: dict={},
                   usecols: str = None):
        """
        It checks the data to be inserted. Conditions to be tested:
            1.- not null columns: the columns is not nullable and must have
                not null values
            2.- foreign keys: the values in these column must join the
                data in the foreign key table
        """
        if self.data is None:
            self.data = self.read_data(fi, sheet_name, converters,
                                       usecols)
            self.converters = converters
            self.usecols = usecols
        tests = {'test not null columns': True,
                 'foreign keys': True}

        # test 1
        tests['test not null columns'] = self.__check_not_null()

        # test 2
        tests['foreign keys'] = self.__check_foreign_keys()

        ner = 0
        for k, v in tests.items():
            print(k, v, sep=': ')
            if not v:
                ner += 1

        if ner == 0:
            logging.append('Data can be imported')
            return True
        else:
            logging.append('Data must be corrected')
            return False


    def __primary_key(self, column_type:bool = True):
        """
        it returns primary keys columns and its types
        """
        if column_type:
            columns = 'a.attname, format_type(a.atttypid, a.atttypmod) ' +\
                'as data_type'
        else:
            columns = 'a.attname'
        s1 = \
        f"""
        select {columns}
        from pg_index i
        join pg_attribute a on (a.attrelid = i.indrelid and
            a.attnum = any(i.indkey))
        where i.indrelid = '{self.schema}.{self.table}'::regclass
            and i.indisprimary;
        """
        self.cur.execute(s1)
        return [row for row in self.cur.fetchall()]


    def __check_not_null(self) -> bool:
        """
        it returns the name of not nullable columns
        """
        s1 = \
        f"""
        select column_name
        from information_schema.columns
        where table_schema = '{self.schema}'
        	and table_name   = '{self.table}'
            and is_nullable = 'NO';
        """
        df_columns = list(self.data.columns)
        self.cur.execute(s1)
        notnullcols = [col[0] for col in self.cur.fetchall()]
        logging.append('\ncheck column with not null restriction')
        n1 = n2 = 0
        for col in notnullcols:
            if col in df_columns:
                m = self.data[f'{col}'].isnull().sum()
                logging.append(f'{col} is present and has {m:n} null values')
                if m > 0:
                    n2 += 1
            else:
                n1 += 1
                logging.append(f'{col} is not present')
        if n1 > 0:
            logging.append(f'{n1:n} columns must be present but are not')
        if n2 > 0:
            logging.append(f'{n2:n} columns are present but have null ' +\
                           'values and they can not')
        if n1+ n2 > 0:
            return False
        self.notnullcols = notnullcols.copy()
        return True


    def __check_foreign_keys(self) -> bool:
        """
        It checks the data in columns related to foreign keys.
        It checks for each foreign key:
            If the foreign key columns are in data file
            If data in foreign keys columns have not null values
            If data in foreign keys columns have valid values
        """
        s1 = \
        f"""
        select
        	tc.constraint_name
        from
        	information_schema.table_constraints as tc
        join information_schema.key_column_usage as kcu on
        	tc.constraint_name = kcu.constraint_name
        	and tc.table_schema = kcu.table_schema
        join information_schema.constraint_column_usage as ccu on
        	ccu.constraint_name = tc.constraint_name
        	and ccu.table_schema = tc.table_schema
        where
        	tc.constraint_type = 'FOREIGN KEY'
        	and tc.table_schema = '{self.schema}'
        	and tc.table_name = '{self.table}';
        """
        s2 = \
            """
            select
                kcu.column_name,
            	ccu.table_schema as foreign_table_schema,
            	ccu.table_name as foreign_table_name,
            	ccu.column_name as foreign_column_name
            from
            	information_schema.table_constraints as tc
            join information_schema.key_column_usage as kcu on
            	tc.constraint_name = kcu.constraint_name
            	and tc.table_schema = kcu.table_schema
            join information_schema.constraint_column_usage as ccu on
            	ccu.constraint_name = tc.constraint_name
            	and ccu.table_schema = tc.table_schema
            where
            	tc.constraint_type = 'FOREIGN KEY'
            	and tc.table_schema = %s
            	and tc.table_name = %s
            	and tc.constraint_name = %s;
            """

        logging.append('\nCheck foreign keys data')
        self.cur.execute(s1)
        constraint_names = [cname[0] for cname in self.cur.fetchall()]
        nc_names = 0
        if constraint_names:
            logging.append(f'{len(constraint_names):n} primary keys ' +\
                           'to examine')
            dfcolumns = list(self.data.columns)
            for cname in constraint_names:
                logging.append(f'Constrain name {cname}')
                self.cur.execute(s2, (self.schema, self.table, cname))
                fkrows = [row for row in self.cur.fetchall()]
                for i, row in enumerate(fkrows):
                    if i == 0:
                        columns = [row[0]]
                        ftschema = row[1]
                        fttable = row[2]
                        ftcolumns = [row[3]]
                    else:
                        columns.append(row[0])
                        ftcolumns.append(row[3])
                n = 0
                for column in columns:
                    if column not in dfcolumns:
                        n += 1
                        logging.append(f'The column {column} is not in ' +\
                                       'data columns')
                if n > 0:
                    nc_names += 1
                    logging.append(f'Constrain {cname} has not ' +\
                                   'required columns')
                    continue
                fk_table = f'{ftschema}.{fttable}'
                ndata_er = self.__check_data_in_fktable(columns, fk_table,
                                                        ftcolumns)
            if nc_names > 0:
                return False
            else:
                if ndata_er > 0:
                    return False
                else:
                    return True
        else:
            logging.append('The table has not foreign keys')
            return True


    def __check_data_in_fktable(self, data_col_names: [],
                                    fk_table: str, fk_col_names: []) -> int:
        """
        Checks if data file columns are in a foreign key table
        It cheks:
            if columns in foreign keys have not null values
            if columns in foreign keys have valid values
        """
        cols_in_select = ', '.join(fk_col_names)
        cols_in_where = [f'{col}=%s' for col in fk_col_names]
        cols_in_where = ', '.join(cols_in_where)
        s1 = \
            f"""
            select {cols_in_select}
            from {fk_table}
            where {cols_in_where}
            """
        self.data = self.data.where(pd.notnull(self.data), None)
        columns = [f"{col}" for col in data_col_names]
        n1 = n2 = 0
        for row in self.data.loc[:, columns].itertuples():
            if None in row:
                n1 += 1
                logging.append(f'Row {row[0]:n} has null values' +\
                               'in referenced columns', False)
                continue
            self.cur.execute(s1, row[1:])
            rows = [row for row in self.cur.fetchall()]
            if not rows:
                n2 += 1
                logging.append(f'Row {row[0]:n} has invalid values ' +\
                               f'in the table {fk_table}', False)
        if n1 > 0:
            logging.append(f'{n1:n} rows have null values in columns ', +\
                           f'{cols_in_select}')
        if n2 > 0:
            logging.append(f'{n2:n} rows have invalid values in columns ' +\
                           f'{cols_in_select}')
        return n2


    def __count_rows_2_insert_update(self, data_col_names: [],
                                     fk_table: str, fk_col_names: []) -> int:
        """
        Checks if data file columns are in a foreign key table
        """
        pk_columns = self.__primary_key(False)
        cols_in_select = ', '.join(pk_columns)
        cols_in_where = [f'{col}=%s' for col in pk_columns]
        cols_in_where = ', '.join(cols_in_where)
        s1 = \
            f"""
            select {cols_in_select}
            from {fk_table}
            where {cols_in_where}
            """
        data = self.data.where(pd.notnull(self.data), None)
        columns = [f"{col}" for col in pk_columns]
        ner, n2insert, n2update = 0
        for row in data.loc[:, columns].itertuples():
            if None in row:
                logging.append(f'la fila {row[0]:n} tiene valores nulos' +\
                               'en la clav primaria', False)
                ner += 1
                continue
            self.cur.execute(s1, row[1:])
            rows = [row for row in self.cur.fetchone()]
            if not rows:
                n2insert += 1
                logging.append(f'la fila {row[0]:n} se insertará', False)
        return ner



