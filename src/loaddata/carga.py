import os
import json
import pandas as pd
import unicodedata
import re
import hashlib
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import text, create_engine


class LoadData():
    '''Utiliza diferentes métodos para cargar datos en PostgreSQL, instanciar variables para mejorar el flujo de 
    desarrollos internos y comprobar los datos que van a ser cargados.
    '''

    def __init__(self):
        pass
    
    def enginepsql(self):
        engine = create_engine(
            f'postgresql+psycopg2://'+ os.getenv('USUARIO')+':'+ os.getenv('PASSWORD')+'@'+os.getenv('HOSTNAME')+':'+os.getenv('PORT')+'/'+os.getenv('DATABASE'),
            connect_args={'connect_timeout': 10}
        )

        return engine

    def truncate_table(self, schema:str, table:str):
        """Elimina todos los datos de una tabla sin borrar su estructura y sin romper las vistas dependientes.
        
        Parámetros
        ----------
        schema : str
            Nombre del esquema en la base de datos PostgreSQL donde se encuentra la tabla. 
        table : str
            Nombre de la tabla a truncar.
        """

        engine = self.enginepsql()
        
        with engine.begin() as connection: 
            try:
                # Desactivar temporalmente las restricciones (foreign keys)
                connection.execute(text(f"ALTER TABLE {schema}.{table} DISABLE TRIGGER ALL;"))
                
                # Truncar la tabla (solo borra datos, no esquema)
                connection.execute(text(f"TRUNCATE TABLE {schema}.{table};")) # CASCADE
                
                # Reactivar las restricciones
                connection.execute(text(f"ALTER TABLE {schema}.{table} ENABLE TRIGGER ALL;"))
                
                print(f"Tabla '{schema}.{table}' truncada sin romper las vistas.")

            except Exception as e:
                print(f"Error al truncar la tabla '{schema}.{table}':", e)

    def unique_id_(self):
        '''Genera id único para el dataframe que se le pasa y 
        los campos con los que hacerla.
        '''

        # para pasar el unique id a formato numero
        def text_tonum(text: str) -> int:
            """Convierte un string en un número entero único y reproducible.
            """
            # Genera un hash SHA256 del texto
            h = hashlib.sha256(text.encode('utf-8')).hexdigest()
            # Toma los primeros 16 caracteres del hash (64 bits)
            return int(h[:16], 16)
        
        if not isinstance(self.input_table, pd.DataFrame):
            raise ValueError('unique_id no ha recibido un pandas.DataFrame')
            
        # cuando se especifica id único
        # \\ HERE
        if self.uid_need is not None:
        #    self.input_table['unique_id'] = self.input_table[self.uid_need]
           return
        
        # de lo contrario
        cols_id = self.uid_cols

        # valida que las columnas que se especifican existen en el dataframe
        for col in cols_id:
            if col not in self.input_table.columns:
                raise ValueError(f"La columna '{col}' no existe en el DataFrame")

        temp_df = self.input_table[cols_id].copy()

        # quitamos los nulos (lo hacemos en una tabla temporal para evitar quitar nulos ni alterar la tabla original)
        # convertir todos los tipos de columnas a string de forma segura
        for col in temp_df.columns:

            if pd.api.types.is_categorical_dtype(temp_df[col]):
                
                # agrega '' como categoría si no está
                if '' not in temp_df[col].cat.categories:
                    
                    temp_df[col] = temp_df[col].cat.add_categories([''])

                temp_df[col] = temp_df[col].fillna('').astype(str)

            elif pd.api.types.is_numeric_dtype(temp_df[col]):
               
                temp_df[col] = temp_df[col].fillna(0).astype(str)

            else:
                
                temp_df[col] = temp_df[col].fillna('').astype(str)

        # generamos el unique id limpio
        temp_str = (
            temp_df
            .agg("_".join, axis=1)
            .str.lower()
            .apply(lambda x: unicodedata.normalize("NFKD", x)
                            .encode("ascii", "ignore")
                            .decode("utf-8"))
            .apply(lambda x: re.sub(r'[^a-z0-9]', '', x))
        )

        # pasamos unique id a formato numero y lo instanciamos en nuestro dataframe
        self.input_table['unique_id'] = temp_str.apply(text_tonum).apply(lambda x: x % (2**63)).astype('int64')

     
    def load_all_data(self, input_table: pd.DataFrame, output_table:str, uid_cols:list=None, uid_need:str=None, truncate:bool=False):

        '''Comprueba los datos que se intentan cargar en base de datos. Por defecto, realiza una carga incremental. Pero  si se especifica truncate en True, se realiza truncado de\
            la tabla. En este último caso, no es necesario especificar ni uid_cols ni uid_need.
        
        params:
        ----------

        input_table: DataFrame
            La variable instanciada en el script que contiene la información que se quiere cargar.
        output_table: str
            El nombre de la tabla, si existe, en base de datos. 
            Se espera del tipo 'schema_name.table_name'.
        uid_cols: list
            Las columnas a utilizar para crear un id único en caso de que la tabla no tenga. 
        uid_need: str
            El nombre de la columna que es el id único. Si es None, es necesario especificar uid_col para generar una clave. Debe tenerse en cuenta que, 
            si se especifica un id único debe asegurarse  que tenga el tipo correcto para no tener problemas a la hora de realizar el merge. Por defecto es None.
        truncate: bool
            Si se especifica True, entonces se trunca la tabla. Por defecto es False.
        '''

        if uid_cols is None:
            if uid_need is None:
                if truncate is False:
                    raise ValueError("Debes especificar las columnas únicas para crear el id único (uid_cols) o pasar el nombre de la columna del id que ya exista (uid_need)\
                                     en caso de que quiera carga incremental, o establecer truncate True para hacer carga por truncado.")
        else:
            if not isinstance(uid_cols, list):
                raise ValueError("uid_cols debe ser de tipo lista.")

        self.input_table = input_table
        self.uid_cols = uid_cols
        self.datos_aniadidos = 0
        self.uid_need = uid_need
        

        engine_ = self.enginepsql()

        schema_table = output_table.split('.')
            
        if len(schema_table) == 2:

            schema_name = schema_table[0]

            table_name = schema_table[1]
            
            consulta = f'''SELECT * FROM {output_table}'''

        else: 
            raise ValueError(f"El nombre de la tabla en base de datos no es del tipo 'schema.table'.")
            

        # si existe la tabla
        try:
            datos_bbdd = pd.read_sql(consulta, con=engine_)

            self.datos_bbdd = datos_bbdd
            
            if not truncate: # si truncate es False

                # entonces queremos realizar carga incremental
                print('No se ha especificado truncado de la tabla. Se asume carga incremental.')

                # actualizamos input_table con el id único
                self.unique_id_()
                
                # Carga incremental 
                # rows_filter = set(datos_bbdd['unique_id'].unique())

                # self.input_table = self.input_table[~self.input_table['unique_id'].isin(rows_filter)]
                # \\ HERE
                if not self.uid_need:
                    merged = self.input_table.merge(
                        self.datos_bbdd[['unique_id']],
                        on='unique_id',
                        how='left',
                        indicator=True
                    )

                else:
                    merged = self.input_table.merge(
                        self.datos_bbdd[[self.uid_need]],
                        on=self.uid_need,
                        how='left',
                        indicator=True
                    )

                self.input_table = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

                self.input_table = self.input_table.reset_index(drop=True)

                datos_aniadidos = len(self.input_table)

            else:
                print(f'Se va a truncar la tabla {schema_name}.{table_name}')
                self.truncate_table(schema_name, table_name)

                datos_aniadidos = len(self.input_table)


            self.input_table.to_sql(name=table_name, schema=schema_name, con=engine_, if_exists='append', index=False)

        except Exception as e:
            
            # solo la primera línea del error, porque si no imprimo muchas líneas
            mensaje = str(e).split("\n")[0] 

            print(f'Hubo un problema al acceder a la tabla en base de datos. {mensaje}')
            
            msg = str(e).lower()
            
            # en caso de no existir la tabla
            if "does not exist" in msg or "invalid object name" in msg or "no such table" in msg:
                
                if isinstance(self.input_table, pd.DataFrame):
                    self.input_table.columns = self.input_table.columns.str.lower()

                    # solo creamoms unique id si no truncamos tabla
                    if not truncate:
                    
                        self.unique_id_()

                    # Carga inicial
                    datos_aniadidos = len(self.input_table)

                    print(f'Creando tabla {output_table} e insertando los datos en bbdd.')
                    self.input_table.to_sql(name=table_name, schema=schema_name, con=engine_)

            else:
                raise e
            
        self.datos_aniadidos = datos_aniadidos

        self.reportar_datos_aniadidos()
                
    def reportar_datos_aniadidos(self):
        '''Reporta los datos añadidos por consola.
        '''

        print("===DATA_ANIADIDOS===")
        if isinstance(self.datos_aniadidos, int):
            print(json.dumps({"datos_aniadidos":self.datos_aniadidos}))
        print("===FIN_DATA_ANIADIDOS===")

    def generar_rango_fechas(self, fecha_referencia=None, años_atras=2):
        """
        Genera start_date y end_date basados en una fecha de referencia o la fecha actual.
        Devuelve las fechas en el mismo formato detectado o en ISO por defecto.

        Parámetros:
            fecha_referencia (str | datetime | date | None): Fecha base. Si es None, se usa hoy.
            años_atras (int): Años que se restan a la fecha de referencia para el start_date.
        
        Retorna:
            tuple: (start_date_str, end_date_str)
        """

        # Si no se pasa fecha, usamos la actual
        hoy = date.today() if fecha_referencia is None else self._parse_fecha(fecha_referencia)

        # Calculamos el rango
        start_date = hoy - relativedelta(years=años_atras)
        end_date = hoy

        # Detectamos el formato de la fecha original (si era string)
        if isinstance(fecha_referencia, str):
            fmt = self._detectar_formato(fecha_referencia)
            return (start_date.strftime(fmt), end_date.strftime(fmt))
        else:
            # Si no se pasó string, devolvemos ISO
            return (start_date.isoformat(), end_date.isoformat())

    def _parse_fecha(self, fecha):
        """
        Intenta parsear una fecha en distintos formatos comunes.
        Devuelve un objeto datetime.date.
        """
        if isinstance(fecha, (datetime, date)):
            return fecha if isinstance(fecha, date) else fecha.date()

        formatos = [
            "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y",
            "%Y-%m", "%Y/%m", "%m-%Y", "%m/%Y",
            "%y-%m-%d", "%d-%m-%y", "%y/%m/%d", "%d/%m/%y",
        ]
        for fmt in formatos:
            try:
                return datetime.strptime(fecha, fmt).date()
            except ValueError:
                continue

        raise ValueError(f"No se reconoce el formato de fecha: {fecha}")

    def _detectar_formato(self, fecha_str):
        """
        Detecta el formato más probable de una fecha string.
        """
        posibles_formatos = [
            "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y",
            "%Y-%m", "%Y/%m", "%m-%Y", "%m/%Y",
            "%y-%m-%d", "%d-%m-%y", "%y/%m/%d", "%d/%m/%y",
        ]
        for fmt in posibles_formatos:
            try:
                datetime.strptime(fecha_str, fmt)
                return fmt
            except ValueError:
                continue
        return "%Y-%m-%d"  # por defecto

