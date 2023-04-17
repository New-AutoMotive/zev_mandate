#Version updated on 16th Feb 2023

import pandas as pd
import os
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
from shapely import wkt
import json
import psycopg2
import sqlalchemy
import io 
import geopandas as gpd
import slack_sdk as slack
from datetime import datetime

class SlackBot:

    def __init__(self,token_file = './credentials/slack.json',
                 slack_channel = '#driveways'
                 ):
        
        with open(token_file) as json_file:
                params = json.load(json_file)
        self.token = params['SLACK_TOKEN']
        self.client = slack.WebClient(token=self.token)
        self.channel_name = slack_channel
        return 

    def send_log(self,text):

        self.client.chat_postMessage(channel=self.channel_name, 
                                     text=text
                                     )
        return 

class MyLogger:

    def __init__(self,
                folder = 'gardens/',
                file_name = 'general_log.txt',
                slack_channel = '#aa-scraper'):

        self.file_name = os.path.join(folder,file_name)
        try:
            os.makedirs(folder)
        except:
            pass
        self.sb = SlackBot(slack_channel = slack_channel)
        return 
    
    def write_log(self,text,Slack =True):

        time_log = datetime.now().strftime('[%Y-%m-%d @ %H:%M] - ')
        text = time_log + text + '\n'

        with open(self.file_name, 'a') as f:
            f.write(text)
        if(Slack):
            self.sb.send_log(text)
        return 

class MyBucket:
    
    def __init__(self,
                bucket_name='eu_csv',
                credentials_file = './credentials/New AutoMotive Index-487e031dc242.json',
                ):
        self.storage_client = storage.Client.from_service_account_json(credentials_file)
        self.bucket_name =  bucket_name
        self.bucket = self.storage_client.get_bucket(self.bucket_name)
        print('Bucket ' + self.bucket_name + ' successfully found')

    def show_files_in_folder(self,remote_folder):
        file_list = list(self.bucket.list_blobs(prefix=remote_folder))
        if(not file_list):
            raise FileNotFoundError(f'Folder {remote_folder} not found in {self.bucket_name}.')
        return file_list

    def get_pandas_csv(self,
                       file_name,
                       download = False,
                       local_path = './',
                       sep = ","):

        file = self.bucket.blob(file_name)
        data = file.download_as_string()
        if(download):
            file.download_to_filename(os.path.join(local_path,file_name.split('/')[-1]))
        return pd.read_csv(io.BytesIO(data),sep=sep,on_bad_lines='skip')
    
    def upload_file_to_bucket(self,
                              path_file, 
                              destination_blob_name
                              ):

        file_name = path_file.split('/')[-1]
        destination_path = os.path.join(destination_blob_name,file_name)
        blob = self.bucket.blob(destination_path)
        blob.upload_from_filename(path_file)
        print(f"File {path_file} uploaded to {destination_path}.")
        return 

    def download_files(self,
                       prefix_file,
                       local_path,
                       format = '.json',
                       file_target = None
                       ):

        os.makedirs(local_path,exist_ok=True)
        blobs = self.bucket.list_blobs(prefix=prefix_file)
        for blob in blobs:
            file_name = blob.name.split('/')[-1]
            if(not file_target):
                if(file_name.endswith(format)):#downlaod all files with a specific format
                    blob.download_to_filename(local_path+file_name)
                    print('Blob {} downloaded to {}.'.format(blob.name, local_path))
            else:
                if(file_name == file_target):#downlaod only a specific file

                    blob.download_to_filename(local_path+file_name)
                    print('Blob {} downloaded to {}.'.format(blob.name, local_path))

        return 

class MySQL:

    def __init__(self,
                db,
                credentials_files='./credentials/local_credentials.json',#read credentials servers mysql from this json file
                GCR = True
                ):

        with open(credentials_files) as json_file:
            params = json.load(json_file)
        self.db_user = params['user']
        self.db_pass = params['password']
        self.db_name = db
        self.db_host = params['host']
        self.cloud_sql_connection_region = params['cloud_sql_connection_region']
        self.cloud_sql_connection_name = params['cloud_sql_connection_name']

        self.GCR = GCR
        try:
            self.db_connection()
            print(f'Connected to {self.db_name} database at {self.db_host}!\nDeployment in GCR: {self.GCR}\n')
        except Exception as e:
            print(f"Impossible to connect to to {self.db_name} database at {self.db_host}.\nERROR: {e}")
        return 

    def db_connection(self):

        if(not self.GCR):
            engine = sqlalchemy.create_engine(
                sqlalchemy.engine.url.URL.create(
                    drivername="mysql+pymysql",
                    username=self.db_user,  # e.g. "my-database-user"
                    password=self.db_pass,  # e.g. "my-database-password"
                    database=self.db_name,  # e.g. "my-database-name"
                    host=self.db_host,
                ),
            )

        else:
            engine = sqlalchemy.create_engine(
                sqlalchemy.engine.url.URL.create(
                    drivername="mysql+pymysql",
                    username=self.db_user,  # e.g. "my-database-user"
                    password=self.db_pass,  # e.g. "my-database-password"
                    database=self.db_name,  # e.g. "my-database-name"
                    host=self.db_host,
                    query={
                        "unix_socket": "{}/{}".format(
                            "/cloudsql",  # e.g. "/cloudsql"
                            f"rugged-baton-283921:{self.cloud_sql_connection_region}:{self.cloud_sql_connection_name}")#in GCR you need to specify the region and the istance name too
                    }
                ),
            )

        return engine.connect()

    
    def show_db(self):
        
        return self.from_sql_to_pandas(sql_query="""SHOW DATABASES;""")
    
    def show_tables(self,db=None):
        
        if(not db):
            db = self.db_name
        print(db)
        query_columns = f"""SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_ROWS,CREATE_TIME,UPDATE_TIME FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{db}'
        ORDER BY UPDATE_TIME;
        """
        return self.from_sql_to_pandas(sql_query=query_columns)

    def show_columns(self,table):

        query = f'SHOW COLUMNS from {table};'
        return self.from_sql_to_pandas(query)

    def run_query(self,query):
        with self.db_connection() as conn:
            res = conn.execute(query)
        try:
            return res.fetchall()
        except Exception as e:
            print('Nothing to return!')
            return 
    def db_create_table_from_csv(self,table_name = 'mlResults',
                                file_path=f'../analysis/Wakefield/properties_Wakefield.csv',
                                columns=False,
                                rename_columns_dict=False):
        csv_df = pd.read_csv(file_path)
        if(rename_columns_dict):
            csv_df = csv_df.rename(columns=rename_columns_dict)
        if(columns):
            csv_df = csv_df[columns]
        print(f'Creating {table_name} from {file_path} ...\n\n')
        try: 
            with self.db_connection() as conn:
                csv_df.to_sql(table_name, conn, if_exists='fail', chunksize=1000,index= False)
                return True
        except ValueError:
            print(f"ERROR: {table_name} already exists! Create a new table or use use the method db_update_table_from_csv(table_name,file_path)")
            return False


    def db_update_table_from_csv(self,
                                table_name,
                                file_path,
                                create_first=True,
                                columns=False,
                                rename_columns_dict=False):
        
        if(create_first):
            if(self.db_create_table_from_csv(table_name=table_name,file_path=file_path,columns=columns,rename_columns_dict=rename_columns_dict)):
                return 
        print(f"Updating {table_name} importing {file_path} ...\n\n")
        df_updates = pd.read_csv(file_path)
        if(rename_columns_dict):
            df_updates = df_updates.rename(columns=rename_columns_dict)
        if(columns):
            df_updates = df_updates[columns]
        with self.db_connection() as conn:
            df_updates.to_sql(f"""{table_name}""", conn, if_exists='append', chunksize=1000,index= False)
        return 

    def from_sql_to_pandas(self,sql_query = f"SELECT * FROM mlResults LIMIT 20;",geometry = False):
        
        sql_df = pd.read_sql(sql=sql_query,con=self.db_connection())
        if(geometry):
            sql_df[geometry] = gpd.GeoSeries.from_wkt(sql_df[geometry])
            sql_df = gpd.GeoDataFrame(sql_df,geometry=geometry)
        return sql_df 
    
class MyPostgres:

    def __init__(self,credentials_file='./credentials/driveways-postgres.json',
                ):
        with open(credentials_file) as json_file:
                params = json.load(json_file)
        self.db_user = params['user']
        self.db_pass = params['password']
        self.db_name = params['database']
        self.db_host = params['host']

        self.con = psycopg2.connect(
                                    host=self.db_host,
                                    database=self.db_name,
                                    user=self.db_user,
                                    password=self.db_pass
                                    )
        return 

    
    def show_tables(self):

        con = psycopg2.connect(
                                    host=self.db_host,
                                    database=self.db_name,
                                    user=self.db_user,
                                    password=self.db_pass
                                    )
        
        cur = con.cursor()
        cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
        return(pd.DataFrame(cur.fetchall(),columns=['table_name']))


    def show_columns(self,table='topographicarea'):
        
        con = psycopg2.connect(
                                host=self.db_host,
                                database=self.db_name,
                                user=self.db_user,
                                password=self.db_pass
                                )
        cur = con.cursor()
        try:
            cur.execute(f"Select * FROM {table} LIMIT 0")
            colnames = [desc[0] for desc in cur.description]
        except:
            print(f'Table {table} does not exist!')
            return 
        return pd.DataFrame(colnames,columns=['column_name'])

    def run_query(self,query):
        
        con = psycopg2.connect(
                                    host=self.db_host,
                                    database=self.db_name,
                                    user=self.db_user,
                                    password=self.db_pass
                                    )
        cur = con.cursor()
        cur.execute(query=query)
        return cur.fetchall()

    def from_postgres_to_geopandas(self,sql,geom_col,crs='epsg:2770'):
        
        con = psycopg2.connect(
                                    host=self.db_host,
                                    database=self.db_name,
                                    user=self.db_user,
                                    password=self.db_pass
                                    )
        
        return gpd.read_postgis(sql=sql, con=con, crs=crs, geom_col=geom_col)

class MyBigQuery:

    def __init__(self,
                 credentials_file = './credentials/New AutoMotive Index-487e031dc242.json',
                ):

        self.project_id = 'rugged-baton-283921'
        self.bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file(credentials_file), 
                                        project=self.project_id
                                        )

    def from_bq_to_dataframe(self,query):

        query_job = self.bq_client.query(query)
        df = query_job.to_dataframe()
        return df 
        
    def get_info_schema(self,schema='mots_uk'):
         
         return self.from_bq_to_dataframe(query=f"SELECT * FROM {self.project_id}.{schema}.INFORMATION_SCHEMA.TABLES;")[['table_catalog','table_schema','table_name','creation_time']]
          
    def get_info_table(self,schema='mots_uk',table_name='vehicles_old'):
        
        return self.from_bq_to_dataframe(query=f"SELECT * FROM {self.project_id}.{schema}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS WHERE table_name='{table_name}';")[['table_schema','table_name','column_name','data_type']]

    def upsert_from_df(self,
                    table_name,
                    df,
                    dataset_name='mots_uk',
                    job_config=None,
                    unique_fields=None):
        """
        This method allows to create/update in many ways:
        1. if the table does not exist, it creates a new one using job_config if specified
        2. if the table already exists, it appends the content. A list of already existing columns can be
           passed and the method will not append duplicated rows in the fields, i.e. do not appends 
           new rows having already exixsting values for the columns specified.
        """
        table_ref = self.bq_client.dataset(dataset_name).table(table_name)

        # Check if table already exists
        table_exists = table_name in self.get_info_schema(schema=dataset_name).table_name.to_list()

        # If table exists and merge=True, update the existing table; otherwise, create a new table
        if table_exists:
            print("Table exixsts. Upserting ...")
            job_config = job_config or bigquery.LoadJobConfig()
            job_config.write_disposition = 'WRITE_APPEND'
            job_config.schema_update_options = ['ALLOW_FIELD_ADDITION']
            if unique_fields is not None:
                columns = ','.join(unique_fields)
                query = f'SELECT DISTINCT {columns} FROM `{dataset_name}.{table_name}`'
                df_query = self.from_bq_to_dataframe(query)
                df=pd.merge(df,df_query,on=unique_fields, how='outer', indicator=True).query("_merge == 'left_only'").drop('_merge', axis=1).reset_index(drop=True)
                print(f"Size updates to insert {len(df)}")
        else:
            print("Table does not exist")
            if job_config:
                job_config.write_disposition = 'WRITE_TRUNCATE'
            else:
                job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')

        # If an id column is specified, query the table to get the existing ids


        # Load the dataframe into BigQuery
        bigquery_job = self.bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        bigquery_job.result()
        return