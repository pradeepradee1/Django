from django.db import connection
from django import db
from sqlalchemy import create_engine
import os,hashlib

def create_table(table_name,df):
    try:
        if type(df)==dict:
            df=list(df.values())
            DROP_TABLE = "DROP TABLE IF EXISTS " + table_name + ";"
            CREATE_TABLE = "CREATE TABLE " + table_name + "("
            for column in df:
                CREATE_TABLE += "{} VARCHAR(15), ".format("`" + column+str('_ACCOUNT_OPERATIVENESSSTATUS')  + "`")
            CREATE_TABLE = CREATE_TABLE.rstrip(" ,")
            CREATE_TABLE += ") ENGINE=Aria"
            cursor = connection.cursor()
            cursor.execute(DROP_TABLE)
            cursor.execute(CREATE_TABLE)
        else:
            DROP_TABLE = "DROP TABLE IF EXISTS " + table_name + ";"
            CREATE_TABLE = "CREATE TABLE " + table_name + "("
            for column in df:
                CREATE_TABLE += "{} VARCHAR(12), ".format("`" + column  + "`")
            CREATE_TABLE = CREATE_TABLE.rstrip(" ,")
            CREATE_TABLE += ") ENGINE=Aria"
            cursor = connection.cursor()
            cursor.execute(DROP_TABLE)
            cursor.execute(CREATE_TABLE)            
            
    except db.DatabaseError as e:
        connection.rollback()
        raise e
    
    except Exception as e:
        raise e
    
    else:
        connection.commit()

def df_tosql(table_name,df):

    try:
        MYSQL_URL = os.getenv('MYSQL_URL')
        my_conn = create_engine(MYSQL_URL)
        df.to_sql(con=my_conn,name=table_name,if_exists='replace',index=False,method='multi')

    except db.DatabaseError as e:
        raise e
    
    except Exception as e:
        raise e

def get_values_from_tables(table_name):
    
    cursor = connection.cursor()
    cursor.execute("select * from "+str(table_name))
    myresult = cursor.fetchall()
    columnslist=[]
    for i in myresult:
        columnslist.append(i[0]) 
    return columnslist


def get_checksum(file):

    md5_hash = hashlib.md5()

    try:
        with open(file,"rb") as f:
            # Read and update hash in chunks of 4K
            for byte_block in iter(lambda: f.read(4096),b""):
                md5_hash.update(byte_block)
                model_checksum = md5_hash.hexdigest()
            return model_checksum
    except Exception as e:
        raise e

def get_file_size(file):

    try:
        file_size = os.path.getsize(file)
        file_size = "{:.2f}".format(file_size/(1024*1024)) +" MB"
    
    except Exception as e:
        raise e
        
    return file_size