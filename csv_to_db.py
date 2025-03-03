import pandas as pd
import json 

def load_config(file_name):
    # Load the config file
    with open(file_name) as f:
        return json.load(f)

config = load_config('config.json')
jobs = pd.read_csv(config["csv_path"])

jobs['applied'] = 0
jobs["hidden"] = 0
jobs["interview"] = 0
jobs["rejected"] = 0
jobs["interesting"] = 0
jobs["bad_location"] = 0

import sqlite3

def create_table(conn, df, table_name):
    ''''
    # Create a new table with the data from the dataframe
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print (f"Created the {table_name} table and added {len(df)} records")
    '''
    # Create a new table with the data from the DataFrame
    # Prepare data types mapping from pandas to SQLite
    type_mapping = {
        'int64': 'INTEGER',
        'float64': 'REAL',
        'datetime64[ns]': 'TIMESTAMP',
        'object': 'TEXT',
        'bool': 'INTEGER'
    }
    
    # Prepare a string with column names and their types
    columns_with_types = ', '.join(
        f'"{column}" {type_mapping[str(df.dtypes[column])]}'
        for column in df.columns
    )
    
    # Prepare SQL query to create a new table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        id INTEGER PRIMARY KEY,
        {columns_with_types}
    );
    """
    # create_table_sql = f"""
    #     CREATE TABLE IF NOT EXISTS "{table_name}" (
    #         {columns_with_types},
    #         PRIMARY KEY ("id")
    #     );
    #"""
    print(create_table_sql)
    
    # Execute SQL query
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    
    # Commit the transaction
    conn.commit()

    # Insert DataFrame records one by one
    insert_sql = f"""
        INSERT INTO "{table_name}" ({', '.join(f'"{column}"' for column in df.columns)})
        VALUES ({', '.join(['?' for _ in df.columns])})
    """
    for record in df.to_dict(orient='records'):
        cursor.execute(insert_sql, list(record.values()))
    
    # Commit the transaction
    conn.commit()

    print(f"Created the {table_name} table and added {len(df)} records")

def update_table(conn, df, table_name):
    # Update the existing table with new records.
    df_existing = pd.read_sql(f'select * from {table_name}', conn)

    # Create a dataframe with unique records in df that are not in df_existing
    df_new_records = pd.concat([df, df_existing, df_existing]).drop_duplicates(['id'], keep=False)
    df_new_records = pd.concat([df_new_records, df_existing, df_existing]).drop_duplicates(['title', 'company', 'date_posted'], keep=False)

    # If there are new records, append them to the existing table
    if len(df_new_records) > 0:
        df_new_records.to_sql(table_name, conn, if_exists='append', index=False)
        print (f"Added {len(df_new_records)} new records to the {table_name} table")
    else:
        print (f"No new records to add to the {table_name} table")

def table_exists(conn, table_name):
    # Check if the table already exists in the database
    cur = conn.cursor()
    cur.execute(f"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if cur.fetchone()[0]==1 :
        return True
    return False


def create_connection(db_path):
    # Create a database connection to a SQLite database
    conn = None
    try:
        conn = sqlite3.connect(db_path) # creates a SQL database in the 'data' directory
        #print(sqlite3.version)
    except Error as e:
        print(e)

    return conn


conn = create_connection(config["db_path"])
jobs_tablename = config["jobs_tablename"]

if conn is not None:
    #Update or Create the database table for the job list
    if table_exists(conn, jobs_tablename):
        update_table(conn, jobs[jobs.columns[1:]], jobs_tablename)
    else:
        create_table(conn, jobs[jobs.columns[1:]], jobs_tablename)

cursor = conn.cursor()
cursor.execute(f"SELECT COUNT(*) from {jobs_tablename}")
cursor.fetchone()



cursor.execute(f"SELECT DISTINCT job_level from {jobs_tablename} WHERE hidden!=1")
job_levels = cursor.fetchall()



for job_level in job_levels:
    cursor.execute(f"SELECT COUNT(*) from {jobs_tablename} WHERE hidden!=1 AND job_level='{job_level[0]}'")
    print(job_level, cursor.fetchall())

conn.commit()
cursor.close()
conn.close()


