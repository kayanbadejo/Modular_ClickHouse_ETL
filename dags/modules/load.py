import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text


def load_csv_to_postgres(table_name, engine, schema):
    '''
    Loads data from a csv file to pandas dataframe and then to a postgres DB table

    Parameters:
    - csv_file_path (str): Path to csv file
    - table_name (str): a postgres db table
    - engine (sqlalchmey.egine): an SQL alchemy engine object
    - schema (str): a postgres DB schema
    '''
    # read csv to pandas and to sql
    df = pd.read_csv('./dags/raw_files/tripdata.csv')
    df.to_sql(table_name, con=engine, if_exists='replace', index=False, schema=schema)

    print(f'{len(df)} rows loaded to staging successfully')
    
    
    
    
def exec_procedure(engine):
    
    ## execute stored procedure
    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute(text('CALL "Staging".prc_agg_tripdata()'))
    session.commit()

    print('Stored Procedure Executed')
    
    print('Pipeline Executed Sucessfully')
