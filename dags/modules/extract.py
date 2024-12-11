import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text


#function to get data 
def fetch_data(client, query):
    '''
    fetches query results from a clickhouse database and writes to a csv file

    Parameters:
    - client(clikhouse_connect.Client)
    - query (A SQL select query)

    Returns: None
    '''

    #execute the query
    output = client.query(query)
    rows = output.result_rows
    cols = output.column_names

    #close the client
    client.close()
    
    #write to pandas df and csv file
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv('./dags/raw_files/tripdata.csv', index=False)

    print(f'{len(df)} rows successfully extracted')


def get_last_loaded_date(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    result = session.execute(text('select max(pickup_date) from "Staging".tripdata'))
    max_date = result.fetchone()[0]
    session.close()
    
    return max_date
