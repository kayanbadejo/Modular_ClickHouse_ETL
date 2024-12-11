# importing custom functions
from modules.helpers import get_client, get_postgres_engine
from modules.extract import fetch_data
from modules.load import load_csv_to_postgres
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

query = '''
        SELECT 
                  pickup_date,
                  vendor_id,
                  passenger_count,
                  trip_distance,
                  fare_amount,
                  payment_type
            FROM 
                  tripdata
            WHERE
                  year(pickup_date) = 2009 
                  AND month(pickup_date) = 1 
                  AND dayofmonth(pickup_date) = 1 
        '''

client = get_client()
engine = get_postgres_engine()





def main():
    '''
    Main function to run the data pipeline modules
    1. ------
    2. -----

    Paramters: None

    Returns: None
    '''

    #extract the data
    fetch_data(client=client, query=query)

    #load the data
    load_csv_to_postgres('tripdata.csv', 'tripdata', engine, 'Staging')
    
    # Execute Stored Procedure
    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute(text('CALL "Staging".prc_agg_tripdata()'))
    session.commit()
    
    print('Stored Procedure Executed Successfully')
    print('pipeline executed successfully')


if __name__ == '__main__':
    main()