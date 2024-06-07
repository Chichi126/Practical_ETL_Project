import pandas as pd
import numpy as np
from glob import glob
from sqlalchemy import create_engine


#Extracting of the dataset from the source
def extract_data():
    data = []
    file = sorted(glob(r'dags/grade*.xlsx')) 
    for i in file:
       files = pd.read_excel(i)
       data.append(files)
    combined_df = pd.concat(data, ignore_index=True) # to change the orignal indexing of the dataset into proper indexing
    df = pd.DataFrame(combined_df) # change the dataset into a dataframe
    return df
#print(extract_data())

def transform_data():
    df = extract_data()
    df['first_name'] = df['first_name'].str.capitalize() # to capitalize the first letter in the names column
    df['last_name'] = df['last_name'].str.capitalize()
    df = df.dropna()  #to drop the null values
    df['score'] = df['score'].astype(int) #converting the score column data type
    # df.columns =['ID','First_name', 'Last_name', 'Grade', 'Score', 'Is_Pass'] #using the .rename function
    df['is_pass'] = np.where(df['score'] >= 50, 'Pass' , 'Fail') #creating a new column and passing values
    df = df.drop_duplicates('id') # droping a duplicated value in a column
    
    clean_data = pd.DataFrame(df)
    return clean_data
#print(transform_data())

# uploading a copy of the combined file into my local environment 
def load_data():
    df= transform_data()
    df.to_csv(r'dags/combined_grade.xlsx')
load_data()
print('data loaded successfully')

  

# connect_to_sql()
def load_data_to_postgresql(username: str, password: str, host: str, port: str, dbname: str, table_name: str, schema_name: str):
        
        # Create the database engine
        engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}')
        conn = engine.connect()
        df = transform_data()
        df.to_sql(table_name, con=conn, index = True, if_exists= 'replace', schema =schema_name )

load_data_to_postgresql(
    username='chichi',
    password='simple',
    host='localhost',
    port='5432',
    dbname='simple_etl',
    table_name='grade_sheet_raw2',
    schema_name='simple_demo'
)

print('Data loaded successfully')







