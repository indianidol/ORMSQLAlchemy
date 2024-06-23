import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, LOB, Sublob, Steward, DomainURL, XDS  # Assuming models are defined in models.py
import os
# Create an engine and a session
current_file_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_dir =os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','resources')
print(csv_file_dir)

db_file = os.path.join(current_file_dir,'orm_database.db')

db_url=f'sqlite:///{db_file}'


engine  = create_engine(db_url)  # Change to your preferred database URL
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# Function to load data from CSV and insert into database
def load_data_from_csv(file_path, model):
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        record = model(**row.to_dict())
        session.add(record)
    session.commit()

# Load data into tables
load_data_from_csv(os.path.join(csv_file_dir,'LOB.csv'), LOB)
load_data_from_csv(os.path.join(csv_file_dir,'Sublob.csv'), Sublob)
load_data_from_csv(os.path.join(csv_file_dir,'Steward.csv'), Steward)
load_data_from_csv(os.path.join(csv_file_dir,'DomainURL.csv'), DomainURL)
load_data_from_csv(os.path.join(csv_file_dir,'XDS.csv'), XDS)

print("Data inserted successfully!")
