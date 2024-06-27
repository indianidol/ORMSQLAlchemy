from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Define the base
Base = declarative_base()

# Define the LOB model
class LOB(Base):
    __tablename__ = 'lob'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)

# Replace with your actual RDS connection details
RDS_ENDPOINT = 'your-rds-endpoint.amazonaws.com'
RDS_PORT = '5432'
RDS_DB_NAME = 'your-db-name'
RDS_USERNAME = 'your-username'
RDS_PASSWORD = 'your-password'

# Create an engine and bind it to the session
db_url = f'postgresql+psycopg2://{RDS_USERNAME}:{RDS_PASSWORD}@{RDS_ENDPOINT}:{RDS_PORT}/{RDS_DB_NAME}'
engine = create_engine(db_url)

# Create all tables
Base.metadata.create_all(engine)

# Bind the engine to the session
Session = sessionmaker(bind=engine)
session = Session()

# Insert data
try:
    new_lob = LOB(name='Sample LOB')
    session.add(new_lob)
    session.commit()
    print("Data inserted successfully!")
except Exception as e:
    session.rollback()
    print(f"Error occurred: {e}")
finally:
    session.close()
