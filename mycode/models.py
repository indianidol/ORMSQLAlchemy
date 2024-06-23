from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
import os
import pandas as pd

Base = declarative_base()

class LOB(Base):
    __tablename__ = 'lob'
    lob_id = Column(Integer, primary_key=True)
    name = Column(String)

    # One LOB can have many Sublobs
    sublobs = relationship('Sublob', back_populates='lob')

class Sublob(Base):
    __tablename__ = 'sublob'
    sublob_id = Column(Integer, primary_key=True)
    name = Column(String)
    lob_id = Column(Integer, ForeignKey('lob.lob_id'))

    # Relationships
    lob = relationship('LOB', back_populates='sublobs')
    stewards = relationship('Steward', back_populates='sublob')
    domain_urls = relationship('DomainURL', back_populates='sublob')

class Steward(Base):
    __tablename__ = 'steward'
    steward_id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    sublob_id = Column(Integer, ForeignKey('sublob.sublob_id'))

    # Relationships
    sublob = relationship('Sublob', back_populates='stewards')

class XDS(Base):
    __tablename__ = 'xds'
    xds_id = Column(Integer, primary_key=True)
    domain_url_id = Column(Integer, ForeignKey('domainurl.domain_url_id'))
    permit_id = Column(String)
    share_id = Column(String)

class DomainURL(Base):
    __tablename__ = 'domainurl'
    domain_url_id = Column(Integer, primary_key=True)
    domain_url = Column(String)
    domain_url_type = Column(String)
    sublob_id = Column(Integer, ForeignKey('sublob.sublob_id'))

    # Relationships
    sublob = relationship('Sublob', back_populates='domain_urls')
    xds_entries = relationship('XDS', backref='domainurl')

def load_data_from_csv(file_path, model):
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        record = model(**row.to_dict())
        session.add(record)
    session.commit()

# Create an engine and a session
# engine = create_engine('sqlite:///mydatabase.db')  # Change to your preferred database URL

current_file_dir = os.path.dirname(os.path.abspath(__file__))

db_file = os.path.join(current_file_dir,'self_database.db')

db_url=f'sqlite:///{db_file}'


engine  = create_engine(db_url) 
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Example of creating and adding entries
lob_entry = LOB(name='LOB 1')
sublob_entry = Sublob(name='Sublob 1', lob=lob_entry)
steward_entry = Steward(first_name='John', last_name='Doe', sublob=sublob_entry)
domain_url_entry = DomainURL(domain_url='http://example.com', domain_url_type='type1', sublob=sublob_entry)
xds_entry = XDS(domainurl=domain_url_entry, permit_id='permit1', share_id='share1')

session.add(lob_entry)
session.add(sublob_entry)
session.add(steward_entry)
session.add(domain_url_entry)
session.add(xds_entry)
session.commit()
