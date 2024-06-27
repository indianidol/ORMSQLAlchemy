import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
import os

Base = declarative_base()

# Association table for many-to-many relationship between Steward and LOB
steward_lob_association = Table(
    'steward_lob', Base.metadata,
    Column('steward_id', Integer, ForeignKey('steward.id')),
    Column('lob_id', Integer, ForeignKey('lob.id'))
)

class Steward(Base):
    __tablename__ = 'steward'
    id = Column(Integer, primary_key=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    lobs = relationship('LOB', secondary=steward_lob_association, back_populates='stewards')

class LOB(Base):
    __tablename__ = 'lob'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    stewards = relationship('Steward', secondary=steward_lob_association, back_populates='lobs')
    sublobs = relationship('Sublob', back_populates='lob')

class Sublob(Base):
    __tablename__ = 'sublob'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    lob_id = Column(Integer, ForeignKey('lob.id'), nullable=False)
    lob = relationship('LOB', back_populates='sublobs')
    domain_urls = relationship('DomainURL', back_populates='sublob')

class XDS(Base):
    __tablename__ = 'xds'
    id = Column(Integer, primary_key=True)
    domain_url_id = Column(Integer, ForeignKey('domainurl.id'), nullable=False)
    permit_id = Column(Integer, nullable=False)
    share_id = Column(Integer, nullable=False)
    xds_id = Column(Integer, nullable=False)
    domain_url = relationship('DomainURL', back_populates='xds_entries')

class DomainURL(Base):
    __tablename__ = 'domainurl'
    id = Column(Integer, primary_key=True)
    domain_url = Column(String, nullable=False)
    domain_url_type = Column(String, nullable=False)
    sublob_id = Column(Integer, ForeignKey('sublob.id'), nullable=False)
    sublob = relationship('Sublob', back_populates='domain_urls')
    xds_entries = relationship('XDS', back_populates='domain_url')

# Create an engine and bind it to the session
current_file_dir = os.path.dirname(os.path.abspath(__file__))
csv_file_dir = os.path.join(current_file_dir, '..', 'resources')

db_file = os.path.join(current_file_dir, 'orm_database.db')
db_url = f'sqlite:///{db_file}'
engine = create_engine(db_url)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

def load_data_from_csv(file_path, model, valid_fields):
    try:
        df = pd.read_csv(file_path)
        for index, row in df.iterrows():
            record = model(**row.to_dict())
            session.add(record)
        session.commit()
    except Exception as e:
        print(f"Error loading data from {file_path}: {e}")
        session.rollback()

def load_association_data_from_csv(file_path):
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        steward = session.query(Steward).get(int(row['steward_id']))
        lob = session.query(LOB).get(int(row['lob_id']))
        if steward and lob and lob not in steward.lobs:
            steward.lobs.append(lob)
    session.commit()

# Add sample data from CSV files
if __name__ == "__main__":
    load_data_from_csv(os.path.join(csv_file_dir, 'stewards.csv'), Steward, ['id', 'first_name', 'last_name'])
    load_data_from_csv(os.path.join(csv_file_dir, 'lobs.csv'), LOB, ['id', 'name'])
    load_data_from_csv(os.path.join(csv_file_dir, 'sublobs.csv'), Sublob, ['id', 'name', 'lob_id'])
    load_data_from_csv(os.path.join(csv_file_dir, 'domainurls.csv'), DomainURL, ['id', 'domain_url', 'domain_url_type', 'sublob_id'])
    load_data_from_csv(os.path.join(csv_file_dir, 'xds.csv'), XDS, ['id', 'domain_url_id', 'permit_id', 'share_id', 'xds_id'])
    load_association_data_from_csv(os.path.join(csv_file_dir, 'steward_lob.csv'))

    # Query data
    for steward in session.query(Steward).all():
        print(steward.first_name, steward.last_name, [lob.name for lob in steward.lobs])

    for lob in session.query(LOB).all():
        print(lob.name, [sublob.name for sublob in lob.sublobs])

    for sublob in session.query(Sublob).all():
        print(sublob.name, sublob.lob.name, [url.domain_url for url in sublob.domain_urls])

    for domain_url in session.query(DomainURL).all():
        print(domain_url.domain_url, domain_url.domain_url_type, domain_url.sublob.name, [xds.xds_id for xds in domain_url.xds_entries])
