import os
import pytest
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, LOB, Sublob, Steward, DomainURL, XDS

# Define the base path for the CSV files
base_path = os.path.join(os.path.dirname(__file__), '..', 'resources')

@pytest.fixture(scope='module')
def test_engine():
    # Use an in-memory SQLite database for testing
    engine = create_engine('sqlite:///:memory:')
    Base.metadata.create_all(engine)
    return engine

@pytest.fixture(scope='module')
def session(test_engine):
    Session = sessionmaker(bind=test_engine)
    session = Session()
    yield session
    session.close()

def load_data_from_csv(file_path, model, session):
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        record = model(**row.to_dict())
        session.add(record)
    session.commit()

def test_load_lob(session):
    load_data_from_csv(os.path.join(base_path, 'LOB.csv'), LOB, session)
    result = session.query(LOB).all()
    assert len(result) == 2
    assert result[0].name == 'LOB 1'
    assert result[1].name == 'LOB 2'

def test_load_sublob(session):
    load_data_from_csv(os.path.join(base_path, 'Sublob.csv'), Sublob, session)
    result = session.query(Sublob).all()
    assert len(result) == 3
    assert result[0].name == 'Sublob 1'
    assert result[1].name == 'Sublob 2'
    assert result[2].name == 'Sublob 3'
    assert result[0].lob_id == 1
    assert result[1].lob_id == 1
    assert result[2].lob_id == 2

def test_load_steward(session):
    load_data_from_csv(os.path.join(base_path, 'Steward.csv'), Steward, session)
    result = session.query(Steward).all()
    assert len(result) == 3
    assert result[0].first_name == 'John'
    assert result[1].first_name == 'Jane'
    assert result[2].first_name == 'Jim'
    assert result[0].last_name == 'Doe'
    assert result[1].last_name == 'Smith'
    assert result[2].last_name == 'Brown'
    assert result[0].sublob_id == 1
    assert result[1].sublob_id == 2
    assert result[2].sublob_id == 3

def test_load_domain_url(session):
    load_data_from_csv(os.path.join(base_path, 'DomainURL.csv'), DomainURL, session)
    result = session.query(DomainURL).all()
    assert len(result) == 3
    assert result[0].domain_url == 'http://example.com'
    assert result[1].domain_url == 'http://example2.com'
    assert result[2].domain_url == 'http://example3.com'
    assert result[0].domain_url_type == 'type1'
    assert result[1].domain_url_type == 'type2'
    assert result[2].domain_url_type == 'type1'
    assert result[0].sublob_id == 1
    assert result[1].sublob_id == 2
    assert result[2].sublob_id == 3

def test_load_xds(session):
    load_data_from_csv(os.path.join(base_path, 'XDS.csv'), XDS, session)
    result = session.query(XDS).all()
    assert len(result) == 3
    assert result[0].permit_id == 'permit1'
    assert result[1].permit_id == 'permit2'
    assert result[2].permit_id == 'permit3'
    assert result[0].share_id == 'share1'
    assert result[1].share_id == 'share2'
    assert result[2].share_id == 'share3'
    assert result[0].domain_url_id == 1
    assert result[1].domain_url_id == 2
    assert result[2].domain_url_id == 3
