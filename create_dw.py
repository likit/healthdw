from sqlalchemy import (create_engine, Table, Column,
                            Integer, Numeric, String, DateTime,
                            Boolean, ForeignKey)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchem.orm import sessionmaker, backref, relationship

#creating engine for a local postgresql database
engine = create_engine('postgresql+psycopg2://likit@localhost/healthdw_dev')

Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

class Fact(Base):
    __tablename__ = 'facts'
    fact_id = Column(Integer, primary_key=True)
    service_date_id = Column(Integer, ForeignKey('dates.date_id'))
    customer_id = Column(Integer, ForeignKey('customers.customer_id'))
    customer = relationship('Customer', backref=backref('tests'))
    test_id = Column(Integer, ForeignKey('tests.test_id'))
    num_value = Column(Numeric())
    text_value = Column(Integer, ForeignKey('textresults.id'))

class Company(Base):
    __tablename__ = 'companies'
    company_id = Column(Integer, primary_key=True)
    name = Column(String(128), index=True)
    subdistrict = Column(String(64))
    district = Column(String(64))
    province = Column(String(64))
    country = Column(String(64))
    zipcode = Column(String(8))
    

class Date(Base):
    __tablename__ = 'dates'
    date_id = Column(Integer, primary_key=True)
    day = Column(Integer)
    month = Column(Integer)
    quarter = Column(Integer)
    day_no = Column(Integer)
    gregorian_year = Column(Integer)
    fiscal_year = Column(Integer)
    
class Test(Base):
    __tablename__ = 'tests'
    test_id = Column(Integer, primary_key=True)
    code = Column(String(32), index=True)
    fullname = Column(String(64))


class Customer(Base):
    __tablename__ = 'customers'
    customer_id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.company_id'))
    company = relationship('Company', backref=backref('customers'))
    firstname = Column(String(64), index=True)
    lastname = Column(String(64), index=True)
    gender = Column(Integer) # 0=male, 1=female, 2=unknown
    age = Column(Integer)

class TextResult(Base):
    __tablename__ = 'testresults'
    id = Column(Integer, primary_key=True)
    result = Column(String(128))

Base.metadata.create_all(engine)