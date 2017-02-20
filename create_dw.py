from sqlalchemy import (create_engine, Table, Column,
                            Integer, Numeric, String, DateTime,
                            Boolean, ForeignKey)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, backref, relationship

#creating engine for a local postgresql database
dw_engine = create_engine('postgresql+psycopg2://likit@localhost/healthdw_dev')

Base = declarative_base()

class Fact(Base):
    __tablename__ = 'facts'
    fact_id = Column(Integer, primary_key=True)
    service_date_id = Column(Integer, ForeignKey('dates.date_id'))
    customer_id = Column(Integer, ForeignKey('customers.customer_id'))
    company_id = Column(Integer, ForeignKey('companies.company_id'))
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
    firstname = Column(String(64), index=True)
    lastname = Column(String(64), index=True)
    gender = Column(Integer) # 1=male, 2=female, 3=unknown
    age = Column(Integer)

class TextResult(Base):
    __tablename__ = 'textresults'
    id = Column(Integer, primary_key=True)
    result = Column(String(128))

Base.metadata.create_all(dw_engine)
