from sqlalchemy import (Table, Column, DateTime,
                        String, Integer, create_engine,
                        MetaData, Text, ForeignKey, Float)

meta = MetaData()

# create customers table
pexams = Table('phyexams', meta,
                Column('serviceno', String(64), primary_key=True),
                Column('cmscode', Integer(), nullable=True),
                Column('weight', Float(), nullable=True),
                Column('height', Float(), nullable=True),
                Column('heartrate', Integer(), nullable=True),
                Column('systolic', Integer(), nullable=True),
                Column('diastolic', Integer(), nullable=True),
                )

companies = Table('companies', meta,
                Column('company_id', Integer(), primary_key=True),
                Column('shortname', String(255), nullable=True),
                Column('fullname', String(255), nullable=False),
                )

customers = Table('customers', meta,
                Column('cmscode', Integer(), primary_key=True),
                Column('company_id', String(64)),
                Column('title', String(32)),
                Column('firstname', String(64)),
                Column('lastname', String(64)),
                Column('age', Integer()),
                Column('gender', String(1)),
                )

test = Table('tests', meta,
                Column('tcode', String(64), primary_key=True),
                Column('tname', String(128))
                )

prices = Table('prices', meta,
        Column('id', Integer(),
            primary_key=True, autoincrement=True),
        Column('tcode', String(16), ForeignKey('tests.tcode')),
        Column('price', Float(), nullable=False),
        Column('year', Integer(), nullable=False)
        )

services = Table('services', meta,
                Column('serviceno', String(64), primary_key=True),
                Column('servicedate', DateTime()),
                Column('company_id', String(64)),
                Column('customer_id', String(64)),
                Column('cmscode', Integer()),
                )

testresults = Table('testresults', meta,
                Column('no', String(64), primary_key=True),
                Column('serviceno', String(64)),
                Column('servicedate', DateTime()),
                Column('tcode', String(64)),
                Column('result', Text())
                )


# connect to heatlhdbstg: the health data staging database
engine = create_engine('postgresql+psycopg2://likit:password@localhost/healthdbstg')

meta.create_all(engine)  # create all tables
