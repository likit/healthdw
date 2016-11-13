from sqlalchemy import (Table, Column, String, Integer,
                        create_engine, MetaData)

meta = MetaData()

# create customers table

customers = Table('customers', meta,
                Column('custid', Integer(), primary_key=True),
                Column('shortname', String(255), nullable=True),
                Column('fullname', String(255), nullable=False))

# connect to heatlhdbstg: the health data staging database
engine = create_engine('postgresql+psycopg2://likit:password@localhost/healthdbstg')

meta.create_all(engine)  # create all tables
