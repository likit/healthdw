from create_dw import dw_engine
from sqlalchemy import Table, create_engine, cast, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

# create source db engine
db_engine = create_engine('postgresql+psycopg2://likit:password@localhost/healthdbstg')

DWSession = sessionmaker(bind=dw_engine)
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()
dw_session = DWSession()

DBBase = automap_base()
DBBase.prepare(db_engine, reflect=True)
print(DBBase.classes.keys())

services = DBBase.classes.services
companies = DBBase.classes.companies
customers = DBBase.classes.customers
tests = DBBase.classes.tests
testresults = DBBase.classes.testresults
prices = DBBase.classes.prices
phyexams = DBBase.classes.phyexams

def load():
    # query and load company data to staging database
    query = db_session.query(services, phyexams, customers, companies)
    query = query.join(phyexams, services.serviceno==phyexams.serviceno)
    query = query.join(customers, services.cmscode==customers.cmscode)
    query = query.join(companies, cast(services.company_id, Integer)==companies.company_id)
    for service, physical, customer, company in query.limit(10):
        print(service.serviceno, physical.weight, customer.firstname, company.company_id)

if __name__=='__main__':
    load()