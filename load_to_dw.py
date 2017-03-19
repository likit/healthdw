from create_dw import dw_engine
from sqlalchemy import Table, create_engine, cast, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

# create source db engine
db_engine = create_engine('postgresql+psycopg2://likit:password@localhost/healthdbstg')
dw_engine = create_engine('postgresql+psycopg2://likit:password@localhost/healthdw_dev')

DWSession = sessionmaker(bind=dw_engine)
DBSession = sessionmaker(bind=db_engine)
db_session = DBSession()
dw_session = DWSession()

DBBase = automap_base()
DBBase.prepare(db_engine, reflect=True)

DWBase = automap_base()
DWBase.prepare(dw_engine, reflect=True)


dbServices = DBBase.classes.services
dbCompanies = DBBase.classes.companies
dwCompanies = DWBase.classes.companies
dbCustomers = DBBase.classes.customers
dwCustomers = DWBase.classes.customers
dbTests = DBBase.classes.tests
dwTests = DWBase.classes.tests
dbTestResults = DBBase.classes.testresults
dwFacts = DWBase.classes.facts
dwDates = DWBase.classes.dates
#prices = DBBase.classes.prices
#phyexams = DBBase.classes.phyexams


def loadtest(tcode):
    # query and load company data to staging database
    #query = db_session.query(services, phyexams, customers, companies)
    #query = query.join(phyexams, services.serviceno==phyexams.serviceno)
    #query = query.join(customers, services.cmscode==customers.cmscode)
    #query = query.join(companies, cast(services.company_id, Integer)==companies.company_id)
    #for service, physical, customer, company in query.limit(10):
    #    print(service.serviceno, physical.weight, customer.firstname, company.company_id)
    atest = dw_session.query(dwTests).filter(dwTests.code==tcode).first()
    if not atest:
        record = db_session.query(dbTests).filter(dbTests.tcode==tcode).first()
        newtest = dwTests(code=record.tcode, fullname=record.tname)
        dw_session.add(newtest)
        dw_session.commit()
    else:
        print(tcode, 'already exists')


def load_test_results(tcode):
    for res in db_session.query(dbTestResults.servicedate,
                            dbTestResults.tcode,
                            dbTestResults.result,
                            dbServices.serviceno,
                            dbServices.company_id,
                            dbServices.cmscode).join(dbServices,
            dbTestResults.serviceno==dbServices.serviceno)\
            .filter(dbTestResults.tcode==tcode):
        if res.cmscode == '0' or not res.cmscode:
            continue
        test = dw_session.query(dwTests).filter(dwTests.code==tcode).first()
        customer = dw_session.query(dwCustomers)\
                        .filter(dwCustomers.customer_id==int(res.cmscode)).first()
        if not customer:
            print('cannot find a customer with cmscode={}'.format(res.cmscode))
            continue
        company = dw_session.query(dwCompanies)\
                        .filter(dwCompanies.company_id==int(res.company_id)).first()
        if not company:
            print('cannot find a company with id={}'.format(res.company_id))
            continue
        date = dw_session.query(dwDates).filter(
                    dwDates.day==res.servicedate.day,
                    dwDates.month==res.servicedate.month,
                    dwDates.gregorian_year==res.servicedate.year).first()
        if not date:
            print('cannot find a company with id={}'.format(res.servicedate))
            continue
        # check for existing data
        aresult = dw_session.query(dwFacts, dwCustomers.customer_id, dwTests.code, dwDates)\
                    .join(dwCustomers, dwCustomers.customer_id==dwFacts.customer_id)\
                    .join(dwTests, dwTests.test_id==dwFacts.test_id)\
                    .join(dwDates, dwDates.date_id==dwFacts.service_date_id)\
                    .filter(dwFacts.customer_id==int(res.cmscode),
                            dwTests.code==res.tcode,
                            dwDates.day==res.servicedate.day,
                            dwDates.month==res.servicedate.month,
                            dwDates.gregorian_year==res.servicedate.year).first()
        if not aresult:
            try:
                value = float(res.result)
            except:
                pass
            else:
                # add a fact with a numeric result value to the table
                afact = dwFacts(customer_id=customer.customer_id,
                                    company_id=company.company_id,
                                    test_id=test.test_id,
                                    service_date_id=date.date_id,
                                    num_value=res.result)
            dw_session.add(afact)
            dw_session.commit()
        else:
            print("the result already exists")


def load_companies():
    for rec in db_session.query(dbCompanies):
        acomp = dw_session.query(dwCompanies).\
                    filter(dwCompanies.company_id==int(rec.company_id)).first()
        if not acomp:
            newcomp = dwCompanies(company_id=int(rec.company_id),
                                    name=rec.fullname)
            dw_session.add(newcomp)
            dw_session.commit()
        else:
            print('ID={} Already exists, skipped'.format(acomp.company_id))

def load_customers():
    for n,rec in enumerate(db_session.query(dbCustomers)):
        if n % 1000 == 0:
            print('{} customers added...'.format(n))
        if rec.cmscode == '0' or not rec.cmscode:
            continue
        acust = dw_session.query(dwCustomers).\
                    filter(dwCustomers.customer_id==int(rec.cmscode)).first()
        if not acust:
            if not rec.gender:
                gender = 0
            else:
                gender = rec.gender
            if not rec.firstname and not rec.lastname:
                continue
            newcust = dwCustomers(customer_id=int(rec.cmscode),
                                    firstname=rec.firstname,
                                    lastname=rec.lastname,
                                    age=rec.age,
                                    gender=gender)
            dw_session.add(newcust)
            dw_session.commit()
        else:
            print('ID={} Already exists, skipped'.format(acust.customer_id))

if __name__=='__main__':
    #loadtest('CHO')
    load_test_results('CHO')
    #load_companies()
    #load_customers()