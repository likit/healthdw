from sqlalchemy import (create_engine, Table,
                            MetaData, select, and_)

# create source db engine
sourcedb = create_engine('mssql+pyodbc://sa:Genius1@mumt')

# create destination db engine
destdb = create_engine('postgresql+psycopg2://likit:password@localhost/healthdbstg')

sourcemeta = MetaData()
destmeta = MetaData()

sconnection = sourcedb.connect()
dconnection = destdb.connect()


def import_company_data():
    # query and load company data to staging database
    print('Importing companies data..')
    s_customers = Table('Customer', sourcemeta, autoload=True, autoload_with=sourcedb)

    d_customers = Table('companies', destmeta, autoload=True, autoload_with=destdb)

    s = select([s_customers.c.CustID,
                s_customers.c.CShortName,
                s_customers.c.Cname])

    for n, rec in enumerate(sconnection.execute(s)):
        # check whether the data already exists
        # if so, skip
        if dconnection.execute(select([d_customers]).where(
            d_customers.c.company_id==rec[0]
            )).fetchone():
            continue
        else:
            ins = d_customers.insert().values(
                    company_id=rec[0],
                    shortname=rec[1],
                    fullname=rec[2],
                    )
            result = dconnection.execute(ins)
        if n%1000 == 0:
            print('{} added..'.format(n))

def import_customer_data():
    # query and load customers data to staging database
    print('Importing customers data..')
    s_customers = Table('Employee', sourcemeta, autoload=True, autoload_with=sourcedb)

    d_customers = Table('customers', destmeta, autoload=True, autoload_with=destdb)

    s = select([s_customers.c.cmsCode,
                s_customers.c.CustID,
                s_customers.c.prename,
                s_customers.c.Fname,
                s_customers.c.Lname,
                s_customers.c.Age,
                s_customers.c.Sex])

    for n, rec in enumerate(sconnection.execute(s)):
        # check whether the data already exists
        # if so, skip
        if dconnection.execute(select([d_customers]).where(
            d_customers.c.cmscode==rec[0]
            )).fetchone():
            continue
        else:
            ins = d_customers.insert().values(
                    cmscode=rec[0],
                    company_id=rec[1],
                    title=rec[2],
                    firstname=rec[3],
                    lastname=rec[4],
                    age=rec[5],
                    gender=rec[6]
                    )
            result = dconnection.execute(ins)
        if n%1000 == 0:
            print('{} added..'.format(n))

def import_test_data():
    s_tests = Table('Test', sourcemeta, autoload=True, autoload_with=sourcedb)

    d_tests = Table('tests', destmeta, autoload=True, autoload_with=destdb)

    # query and load tests data
    print('Importing tests data..')
    s = select([s_tests])
    for n, rec in enumerate(sconnection.execute(s)):
        if dconnection.execute(select([d_tests]).where(
            d_tests.c.tcode==rec[0]
            )).fetchone():
            continue
        else:
            ins = d_tests.insert().values(
                    tcode=rec[0],
                    tname=rec[2],
                    )
            result = dconnection.execute(ins)
        if n%1000 == 0:
            print('{} added..'.format(n))


def import_physical_exam_data():
    s_phyexams = Table('Physical_Exam', sourcemeta, autoload=True,
                            autoload_with=sourcedb)

    d_phyexams = Table('phyexams', destmeta, autoload=True,
                            autoload_with=destdb)

    # query and load test results data
    print('Importing test results data..')
    s = select([s_phyexams.c.ServiceNo, s_phyexams.c.cmsCode,
        s_phyexams.c.Weight, s_phyexams.c.Height,
        s_phyexams.c.HeartRate, s_phyexams.c.Systolic]).where(and_(
            s_phyexams.c.ServiceDate>='2012-01-01 00:00:00',
            s_phyexams.c.ServiceDate<='2015-12-31 00:00:00'
            ))

    for n, rec in enumerate(sconnection.execute(s)):
        if dconnection.execute(select([d_phyexams]).where(
            d_phyexams.c.serviceno==str(rec[0]))).fetchone():
            continue
        else:
            try:
                weight = float(rec[2])
            except:
                weight = None
            try:
                height = float(rec[3])
            except:
                height = None
            try:
                hr = int(rec[4])
            except:
                hr = None
            try:
                systolic, diastolic = rec[5].split('/')
                systolic = int(systolic)
                diastolic = int(diastolic)
            except:
                systolic = None
                diastolic = None

            ins = d_phyexams.insert().values(
                    serviceno=rec[0],
                    cmscode=int(rec[1]),
                    weight=weight,
                    height=height,
                    heartrate=hr,
                    systolic=systolic,
                    diastolic=diastolic
                    )
            result = dconnection.execute(ins)
        if n%1000==0:
            print('{} records added...'.format(n))

def import_result_data():
    s_testresults = Table('Lab', sourcemeta, autoload=True,
                            autoload_with=sourcedb)

    d_testresults = Table('testresults', destmeta, autoload=True,
                            autoload_with=destdb)

    # query and load test results data
    print('Importing test results data..')
    s = select([s_testresults]).where(and_(
            s_testresults.c.ServiceDate>='2014-01-01 00:00:00',
            s_testresults.c.ServiceDate<='2014-12-31 00:00:00'
            ))

    for n, rec in enumerate(sconnection.execute(s)):
        if dconnection.execute(select([d_testresults]).where(
            d_testresults.c.no==str(rec[0]))).fetchone():
            continue
        else:
            ins = d_testresults.insert().values(
                    no=rec[0],
                    serviceno=rec[1],
                    servicedate=rec[3],
                    tcode=rec[5],
                    result=rec[6],
                    )
            result = dconnection.execute(ins)
        if n%1000==0:
            print('{} records added...'.format(n))


def import_service_data():
    # query and load service data to staging database
    print('Importing service data..')
    s_services = Table('Services', sourcemeta,
                        autoload=True, autoload_with=sourcedb)

    d_services = Table('services', destmeta,
                        autoload=True, autoload_with=destdb)

    s = select([s_services.c.ServiceNo,
                s_services.c.ServiceDate,
                s_services.c.CustID,
                s_services.c.EmpID,
                s_services.c.cmsCode,
                ]).where(and_(
                    s_services.c.ServiceDate>='2007-01-01 00:00:00',
                    s_services.c.ServiceDate<'2018-01-01 00:00:00'
                ))

    for n, rec in enumerate(sconnection.execute(s)):
        # check whether the data already exists
        # if so, skip
        if dconnection.execute(select([d_services]).where(
            d_services.c.serviceno==rec[0]
            )).fetchone():
            continue
        else:
            ins = d_services.insert().values(
                    serviceno=rec[0],
                    servicedate=rec[1],
                    company_id=rec[2],
                    customer_id=rec[3],
                    cmscode=rec[4],
                    )
            result = dconnection.execute(ins)
        if n%1000 == 0:
            print('{} added..'.format(n))


if __name__=='__main__':
    #import_physical_exam_data()
    import_customer_data()
