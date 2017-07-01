--CREATE DATABASE healthdw_old;
--CREATE SCHEMA IF NOT EXISTS checkup AUTHORIZATION may;
--grant all on schema checkup to may;

CREATE TABLE dates
(
  date_id integer NOT NULL,
  day integer NOT NULL,
  month_no integer NOT NULL,
  gregorian_year integer NOT NULL,
  quarter integer NOT NULL,
  day_of_Week integer NOT NULL,
  day_of_year integer NOT NULL,
  fiscal_year integer NOT NULL,
  weekday varchar(16) NOT NULL,
  month varchar(16) NOT NULL,
  buddhist_year integer NOT NULL,
  CONSTRAINT dates_pk PRIMARY KEY (date_id)
  );

  CREATE TABLE customers
(
  customer_id serial NOT NULL,
  first_name varchar(45),
  last_name varchar(45),
  full_name varchar(90),
  gender varchar(6) NOT NULL,
  CONSTRAINT customers_pk PRIMARY KEY (customer_id)
  );

CREATE TABLE affiliations
(
  affiliation_id integer,
  name varchar(90),
  CONSTRAINT affiliations_pk PRIMARY KEY (affiliation_id)
);

CREATE TABLE companies
(
  company_id serial NOT NULL,
  name varchar(45) NOT NULL,
  street_Addr varchar(90),
  district varchar(45),
  subdistrict varchar(45),
  province varchar(50),
  village varchar(45),
  zipcode varchar(5),
  country varchar(45),
  latitute varchar(45),
  longitude varchar(45),
  affiliation_id integer,
  sector varchar(45),
  CONSTRAINT companies_pk PRIMARY KEY (company_id),
  CONSTRAINT companies_affiliationId_fk FOREIGN KEY(affiliation_id) REFERENCES affiliations(affiliation_id)
  );

CREATE TABLE tests
(
  test_id serial NOT NULL,
  description varchar(90) NOT NULL,
  coverage_type varchar(90),
  unit varchar(45),
  ref_minimum decimal(2),
  ref_maximum decimal(2),
  profile varchar(45),
  cost decimal(2),
  CONSTRAINT tests_pk PRIMARY KEY (test_id)
);

CREATE TABLE results
(
  result_id serial NOT NULL,
  service_date_id integer NOT NULL,
  customer_id integer NOT NULL,
  company_id integer NOT NULL,
  test_id integer NOT NULL,
  numeric_result numeric(7, 2),
  text_result varchar(45),
  age integer,
  CONSTRAINT results_pk PRIMARY KEY (result_id),
  CONSTRAINT results_date_id_fk FOREIGN KEY(service_date_id) REFERENCES dates(date_id),
  CONSTRAINT results_customerId_fk FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
  CONSTRAINT results_companyId_fk FOREIGN KEY(company_id) REFERENCES companies(company_id),
  CONSTRAINT results_testId_fk FOREIGN KEY(test_id) REFERENCES tests(test_id)
);
