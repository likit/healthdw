import click
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import and_
import numpy as np
from collections import defaultdict


def get_datasets(test_id, years, engine):
    datasets = []
    query = 'select results.*, tests.test_code from results' + \
            ' inner join dates on results.service_date_id=dates.date_id' + \
            ' inner join tests on tests.test_id=results.test_id' + \
            ' where tests.test_id={} and dates.gregorian_year = {};'
    for year in years:
        data = pd.read_sql(query.format(test_id, year), engine)
        data.rename(columns={'numeric_result': str(year)}, inplace=True)
        data = data.set_index('customer_id')
        datasets.append(data)
    return datasets


def get_regular_customers(alldata, years, excluded_indexes=[]):
    reg_indexes = alldata[0].index
    for i in range(1, len(alldata)):
        reg_indexes = reg_indexes.intersection(alldata[i].index)
    reg_indexes = reg_indexes[~reg_indexes.duplicated()]
    if len(excluded_indexes):
        reg_indexes = reg_indexes.difference(excluded_indexes)
    regcust = alldata[0].loc[alldata[0].index.intersection(reg_indexes)]
    for i in range(1, len(years)):
        regcust = regcust.merge(alldata[i].loc[
                    alldata[i].index.intersection(reg_indexes)][[str(years[i])]],
                    left_index=True, right_index=True, how='inner')
    regcust = regcust.loc[~regcust.index.duplicated()]  # remove duplicated indexes
    return regcust


def cal_transition_prob(df, states, pcts):
    matrices = {}
    for state in states:
        matrices[state] = [0] * len(states)
    for idx, row in df.iterrows():
        for i in range(len(pcts)-1):
            if pd.isna(row[pcts[i]]) or pd.isna(row[pcts[i+1]]):
                continue
            else:
                init_state = row[pcts[i]]
                next_state = row[pcts[i+1]]
            state_idx = states.index(next_state)
            matrices[init_state][state_idx] += 1
    for s, v in matrices.iteritems():
        mt = []
        total = sum(v)
        for cnt in v:
            if total > 0:
                p = float(cnt)/total
            else:
                p = 0
            mt.append(p)
        matrices[s] = mt
    return matrices


def predict_forward(df, txm, states, values, pctchange_col, nyears=1):
    predicted_values = pd.DataFrame([], index=df.index, columns=range(nyears))
    indexes = []
    hires = []
    lores = []
    for idx, row in df.iterrows():
        if pd.isna(row[pctchange_col]):
            continue
        hi_values = list(row[values])
        lo_values = list(row[values])
        init_probs = pd.Series([0]*len(states), index=states)
        init_probs[row[pctchange_col]] += 1
        final_probs = [np.dot(init_probs, np.linalg.matrix_power(txm, n+len(row[values])))
                      for n in range(nyears)]
        final_probs = [pd.Series(fp, index=states) for fp in final_probs]
        for n in range(nyears):
            fp = final_probs[n]
            sorted_final_probs = fp[fp>0].sort_values(ascending=False)
            hichange = hi_values[0] * float(sorted_final_probs.index[0])
            lochange = lo_values[0] * float(sorted_final_probs.index[1])
            hi_predicted_value = hi_values[0] + (hichange/100.0)
            lo_predicted_value = lo_values[0] + (lochange/100.0)
            hi_values.append(hi_predicted_value)
            lo_values.append(lo_predicted_value)
        indexes.append(idx)
        hires.append(hi_values)
        lores.append(lo_values)
    hi_results = pd.DataFrame(hires, index=indexes)
    lo_results = pd.DataFrame(lores, index=indexes)
    return hi_results, lo_results

@click.command()
@click.argument('test')
@click.argument('years')
@click.option('--step', help='step', default='20', type=int)
@click.option('--db', help='database')
@click.option('--username', help='database username')
@click.option('--dbhost', help='database host', default='localhost')
@click.option('--minage', help='minimum age', default='25', type=int)
@click.option('--maxage', help='maximum age', default='55', type=int)
def main(test, years, db, dbhost, username, minage, maxage, step):
    print('Parameters:\n\tTest: {}, Years: {}, DB: {}, DBHost: {}'.format(test, years, db, dbhost))
    if not db:
        db = 'healthdw20161216'  # just for convenience, not used in production
    engine = create_engine('postgresql+psycopg2://{}@{}/{}'.format(username, dbhost, db))
    if not test or not years:
        print('Some required parameters are missing.')
        raise SystemExit
    else:
        try:
            df = pd.read_sql('select * from tests', engine)
        except Exception as e:
            print('Cannot connect to the database.')
            raise e
            raise SystemExit
        try:
            test_id = df[df['test_code']==test.upper()].test_id.iloc[0]
        except IndexError:
            print('\t{} not found.'.format(test))
            raise SystemExit


        years = [int(year) for year in years.split(',')]
        regcust = get_regular_customers(get_datasets(test_id, years, engine), years)
        flt_regcust = regcust[(regcust['age']>=minage) & (regcust['age']<=maxage)]

        for i in range(len(years)-1):
            curyear = str(years[i])
            nextyear = str(years[i+1])  # not need to be string in the first place
            column = 'pctchange%d' % (i+1)
            kwargs = {column: lambda x: (x[nextyear] - x[curyear])*100/x[curyear]}
            flt_regcust = flt_regcust.assign(**kwargs)

        states = np.arange(-50, 100, step)
        states_labels = [str(s) for s in states][1:]

        # needs to iterate all pct changes
        for colname in flt_regcust.columns:
            if colname.startswith('pctchange'):
                column = colname + 'cat'
                kwargs = {
                    column: lambda x: pd.cut(x[colname], states, right=True, labels=states_labels)
                }
                flt_regcust = flt_regcust.assign(**kwargs)

        pcts = ['pctchange{}cat'.format(i+1) for i in range(len(years)-1)]
        matrices = cal_transition_prob(flt_regcust, states_labels, pcts)
        txm = pd.DataFrame(matrices, columns=states_labels, index=states_labels).T
        txm.to_csv('{}_mat_{}_{}-{}.csv'.format(test, step, years[0], years[-1]))



if __name__ == '__main__':
    main()