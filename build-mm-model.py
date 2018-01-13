import click
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import and_
import numpy as np
from collections import defaultdict

# engine = create_engine('postgresql+psycopg2://likit@localhost/healthdw20161216')

def get_datasets(years):
    datasets = []
    for year in years:
        data = pd.read_sql(query % year, engine)
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

query = 'select results.*, tests.test_code from results' + \
        ' inner join dates on results.service_date_id=dates.date_id' + \
        ' inner join tests on tests.test_id=results.test_id' + \
        ' where tests.test_id=68 and dates.gregorian_year = %d;'

# years = [2011, 2012, 2013, 2014, 2015]

# regcust = get_regular_customers(get_datasets(years), years)
lowage = 25
hiage = 55
# flt_regcust = regcust[(regcust['age']>=lowage) & (regcust['age']<=hiage)]
'''needs to iterate all years'''
# flt_regcust = flt_regcust.assign(pctchange1 = (flt_regcust['2012'] - flt_regcust['2011'])*100/flt_regcust['2011'])
states = np.arange(-50, 100, 10)
states_labels = [str(s) for s in states][1:]

# needs to iterate all pct changes
# flt_regcust = flt_regcust.assign(pctchange1cat=pd.cut(flt_regcust['pctchange1'], states, right=True, labels=states_labels))
pcts = ['pctchange1cat', 'pctchange2cat', 'pctchange3cat', 'pctchange4cat']
# matrices = cal_transition_prob(flt_regcust, states_labels, pcts)
# txm = pd.DataFrame(matrices, columns=states_labels, index=states_labels).T
# output txm

@click.command()
@click.argument('test')
@click.argument('years')
@click.option('--db', help='database')
@click.option('--dbhost', help='database host', default='localhost')
def main(test, years, db, dbhost):
    print(test, years, db, dbhost)


if __name__ == '__main__':
    main()