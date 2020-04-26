import pandas as pd
import glob
import csv
from itertools import dropwhile, takewhile

metric = pd.read_csv(r"F:\Projekty\covid\metric.csv")
cntr = pd.read_csv(r"F:\Projekty\covid\cntr.csv")

mt = metric.set_index(['FIPS','country'])
cnt = cntr.set_index(['FIPS','country'])
master = mt.merge(cnt.iloc[:,-1], left_index=True, right_index=True)

result = pd.DataFrame()
result['M1'] = (master['cases']/master['day10']).astype(str) + ';' + (master['cases']*master['day10']).astype(str)
result['M4'] = (master['metric']*master['day10']).astype(str) + ';' + (master['metric']/master['day10']).astype(str)
result['M3'] = master['cases']

fin = result.reset_index().melt(['FIPS','country'], var_name='Metric', value_name='Value')
fin[['Metric','MOE']] = fin['Value'].apply(lambda x: pd.Series(str(x).split(";")))
#for DATA2

def group_by_state(df):
    df2 = df.groupby('country').sum().drop(columns="FIPS")
    return df2

def make_percentage(dfs, period, name):
    total= pd.DataFrame()
    for i in dfs:
        dd = i.reset_index().set_index(['country','Metric']).pct_change(axis=1,periods=period)*100
        total = total.append(dd)
    result = total.reset_index().melt(['country','Metric'],var_name='Date', value_name=name)
    return result

def timeseries_metrics(covid_cases,metrics):

    cnt = group_by_state(covid_cases)
    mt = group_by_state(metrics)
    mt = mt[['metric','cases']]
    cnt = mt.merge(cnt, left_index=True, right_index=True).drop(columns=["metric","cases"])
    cnt_no = cnt[cnt.select_dtypes(include=['number']).columns]

    new1 = cnt_no.diff(axis=1, periods=4)
    new2 = new1.divide(mt['cases'], axis="index")
    new3 = cnt_no.divide(mt['metric'],axis='index')

    new1['Metric'] = 'difference'
    new2['Metric'] = 'difference | cases'
    new3['Metric'] = 'difference | metric'

    new = cnt_no.append([new1, new2, new3]).reset_index().melt(['country','Metric'], var_name='Date', value_name='Value')

    dfs = [new1, new2, new3]
    day = make_percentage(dfs, 1, 'Day-to-day')
    week = make_percentage(dfs, 7, 'Week-to-week')

    result = new.merge(day, on=['country','Metric','Date']).merge(week,on=['country','Metric','Date'])

    return result



datafr = timeseries_metrics(cntr,metric)
