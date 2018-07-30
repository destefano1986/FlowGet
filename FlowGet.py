# -*- coding:utf-8 -*-
import psycopg2
import pandas as pd
import datetime

def postgre_resource_tdd_pred(date):
    conn = psycopg2.connect(host='10.11.122.127', port=5432, user='gpuser', \
                            password='gp@qetuo', database='shwygpdb')
    sql2part1_1 = "SELECT n.dn, n.userlabel, n.pdcpupoctul, n.pdcpupoctdl, n.rrutotalprbusagemeanul, n.rrutotalprbusagemeandl, n.rrcconnmean, n.rrcconnmax, n.rrceffectiveconnmean, n.rrceffectiveconnmax, r.* FROM (SELECT * FROM pm_eutrancelltdd_day WHERE datetime = "
    sql2part1_2 = ") AS n, (SELECT * FROM resource_eutrancell WHERE date = "
    sql2part1_3 = ") as r WHERE n.dn=r.dn"
    temp = date
    sql2part2 = repr(date)
    sql2part3 = repr(str(datetime.datetime.strptime(temp, "%Y-%m-%d")))
    sql2 = str(sql2part1_1 + str(sql2part3) + sql2part1_2 + str(sql2part2) + sql2part1_3)
    print (sql2)
    cursor = conn.cursor()
    cursor.execute(sql2)
    title = [i[0] for i in cursor.description]
    data = cursor.fetchall()
    df_paras = pd.DataFrame(data, columns=title)
    cursor.close()
    conn.close()
    return df_paras

if __name__ == '__main__':
    begin = datetime.datetime(2018, 7, 1)
    end = datetime.datetime(2018, 7, 29)
    d = begin
    delta = datetime.timedelta(days=1)
    datelist = []
    while d <= end:
        temp = d.strftime("%Y-%m-%d")
        datelist.append(temp)
        d += delta
    for date in datelist:
        df = postgre_resource_tdd_pred(date)
        df.to_csv('nrm'+str(date)+'.csv', index=True, header=True, encoding='gbk')
