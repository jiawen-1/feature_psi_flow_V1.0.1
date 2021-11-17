# -*- coding: utf-8 -*-
import time
import pandas as pd
import numpy as np
import datetime
import os
import pickle
import json
import csv
import subprocess
import gc
from numpy import inf
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime

# utc 时间需要+8才是北京时间, 8+8=16
default_args = {
    'owner': 'jiawen.hou',
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

with DAG(
        dag_id='feature_psi_flow',
        default_args=default_args,
        start_date=datetime.datetime(2021, 10, 20),
        schedule_interval='05 09 * * *',
        tags=['report'],
) as dag:
    def create_feature_monitor_bin(feature_source='opay_ods.ods_feature_risk_result_history_di',
                                   local_file='feature_boundaries.txt',
                                   benchmark_dt='2021-10-08'):
        command = """
        hive -e 'create table if not exists test_db.H_feature_boundaries_for_psi (
        feature_name string comment "特征名",
        feature_bin string comment "特征分箱"
        )
        partitioned by (feature_source string, dt string)
        row format delimited fields terminated by ";"
        location "oss://opay-datalake/features_monitor/feature_boundaries"
        tblproperties ("skip.header.line.count"="1");
        load data local inpath "{}" overwrite into table test_db.H_feature_boundaries_for_psi partition(feature_source="{}", dt="{}")'
        """.format(local_file, feature_source, benchmark_dt)


        ret = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        return ret


    def feature_discrete_v2(data, engine='hive://jiawen.hou@10.130.11.94:10000/opay_dw', benchmark_dt='2021-10-08',
                            feature_source='opay_ods.ods_feature_risk_result_history_di', stats_col='from_user_id',
                            benchmark=True):
        """
        特征离散化并计算分布.

        Args:
            data(pd.DataFrame): 特征数据集
            engine(str): hive连接符
            benchmark_dt(str): 'yyyy-MM-dd'格式日期, 当 benchmark=True时, 该列为必须项.
            feature_source(str): 特征所在库表
            stats_col(str): 需要进行统计的列名
            benchmark(bool): True:计算 benchmark 分箱特征分布; False 计算所需监控的特征分箱分布.
        Returns:
            分箱后的特征对应统计分布.
        """
        dt = data.copy()
        dt['model0823_score'] = dt['model0823_score'].astype(float)
        feature_bin_sql = """
        select feature_name, feature_bin from test_db.H_feature_boundaries_for_psi 
        where feature_source = '{}'
        and dt = '{}'
        """.format(feature_source, benchmark_dt)
        cols_bins = pd.read_sql(feature_bin_sql, engine)
        col_zip = zip(list(dt.columns), list(dt.dtypes))
        total_user = dt['{}'.format(stats_col)].nunique()

        distribution_list = []
        float_distribution_list = []
        for i in col_zip:
            if i[0] not in ['from_user_id', 'dt', 'feature_bins']:
                if i[1] != 'object':
                    cut_bins = [-np.inf] + json.loads(
                        cols_bins.loc[cols_bins.feature_name == i[0]]['feature_bin'].iloc[0]) + [np.inf]
                    dt['feature_bins'] = pd.cut(dt[i[0]].fillna(-9999.0), cut_bins)
                    tmp = (dt.groupby('feature_bins')['{}'.format(stats_col)].nunique() / total_user).round(2).reset_index()
                    tmp['feature_name'] = i[0]
                    tmp = tmp[['feature_name', 'feature_bins', '{}'.format(stats_col)]]
                    tmp.columns = ['feature_name', 'feature_bins', 'user_cnt_rate']
                    tmp['rn'] = tmp.index + 1
                    tmp['feature_bins'] = tmp.apply(lambda row: str(row['rn']) + ') ' + str(row['feature_bins']),
                                                    axis=1)
                    tmp = tmp.drop('rn', axis=1)
                    float_distribution_list.append(tmp)

        result = pd.concat(float_distribution_list, axis=0)
        # print(result)
        if benchmark is False:
            result.rename(columns={'user_cnt_rate': 'user_cnt_last_rate'}, inplace=True)
            result['user_cnt_last'] = total_user
        else:
            result['benchmark_user_cnt'] = total_user
            result.rename(columns={'user_cnt_rate': 'benchmark_user_cnt_rate'}, inplace=True)
        del distribution_list
        del float_distribution_list
        del dt
        del total_user
        gc.collect()
        return result


    def load_psi_benchmark(benchmark_dt='2021-10-08',
                           local_file='feature_model0823_benchmark.txt',
                           feature_source='opay_ods.ods_feature_risk_result_history_di'):
        command = """
        hive -e '
        create table if not exists test_db.H_feature_benchmark_for_psi (
        feature_name string comment "特征名",
        feature_bins string comment "特征分箱",
        benchmark_user_cnt_rate float comment "每箱对应用户数量",
        benchmark_user_cnt float comment "总用户数量"
        )
        partitioned by (feature_source string, dt string)
        row format delimited fields terminated by ";"
        location "oss://opay-datalake/features_monitor/feature_benchmark_for_psi"
        tblproperties ("skip.header.line.count"="1");
        load data local inpath "{}" overwrite into table test_db.H_feature_benchmark_for_psi partition(feature_source="{}",dt="{}")'
        """.format(local_file, feature_source, benchmark_dt)

        ret = subprocess.run(command, shell=True)
        return ret


    def load_into_psi(feature_table='feature_model0823', feature_source='opay_ods.ods_feature_risk_result_history_di',
                      dt= datetime.datetime.strftime(datetime.datetime.utcnow().date() + datetime.timedelta(days=-1), '%Y-%m-%d')):
        local_file = '{}_psi.txt'.format(feature_table)
        command = """
        hive -e '
        create table if not exists test_db.H_feature_psi_df (
        feature_name string comment "特征名",
        feature_bins string comment "特征分箱",
        benchmark_user_cnt_rate float comment "benchmark用户占比",
        benchmark_user_cnt float comment "benchmark总用户数量",
        user_cnt_last_rate float comment "dt对应用户占比",
        user_cnt_last float comment "dt对应用户总数量",
        psi_index float comment "每箱 psi 值",
        psi float comment "特征 psi 值"
        )
        partitioned by (feature_source string, dt string)
        row format delimited fields terminated by ";"
        location "oss://opay-datalake/features_monitor/feature_psi_df"
        tblproperties ("skip.header.line.count"="1");
        load data local inpath "{}" overwrite into table test_db.H_feature_psi_df partition(feature_source="{}", dt="{}")
        '
        """.format(local_file, feature_source, dt)

        ret = subprocess.run(command, shell=True)
        return ret


    def cal_psi_v2(dt, date, engine='hive://jiawen.hou@10.130.11.94:10000/opay_dw', feature_table='feature_model0823',
                   feature_source='opay_ods.ods_feature_risk_result_history_di', benchmark_dt='2021-10-08'):
        new_data = dt.copy()
        benchmark_sql = """
        select feature_name
            , feature_bins
            , benchmark_user_cnt_rate
            , benchmark_user_cnt
        from test_db.H_feature_benchmark_for_psi
        where dt = '{}'
        and feature_source = '{}'
        """.format(benchmark_dt, feature_source)
        bench_mark = pd.read_sql(benchmark_sql, engine)
        merge_data = pd.concat([bench_mark, new_data], axis=1)
        merge_data = merge_data.loc[:, ~merge_data.columns.duplicated()]
        # print(merge_data)
        merge_data['psi_index'] = (merge_data['user_cnt_last_rate'] - merge_data['benchmark_user_cnt_rate']) * np.log(
            merge_data['user_cnt_last_rate'] / merge_data['benchmark_user_cnt_rate'])
        result = merge_data.merge(merge_data.groupby('feature_name').agg(psi=('psi_index', sum)).reset_index(),
                                  on='feature_name', how='left')
        result.to_csv('{}_psi.txt'.format(feature_table), index=False, sep=';', escapechar=' ', quoting=csv.QUOTE_NONE)
        load_psi_params = {
            'feature_table': '{}'.format(feature_table),
            'feature_source': '{}'.format(feature_source)
        }
        ret = load_into_psi(**load_psi_params)
        # print(ret)
        return ret


    def feature_psi_flow(engine='hive://jiawen.hou@10.130.11.94:10000/opay_dw',
                         feature_table='feature_model0823',
                         feature_source='opay_ods.ods_feature_risk_result_history_di', benchmark_dt='2021-10-08'):
        date = datetime.datetime.strftime(datetime.datetime.utcnow().date() + datetime.timedelta(days=-1), '%Y-%m-%d')
        params = {
            'feature_source': feature_source,
            'local_file': 'feature_boundaries.txt',
            'benchmark_dt': benchmark_dt
        }
        create_feature_monitor_bin(**params)
        sql = '''
                select from_user_id,model0823_score,dt from 
                (select from_user_id,
                get_json_object(`urule_response`,'$.keyParameters.model0823_score') as model0823_score,
                row_number()over(partition by from_user_id order by create_time desc)as rn,
                dt
                from opay_ods.ods_feature_risk_result_history_di 
                where get_json_object(`rule_mappings`,'$[0].knowledgeCode')='FIRST_CREDIT_FLOW' 
                and dt='2021-10-08' )   di 
                where di.rn='1'
                '''
        date = pd.read_sql(sql, engine)
        descrete_params = {
            'feature_source': feature_source,
            'benchmark': True
        }
        model0823_descrete = feature_discrete_v2(date, **descrete_params)
        model0823_descrete.to_csv("feature_model0823_benchmark.txt", sep=';', index=False)
        benchmark_param = {
            'local_file': 'feature_model0823_benchmark.txt',
            'feature_source': feature_source,
            'benchmark_dt': benchmark_dt
        }
        load_psi_benchmark(**benchmark_param)
        sql_model0823 = """
                select from_user_id,model0823_score,dt 
                from  (select from_user_id,get_json_object(`urule_response`,'$.keyParameters.model0823_score') as model0823_score,
                row_number()over(partition by from_user_id order by create_time desc)as rn,
                dt
                from opay_ods.ods_feature_risk_result_history_di 
                where get_json_object(`rule_mappings`,'$[0].knowledgeCode')='FIRST_CREDIT_FLOW' 
                and dt=DATE_SUB(CURRENT_DATE,1))   di 
                where di.rn='1'
                """
        model0823 = pd.read_sql(sql_model0823, engine)
        descrete_params = {
            'feature_source': feature_source,
            'benchmark': False
        }
        model0823_descrete_new = feature_discrete_v2(model0823, **descrete_params)
        psi_params = {
            'engine': engine,
            'feature_table': feature_table,
            'feature_source': feature_source
        }
        result = cal_psi_v2(model0823_descrete_new, model0823_descrete, **psi_params)
        return result


    run_this = PythonOperator(
        task_id='feature_psi_flow',
        python_callable=feature_psi_flow,
        op_kwargs={'date': str(datetime.datetime.utcnow().date()+datetime.timedelta(days=-1)),
        }
    )
