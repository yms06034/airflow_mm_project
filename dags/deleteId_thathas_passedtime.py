# Deleting an ID that is past the subscription period

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime
import pendulum
import pymysql

kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner' : 'KSJ_mm',
    'start_date' : datetime(2023,8,14, tzinfo=kst),
    'schedule_interval' : '@daily',
}

with DAG(
    dag_id = 'deleteId_thathas_passedtime',
    default_args = default_args,
    tags = ['macro', 'delete_id'],
    catchup=False
) as dag:
    
    mysql_conn_id = 'mmkdb_mysql_conn'
    
    sql_query = """
    SELECT id
    FROM n_c_home_userinfo
    WHERE end_data_at < NOW();
    """
    
    select_ids_extraction = MySqlOperator (
        task_id = 'select_ids_extraction',
        mysql_conn_id = mysql_conn_id,
        sql = sql_query,
        do_xcom_push = True,
        doc_md = "Selecting an ID that is past the subscription period",
    )
    
    def delete_ids(**kwargs):
        ti = kwargs['ti']
        select_id = ti.xcom_pull(task_ids='select_ids_extraction')
        
        ids = [r_id[0] for r_id in select_id]
        
        if ids:
            delete_query = """
            DELETE 
            FROM n_c_home_userinfo
            WHERE id IN ({});
            """
            
            mysql_hook = MySqlHook(mysql_conn_id = mysql_conn_id)
            mysql_hook.run(delete_query.format(','.join(['%s'] * len(ids))), parameters=ids)
            
    delete_ids_task = PythonOperator(
        task_id = 'delete_ids_task',
        python_callable = delete_ids,
        provide_context = True,
        doc_md = "Deleting an ID that is past the subscription period"
    )
    
    select_ids_extraction >> delete_ids_task
