from datetime import datetime, timedelta
from io import BytesIO, StringIO

from airflow import DAG


from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.python import PythonVirtualenvOperator


def read_file(**kwargs):
    sftp_hook = SSHHook(ssh_conn_id="sftp_conn")
    sftp_client = sftp_hook.get_conn().open_sftp()
    fp = BytesIO()
    input_path = "/home/debian/test.csv"
    sftp_client.getfo(input_path, fp)
    print(fp.getvalue())
    print(type(fp.getvalue()))
    print(fp.getvalue().decode("utf-8"))
    print(type(fp.getvalue().decode("utf-8")))

    ti = kwargs['ti']
    ti.xcom_push(key="raw_input_file", value=fp.getvalue().decode("utf-8"))


def all_in_one():
    from airflow.providers.ssh.hooks.ssh import SSHHook
    import pandas as pd
    from io import BytesIO

    sftp_hook = SSHHook(ssh_conn_id="sftp_conn")
    sftp_client = sftp_hook.get_conn().open_sftp()
    fp = BytesIO()
    input_path = "/home/debian/test.csv"
    sftp_client.getfo(input_path, fp)
    df = pd.read_csv(BytesIO(fp.getvalue()))

    df['Age'] = df['Age'] + 20

    with sftp_client.open('/home/debian/processed_test.csv', "w") as f:
        f.write(df.to_csv(index=False))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
        'sftp_exercise',
        default_args=default_args,
        description='',
        start_date=datetime(2022, 2, 22),
        catchup=False,
) as dag:
    sftp_sensor = SFTPSensor(task_id="check-for-file",
                                     sftp_conn_id="sftp_conn",
                                     path="/home/debian/test.csv",
                                     poke_interval=10)

    # read_file = PythonOperator(
    #     task_id='read_file',
    #     python_callable=read_file,
    # )

    etl = PythonVirtualenvOperator(
        task_id='etl',
        python_callable=all_in_one,
        requirements=['pandas', 'apache-airflow'],
        do_xcom_push=True,
    )


    sftp_sensor >> etl

