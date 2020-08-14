# Importando as bibliotecas que vamos usar nesse exemplo
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
# Definindo alguns argumentos básicos
default_args = {
    'owner': 'miguel_pedro',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 29),
    'retries': 1,
    'email': ['inteligencia@crd.med.br'],
    'email_on_failure': True,
    'email_on_retry': False,
}
# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
with DAG(
    'dag_hrsm',
    catchup=False,
    schedule_interval= '15 1,13 * * *',
    default_args=default_args
) as dag:
    # Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
    t1 = BashOperator(
        task_id='etl_atendimentos_hrsm',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_scripts
        python3 etl_atendimentos.py 11 131.72.222.93 1157 10.233.95.19 dicomvix system98 5420 clinux_santa_maria system98 1 limpar
    """)
    t2 = BashOperator(
        task_id='etl_exames_hrsm',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_scripts
        python3 etl_exames.py 11 131.72.222.93 1157 10.233.95.19 dicomvix system98 5420 clinux_santa_maria system98 1 limpar
    """)
    t3 = BashOperator(
        task_id='etl_pacientes_hrsm',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_scripts
        python3 etl_pacientes.py 11 131.72.222.93 1157 10.233.95.19 dicomvix system98 5420 clinux_santa_maria system98 1 limpar

    """)
    
# Definindo o padrão de execução, nesse caso executamos t1 e depois t2
t1 >> t2 >> t3
