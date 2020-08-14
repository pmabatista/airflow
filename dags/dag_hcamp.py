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
    'dag_hcamp',
    catchup=False,
    schedule_interval= '30 1,13 * * *',
    default_args=default_args
) as dag:
    # Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
    t1 = BashOperator(
        task_id='etl_atendimentos_hcamp',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_scripts
        python3 etl_atendimentos.py 8 54.207.126.77 22 localhost dicomvix Gtecbsb@2019 5425 clinux_hugo system98 3 limpar
    """)
    t2 = BashOperator(
        task_id='etl_exames_hcamp',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_scripts
        python3 etl_exames.py 8 54.207.126.77 22 localhost dicomvix Gtecbsb@2019 5425 clinux_hugo system98 3 limpar
    """)
    t3 = BashOperator(
        task_id='etl_pacientes_hcamp',
        bash_command="""
        cd $AIRFLOW_HOME/dags/etl_scripts
        python3 etl_pacientes.py 8 54.207.126.77 22 localhost dicomvix Gtecbsb@2019 5425 clinux_hugo system98 3 limpar

    """)
    
# Definindo o padrão de execução, nesse caso executamos t1 e depois t2
t1 >> t2 >> t3
