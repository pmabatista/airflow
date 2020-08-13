import psycopg2
import math
import sys
from pprint import pprint
from sshtunnel import SSHTunnelForwarder

param = sys.argv[1:]
cd_empresa = param[0]
ip_server = param[1]
port_server = int(param[2])
user_server = param[3]
password_server = param[4]
port_tunnel = int(param[5])
db_name = param[6]
empresa = param[7]
clear = param[8]

count = """select count(*)
from pacientes pa
         join atendimentos ae using (cd_paciente)
         join salas sa using (cd_sala)
         join empresas using (cd_empresa)
where sa.cd_empresa = (%s)"""
select = """select distinct pa.cd_paciente,
                ds_paciente,
                dt_nascimento,
                ds_cpf,
                ds_sexo,
                case ds_civil
                    when '1' then 'SOLTEIRO(A)'
                    when '2' then 'CASADO(A)'
                    when '3' then 'DIVORCIADO(A)'
                    when '4' then 'DESQUITADO(A)'
                    when '5' then 'SEPARADO(A)'
                    when '6' then 'VIUVO(A)'
                    when '7' then 'OUTRO(A)'
                    else 'OUTRO(A)' end as ds_civil,
                nr_peso,
                nr_altura,
                pa.ds_cidade,
                pa.ds_estado,
                ds_profissao,
                (%s)::integer          AS cd_empresa
from pacientes pa
         join atendimentos ae using (cd_paciente)
         join salas sa using (cd_sala)
         join empresas using (cd_empresa)
where sa.cd_empresa = (%s)
limit 500
offset(%s)"""

delete = "delete from pacientes where cd_empresa = (%s)"
try:

    with SSHTunnelForwarder(
            (ip_server, port_server),
            # ssh_private_key="</path/to/private/ssh/key>",
            # in my case, I used a password instead of a private key
            ssh_username=user_server,
            ssh_password=password_server,
            remote_bind_address=('localhost', 5432),
            local_bind_address=('localhost', port_tunnel)) as server:

        server.start()
        print("server connected")
        clinux = {
            'database': db_name,
            'user': 'dicomvix',
            'password': 'system98',
            'host': 'localhost',
            'port': port_tunnel
        }
        dw = {
            'database': 'dw',
            'user': 'crd',
            'password': 'system98',
            'host': 'localhost',
            'port': 5432
        }
        try:
            conn = psycopg2.connect(**clinux)
            cursclinux = conn.cursor()
            conn2 = psycopg2.connect(**dw)
            print("database connected clinux")
            print("ETL EXAMES CLINUX")
            cursdw = conn2.cursor()
            if (clear == "limpar"):
                cursdw.execute(delete, cd_empresa)
                cursdw.close()
                print("Tabela Limpa.")
            cursclinux.execute(count,empresa)
            offset = cursclinux.fetchone()
            offset = offset[0]
            pprint(offset)
            cursclinux.close()
            i = 0
            while (i <= offset):
                cursclinux = conn.cursor()
                range = str(i)
                pos = (i / offset) * 100
                print("{:.0f} / 100".format(pos))
                cursclinux.execute(select, [cd_empresa,empresa, (range,)])
                pacientes = cursclinux.fetchall()
                cursclinux.close()
                try:
                    cursdw = conn2.cursor()
                    args_str = b','.join(
                        cursdw.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", paciente) for paciente in
                        pacientes).decode()
                    cursdw.execute(
                        "INSERT INTO pacientes (cd_paciente, ds_paciente, dt_nascimento, ds_cpf, ds_sexo, ds_civil, nr_peso, nr_altura, ds_cidade, ds_estado, ds_profissao,cd_empresa) VALUES " +
                        args_str + "ON CONFLICT (cd_paciente,cd_empresa) DO UPDATE SET ds_paciente = excluded.ds_paciente")
                    conn2.commit()
                    cursdw.close()
                except Exception as e:
                    print(e)
                i = i + 500
            conn2.close()
            conn.close()
            server.close()
        except Exception as e:
            pprint(e)
except:
    print("Connection Failed")
