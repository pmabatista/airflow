import psycopg2
import math
import sys
from pprint import pprint
from sshtunnel import SSHTunnelForwarder

param = sys.argv[1:]
cd_empresa = int(param[0])
ip_server = param[1]
port_server = int(param[2])
ip_remote = param[3]
user_server = param[4]
password_server = param[5]
port_tunnel = int(param[6])
db_name = param[7]
db_password = param[8]
empresa = param[9]
clear = param[10]



count = """select count(*)
from atendimentos ae
join salas sa using(cd_sala)
join empresas em using(cd_empresa)
join medicos me using(cd_medico)
left join medicos mr on (mr.cd_medico = ae.cd_revisor)
join pacientes pa using(cd_paciente)
left join atendimentos_avisos aa on (aa.cd_aviso = ae.nr_aviso)
where em.cd_empresa = (%s)"""
select = """select ae.cd_atendimento,
       ae.dt_data,
       	ae.dt_hora,
	ae.nr_controle,
	to_char(ae.dt_agendamento, 'YYYY-MM-DD HH:mm:ss') as dt_agendamento,
	(%s)::integer AS cd_empresa,
	me.ds_medico as ds_executante,
	aa.ds_aviso,
	(ae.dt_hora_entrada)                                   AS entrada,
    (ae.dt_hora_senha)                                     AS senha,
    (ae.dt_hora_ficha)                                     AS ficha,
    (ae.dt_hora_sala00)                                    AS entrada_preparo,
    (ae.dt_hora_sala10)                                    AS saida_preparo,
    (ae.dt_hora_sala01)                                    AS entrada_exame,
    (ae.dt_hora_sala02)                                    AS saida_exame,
    (ae.dt_hora_saida)                                     AS saida,
CASE
WHEN ae.ds_status = 1 THEN 'CANCELADO'
WHEN ae.ds_status = 2 THEN 'MARCADO'
WHEN ae.ds_status = 3 THEN 'CONFIRMADO'
WHEN ae.ds_status = 4 THEN 'BLOQUEADO'
WHEN ae.ds_status = 5 THEN 'ATENDIDO'
WHEN ae.ds_status = 6 THEN 'ENTREGA RESULTADO'
WHEN ae.ds_status = 7 THEN 'MODIFICACAO DADOS'
WHEN ae.ds_status = 8 THEN 'TRANSFERENCIA HORARIO'
WHEN ae.ds_status = 9 THEN 'IMPRESSAO FICHA'
WHEN ae.ds_status = 10 THEN 'DIGITACAO LAUDO'
WHEN ae.ds_status = 11 THEN 'RECEPCAO'
WHEN ae.ds_status = 12 THEN 'IMPRESSAO LAUDO'
WHEN ae.ds_status = 13 THEN 'IMPRESSAO ETIQUETA'
WHEN ae.ds_status = 14 THEN 'CONGELADO'
WHEN ae.ds_status = 15 THEN 'ASSINATURA LAUDO'
WHEN ae.ds_status = 16 THEN 'ASSINATURA PROVISORIA'
WHEN ae.ds_status = 17 THEN 'IMPRESSAO PROTOCOLO'
END as ds_status,
ae.cd_paciente
from atendimentos ae
join salas sa using(cd_sala)
join empresas em using (cd_empresa)
join medicos me using(cd_medico)
left join medicos mr on (mr.cd_medico = ae.cd_revisor)
join pacientes pa using(cd_paciente)
left join atendimentos_avisos aa on (aa.cd_aviso = ae.nr_aviso)
where em.cd_empresa = (%s) limit 500 offset (%s)"""

delete = "delete from atendimentos where cd_empresa = (%s)"

try:

    with SSHTunnelForwarder(
            (ip_server, port_server),
            # ssh_private_key="</path/to/private/ssh/key>",
            # in my case, I used a password instead of a private key
            ssh_username=user_server,
            ssh_password=password_server,
            remote_bind_address=(ip_remote, 5432),
            local_bind_address=('localhost', port_tunnel)) as server:

        server.start()
        print("server connected")

        clinux = {
            'database': db_name,
            'user': 'dicomvix',
            'password': db_password,
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
                try:
                    cursdw.execute(delete, [cd_empresa])
                    cursdw.close()
                    print("Tabela Limpa.")
                except Exception as e:
                    print(e)
            cursclinux.execute(count, [empresa])
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
                atendimentos = cursclinux.fetchall()
                cursclinux.close()
                try:
                    cursdw = conn2.cursor()
                    args_str = b','.join(
                        cursdw.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", atendimento) for atendimento in
                        atendimentos).decode()
                    cursdw.execute(
                        "INSERT INTO atendimentos (cd_atendimento,dt_data,dt_hora,nr_controle,dt_agendamento,cd_empresa,ds_executante,ds_aviso,entrada, senha, ficha,entrada_preparo, saida_preparo, entrada_exame,saida_exame,saida,ds_status,cd_paciente) VALUES " +
                        args_str + "ON CONFLICT (cd_atendimento,cd_empresa) DO UPDATE set cd_paciente = excluded.cd_paciente")
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
