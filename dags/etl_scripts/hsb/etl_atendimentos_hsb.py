import psycopg2
import math
from pprint import pprint
from sshtunnel import SSHTunnelForwarder

count = """select count(*)
from atendimentos ae
join salas sa using(cd_sala)
join empresas em using(cd_empresa)
join medicos me using(cd_medico)
left join medicos mr on (mr.cd_medico = ae.cd_revisor)
join pacientes pa using(cd_paciente)
left join atendimentos_avisos aa on (aa.cd_aviso = ae.nr_aviso)
where ae.nr_controle is not null"""
select = """select ae.cd_atendimento,
       ae.dt_data,
       	ae.dt_hora,
	ae.nr_controle,
	to_char(ae.dt_agendamento, 'YYYY-MM-DD HH:mm:ss') as dt_agendamento,
	'2'::integer AS cd_empresa,
	me.ds_medico as ds_executante,
	aa.ds_aviso,
       ae.dt_hora_sala01                           as hr_entrada_sala,
       ae.dt_hora_sala02                           as hr_saida_sala,
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
join medicos me using(cd_medico)
left join medicos mr on (mr.cd_medico = ae.cd_revisor)
join pacientes pa using(cd_paciente)
left join atendimentos_avisos aa on (aa.cd_aviso = ae.nr_aviso)
where ae.nr_controle is not null limit 500 offset (%s)"""


try:

    with SSHTunnelForwarder(
            ('189.112.139.121', 1157),
            # ssh_private_key="</path/to/private/ssh/key>",
            # in my case, I used a password instead of a private key
            ssh_username="dicomvix",
            ssh_password="system98",
            remote_bind_address=('localhost', 5432),
            local_bind_address=('localhost', 5423)) as server:

        server.start()
        print("server connected")

        cdm = {
            'database': 'clinux_santa_barbara',
            'user': 'dicomvix',
            'password': 'system98',
            'host': 'localhost',
            'port': 5423
        }

        dw = {
            'database': 'dw',
            'user': 'crd',
            'password': 'system98',
            'host': 'localhost',
            'port': 5432
        }

        try:
            conn = psycopg2.connect(**cdm)
            curscdm = conn.cursor()
            conn2 = psycopg2.connect(**dw)
            print("database connected hsb")
            print("ETL ATENDIMENTOS HSB")
            curscdm.execute(count)
            offset = curscdm.fetchone()
            offset = offset[0]
            pprint(offset)
            curscdm.close()
            i = 0
            while (i <= offset):
                curscdm = conn.cursor()
                range = str(i)
                pos = (i/offset)*100
                print("{:.0f} / 100".format(pos))
                curscdm.execute(select, (range,))
                atendimentos = curscdm.fetchall()
                curscdm.close()
                try:
                    cursdw = conn2.cursor()
                    args_str = b','.join(cursdw.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", atendimento) for atendimento in atendimentos).decode()
                    cursdw.execute("INSERT INTO atendimentos (cd_atendimento,dt_data,dt_hora,nr_controle,dt_agendamento,cd_empresa,ds_executante,ds_aviso,hr_entrada_sala,hr_saida_sala,ds_status,cd_paciente) VALUES " +
                                   args_str + "ON CONFLICT (cd_atendimento,cd_empresa) DO UPDATE set cd_paciente = excluded.cd_paciente")
                    conn2.commit()
                    cursdw.close()
                except Exception as e:
                    print(e)
                i = i+500
            conn2.close()
            conn.close()
            server.close()
        except Exception as e:
            pprint(e)
except:
    print("Connection Failed")
