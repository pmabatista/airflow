import psycopg2
import math
import sys
from pprint import pprint
from sshtunnel import SSHTunnelForwarder

count = """SELECT count(*)
FROM (((((((((((((((atendimentos ae
    JOIN medicos me USING (cd_medico))
    JOIN exames ex USING (cd_atendimento))
    LEFT JOIN atendimentos_avisos aa ON (ae.nr_aviso = aa.cd_aviso)
    JOIN planos pl USING (cd_plano))
    JOIN pacientes pa USING (cd_paciente))
    JOIN procedimentos pr USING (cd_procedimento))
    JOIN modalidades mo USING (cd_modalidade))))
    JOIN salas sa USING (cd_sala))
    JOIN empresas em ON ((sa.cd_empresa = em.cd_empresa)))))
    LEFT JOIN funcionarios fm ON ((fm.cd_funcionario = ae.cd_funcionario_marcacao)))
    LEFT JOIN funcionarios fr ON ((fr.cd_funcionario = ae.cd_funcionario_recepcao))))
WHERE ((ae.nr_controle IS NOT NULL) AND (em.cd_empresa = 21))"""

select = """SELECT (to_char((ae.dt_data)::timestamp with time zone, 'yyyy/mm'::text))::character varying(64) AS data_mes,
       aa.ds_aviso,
       pr.ds_procedimento                                                                        AS descr_procedimento,
       mo.ds_modalidade                                                                          AS modalidade,
       ae.nr_controle,
       ae.dt_data,
       pa.ds_paciente,
       pa.cd_paciente,
       pa.dt_nascimento,
       sa.ds_sala,
       me.ds_guerra                                                                              AS nome_executante,
       fm.ds_funcionario                                                                         AS marcacao,
       fr.ds_funcionario                                                                         AS recepcao,
       '1'::character varying(2)                                                                 AS nr_qte,
       ex.nr_vl_particular,
       ae.dt_hora,
       em.ds_empresa
FROM (((((((((((((((atendimentos ae
    JOIN medicos me USING (cd_medico))
    JOIN exames ex USING (cd_atendimento))
    LEFT JOIN atendimentos_avisos aa ON (ae.nr_aviso = aa.cd_aviso)
    JOIN planos pl USING (cd_plano))
    JOIN pacientes pa USING (cd_paciente))
    JOIN procedimentos pr USING (cd_procedimento))
    JOIN modalidades mo USING (cd_modalidade))))
    JOIN salas sa USING (cd_sala))
    JOIN empresas em ON ((sa.cd_empresa = em.cd_empresa)))))
    LEFT JOIN funcionarios fm ON ((fm.cd_funcionario = ae.cd_funcionario_marcacao)))
    LEFT JOIN funcionarios fr ON ((fr.cd_funcionario = ae.cd_funcionario_recepcao))))
WHERE ((ae.nr_controle IS NOT NULL) AND (em.cd_empresa = 21)) limit 500 offset (%s)"""

delete = "delete from vacina_facil"

try:

    with SSHTunnelForwarder(
            ('crd.zapto.org', 1157),
            # ssh_private_key="</path/to/private/ssh/key>",
            # in my case, I used a password instead of a private key
            ssh_username='dicomvix',
            ssh_password='system98',
            remote_bind_address=('localhost', 5432),
            local_bind_address=('localhost', 5429)) as server:

        server.start()
        print("server connected")

        clinux = {
            'database': 'clinux_crd',
            'user': 'dicomvix',
            'password': 'system98',
            'host': 'localhost',
            'port': 5429
        }

        dw = {
            'database': 'dw',
            'user': 'crd',
            'password': 'system98',
            'host': '192.168.1.233',
            'port': 5432
        }

        try:
            conn = psycopg2.connect(**clinux)
            cursclinux = conn.cursor()
            conn2 = psycopg2.connect(**dw)
            print("database connected clinux")
            print("ETL ATENDIMENTOS CLINUX")
            cursdw = conn2.cursor()
            cursdw.execute(delete)
            cursdw.close()
            print("Tabela Limpa.")
            cursclinux.execute(count)
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
                cursclinux.execute(select, (range,))
                atendimentos = cursclinux.fetchall()
                cursclinux.close()
                try:
                    cursdw = conn2.cursor()
                    args_str = b','.join(
                        cursdw.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", atendimento) for atendimento in
                        atendimentos).decode()
                    cursdw.execute(
                        """INSERT INTO vacina_facil (data_mes,ds_aviso,ds_procedimento,ds_modalidade,nr_controle,
                                       dt_data,ds_paciente,cd_paciente,dt_nascimento,ds_sala,ds_med_solicitante,
                                       ds_fun_marcacao,ds_fun_recepcao,nr_qtde,nr_vl_particular,dt_hora,ds_empresa) 
                            VALUES """ + args_str)
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
