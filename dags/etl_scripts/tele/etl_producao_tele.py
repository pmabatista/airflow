import psycopg2
import math
from pprint import pprint
from sshtunnel import SSHTunnelForwarder

count = """select count(*)
FROM (((((((((atendimentos ae
JOIN pacientes pa USING (cd_paciente))
JOIN salas sa USING (cd_sala))
JOIN empresas em USING (cd_empresa))
LEFT JOIN exames ex USING (cd_atendimento))
LEFT JOIN procedimentos pr ON ((ex.cd_procedimento = pr.cd_procedimento)))
LEFT JOIN modalidades mo ON ((pr.cd_modalidade = mo.cd_modalidade)))
LEFT JOIN atendimentos_avisos av ON ((ae.nr_aviso = av.cd_aviso)))
LEFT JOIN medicos me ON ((me.cd_medico = ae.cd_medico)))
LEFT JOIN laudos_assinados la ON ((la.cd_laudo = ex.cd_exame))
LEFT JOIN atendimentos_complementos ac ON (ae.cd_complemento=ac.cd_complemento)
LEFT JOIN laudos_achados_tipos ah using (cd_achado))
WHERE ((ae.dt_data >= '2019-01-01'::date) AND (ae.dt_data <= '2022-12-31'::date) AND (me.sn_clinico = true) AND
(ae.nr_controle IS NOT NULL) AND (ex.cd_procedimento IS NOT NULL))"""
select = """SELECT em.ds_empresa                       AS empresa,
                   ae.dt_data                          AS data,
                   pa.ds_paciente                      AS paciente,
                   ae.nr_controle                      AS protocolo,
                   av.ds_aviso                         AS aviso,
                   mo.ds_modalidade                    AS modalidade,
                   pr.ds_procedimento                  AS procedimento,
                   me.ds_medico                        AS medico_executante,
                   ex.cd_exame,
                   me.ds_crm_nr                        AS crm_medico_executante,
                   me.ds_crm_uf                        AS crm_uf_med_executante,
                   (ae.dt_data + ae.dt_hora)           AS dthora_pedido_cliente,
                   (SELECT log_clinux.dt_log
                   FROM log_clinux                                                                                                    
                   WHERE (((log_clinux.ds_table)::text = 'atendimentos'::text) AND
                   ((log_clinux.ds_operation)::text = 'INSERT'::text) AND (log_clinux.cd_key = ex.cd_exame))
                   ORDER BY log_clinux.dt_log LIMIT 1) AS dthora_integracao_tele,
                   COALESCE((SELECT log_clinux.dt_log FROM log_clinux
                   WHERE (((log_clinux.ds_field)::text = 'dt_assinado'::text) AND (log_clinux.cd_key = ex.cd_exame) AND 
                   (log_clinux.ds_after IS NOT NULL) AND (log_clinux.dt_log > (ae.dt_data + ae.dt_hora)))
                   ORDER BY log_clinux.ds_after
                   LIMIT 1), ex.dt_assinado)           AS dt_primeira_assinatura,
                   ex.dt_assinado                      AS dt_hora_assinado,
                   split_part(ac.ds_complemento, '- ', 2) as ds_complemento,
                   ah.ds_achado                        AS ds_achado
FROM (((((((((atendimentos ae
JOIN pacientes pa USING (cd_paciente))
JOIN salas sa USING (cd_sala))
JOIN empresas em USING (cd_empresa))
LEFT JOIN exames ex USING (cd_atendimento))
LEFT JOIN procedimentos pr ON ((ex.cd_procedimento = pr.cd_procedimento)))
LEFT JOIN modalidades mo ON ((pr.cd_modalidade = mo.cd_modalidade)))
LEFT JOIN atendimentos_avisos av ON ((ae.nr_aviso = av.cd_aviso)))
LEFT JOIN medicos me ON ((me.cd_medico = ae.cd_medico)))     
LEFT JOIN laudos_assinados la ON ((la.cd_laudo = ex.cd_exame))
LEFT JOIN atendimentos_complementos ac ON (ae.cd_complemento=ac.cd_complemento)
LEFT JOIN laudos_achados_tipos ah using (cd_achado))
WHERE ((ae.dt_data >= '2019-01-01'::date) AND (ae.dt_data <= '2022-12-31'::date) AND (me.sn_clinico = true) AND
(ae.nr_controle IS NOT NULL) AND (ex.cd_procedimento IS NOT NULL))
GROUP BY em.ds_empresa, ae.dt_data, pa.ds_paciente, ae.nr_controle, av.ds_aviso, 
mo.ds_modalidade, pr.ds_procedimento,me.ds_medico, me.ds_crm_nr, me.ds_crm_uf, (ae.dt_data + ae.dt_hora), 
la.dt_assinado, ex.cd_exame,ae.cd_atendimento,ac.ds_complemento, ah.ds_achado limit 50000 offset (%s)"""


try:

    with SSHTunnelForwarder(
            ('teleradiologiacrd.zapto.org', 22),
            # ssh_private_key="</path/to/private/ssh/key>",
            # in my case, I used a password instead of a private key
            ssh_username="dicomvix",
            ssh_password="system98",
            remote_bind_address=('localhost', 5432),
            local_bind_address=('localhost', 5422)) as server:

        server.start()
        print("server connected")

        tele = {
            'database': 'clinux_teleradiologia',
            'user': 'dicomvix',
            'password': 'system98',
            'host': 'localhost',
            'port': 5422
        }

        dw = {
            'database': 'dw',
            'user': 'crd',
            'password': 'system98',
            'host': 'localhost',
            'port': 5432
        }

        try:
            conn = psycopg2.connect(**tele)
            curstele = conn.cursor()
            conn2 = psycopg2.connect(**dw)
            print("database connected tele")
            print("ETL PRODUCAO TELE")
            curstele.execute(count)
            offset = curstele.fetchone()
            offset = offset[0]
            pprint(offset)
            curstele.close()
            i = 0
            while (i <= offset):
                curstele = conn.cursor()
                curstele.itersize = 20000
                range = str(i)
                pos = (i/offset)*100
                print("{:.2f} / 100".format(pos))
                curstele.execute(select, (range,))
                producao = curstele.fetchall()
                curstele.close()
                try:
                    cursdw = conn2.cursor()
                    args_str = b','.join(cursdw.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", atendimento) for atendimento in producao).decode()
                    cursdw.execute("INSERT INTO teleradiologia (empresa, data, paciente, protocolo, aviso, modalidade, procedimento, medico_executante, cd_exame, crm_medico_executante, crm_uf_med_executante, dthora_pedido_cliente, dthora_integracao_tele, dt_primeira_assinatura, dt_hora_assinado,ds_complemento,ds_achado) VALUES " +
                                   args_str + "ON CONFLICT (cd_exame) DO UPDATE SET empresa = excluded.empresa, data = excluded.data, paciente = excluded.paciente, protocolo = excluded.protocolo, aviso = excluded.aviso, modalidade = excluded.modalidade, procedimento = excluded.procedimento, medico_executante = excluded.medico_executante, crm_medico_executante = excluded.crm_medico_executante, crm_uf_med_executante = excluded.crm_uf_med_executante, dthora_pedido_cliente = excluded.dthora_pedido_cliente, dthora_integracao_tele = excluded.dthora_integracao_tele, dt_primeira_assinatura = excluded.dt_primeira_assinatura, dt_hora_assinado = excluded.dt_hora_assinado, ds_complemento = excluded.ds_complemento, ds_achado = excluded.ds_achado")
                    conn2.commit()
                    cursdw.close()
                except Exception as e:
                    print(e)
                i = i+50000
            conn2.close()
            conn.close()
            server.close()
        except Exception as e:
            pprint(e)
except:
    print("Connection Failed")
