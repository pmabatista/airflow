import psycopg2
import math
from pprint import pprint
from sshtunnel import SSHTunnelForwarder

count = """select count(*)
from exames ex
         join procedimentos pr using (cd_procedimento)
         join modalidades mo using (cd_modalidade)
         left join medicos ms using (cd_medico)
         join planos pl using (cd_plano)
         join fornecedores fo on pl.cd_fornecedor = fo.cd_fornecedor"""
select = """select ex.cd_atendimento,
                   ex.cd_exame,
                   fo.ds_fornecedor as ds_convenio,
                   case pr.cd_modalidade
                       when 5 then 4 -- ULTRASSONOGRAFIA
                       when 4 then 1 -- TOMOGRAFIA
                       when 6 then 6 -- CARDIOLOGIA
                       when 3 then 3 -- RAIO-X
                   end as cd_modalidade,
                   pr.ds_procedimento,
                   ms.ds_medico as ds_solicitante,
                   ms.ds_crm_nr || '-' || ms.ds_crm_uf as ds_crm_solicitante,
                   ex.nr_vl_co,
                   ex.nr_vl_hm,
                   ex.nr_vl_mf,
                   ex.nr_vl_ct,
                   ex.nr_vl_md,
                   ex.nr_vl_particular,
                   ex.nr_vl_convenio,
                   '3'::integer AS cd_empresa
             from exames ex
                join procedimentos pr using (cd_procedimento)
                join modalidades mo using (cd_modalidade)
                left join medicos ms using (cd_medico)
                join planos pl using (cd_plano)
                join fornecedores fo on pl.cd_fornecedor = fo.cd_fornecedor
                limit 500 
                offset (%s)"""

try:

    with SSHTunnelForwarder(
            ('54.233.117.112', 22),
            # ssh_private_key="</path/to/private/ssh/key>",
            # in my case, I used a password instead of a private key
            ssh_username="dicomvix",
            ssh_password="Gtecbsb@2019",
            remote_bind_address=('localhost', 5432),
            local_bind_address=('localhost', 5424)) as server:

        server.start()
        print("server connected")

        cdm = {
            'database': 'clinux_hmap',
            'user': 'dicomvix',
            'password': 'system98',
            'host': 'localhost',
            'port': 5424
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
            print("database connected hmap")
            print("ETL EXAMES HMAP")
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
                exames = curscdm.fetchall()
                curscdm.close()
                try:
                    cursdw = conn2.cursor()
                    args_str = b','.join(cursdw.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", exame) for exame in exames).decode()
                    cursdw.execute("INSERT INTO exames (cd_atendimento, cd_exame,ds_convenio, cd_modalidade, ds_procedimento, ds_solicitante, ds_crm_solicitante, nr_vl_co, nr_vl_hm, nr_vl_mf, nr_vl_ct, nr_vl_md, nr_vl_particular, nr_vl_convenio,cd_empresa) VALUES " +
                                   args_str + "ON CONFLICT (cd_exame,cd_empresa) DO UPDATE SET ds_convenio=excluded.ds_convenio, cd_modalidade=excluded.cd_modalidade, ds_procedimento=excluded.ds_procedimento, ds_solicitante=excluded.ds_solicitante, ds_crm_solicitante=excluded.ds_crm_solicitante, nr_vl_convenio=excluded.nr_vl_convenio, nr_vl_particular=excluded.nr_vl_particular ")
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
