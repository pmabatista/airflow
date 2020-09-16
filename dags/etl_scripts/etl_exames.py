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
from exames ex
         join atendimentos ae using(cd_atendimento)
         join salas sa using(cd_sala)
         join empresas em using (cd_empresa)
         join procedimentos pr using (cd_procedimento)
         join modalidades mo on (mo.cd_modalidade = pr.cd_modalidade)
         left join medicos ms on (ms.cd_medico = ex.cd_medico)
         join planos pl using (cd_plano)
         join fornecedores fo on pl.cd_fornecedor = fo.cd_fornecedor 
         where em.cd_empresa = (%s)"""
select = """select ex.cd_atendimento,
                   ex.cd_exame,
                   fo.ds_fornecedor as ds_convenio,
                   mo.ds_modalidade,
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
                   (%s)::integer AS cd_empresa
             from exames ex
                join atendimentos ae using(cd_atendimento)
                join salas sa using(cd_sala)
                join empresas em using (cd_empresa)
                join procedimentos pr using (cd_procedimento)
                join modalidades mo on (mo.cd_modalidade = pr.cd_modalidade)
                left join medicos ms on (ms.cd_medico = ex.cd_medico)
                join planos pl using (cd_plano)
                join fornecedores fo on pl.cd_fornecedor = fo.cd_fornecedor
                where em.cd_empresa = (%s)
                limit 500 
                offset (%s)"""

delete = "delete from exames where cd_empresa = (%s)"

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
                cursdw.execute(delete, [cd_empresa])
                cursdw.close()
                print("Tabela Limpa.")
            cursclinux.execute(count,[empresa])
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
                exames = cursclinux.fetchall()
                cursclinux.close()
                try:
                    cursdw = conn2.cursor()
                    args_str = b','.join(
                        cursdw.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", exame) for exame in
                        exames).decode()
                    cursdw.execute(
                        "INSERT INTO exames (cd_atendimento, cd_exame,ds_convenio, ds_modalidade, ds_procedimento, ds_solicitante, ds_crm_solicitante, nr_vl_co, nr_vl_hm, nr_vl_mf, nr_vl_ct, nr_vl_md, nr_vl_particular, nr_vl_convenio,cd_empresa) VALUES " +
                        args_str + "ON CONFLICT (cd_exame,cd_empresa) DO UPDATE SET ds_convenio=excluded.ds_convenio, ds_modalidade=excluded.ds_modalidade, ds_procedimento=excluded.ds_procedimento, ds_solicitante=excluded.ds_solicitante, ds_crm_solicitante=excluded.ds_crm_solicitante, nr_vl_convenio=excluded.nr_vl_convenio, nr_vl_particular=excluded.nr_vl_particular ")
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
