import psycopg2
import math
from pprint import pprint
from sshtunnel import SSHTunnelForwarder

count = """select count(*)
 FROM lancamentos la
       JOIN pagamentos    pa USING(cd_lancamento)
       JOIN fornecedores  fo USING(cd_fornecedor)
       JOIN contas        co USING(cd_conta)
       JOIN contas_grupos cg USING(cd_grupo)
       JOIN empresas em using(cd_empresa)
       LEFT JOIN lancamentos_formas lf using (cd_forma)
       LEFT JOIN contas_fluxos cf using (cd_fluxo)"""
select = """SELECT
      pa.cd_pagamento,
      pa.cd_lancamento,
      pa.dt_lancamento,
      pa.dt_realizacao,
      case when cg.sn_despesa is true
      then -1 * pa.nr_realizado
      else pa.nr_realizado
      end  as nr_realizado,
      cg.ds_grupo,
      fo.ds_fornecedor,
      lf.ds_forma,
      em.ds_empresa,
      co.ds_conta
      FROM lancamentos la
      JOIN pagamentos    pa USING(cd_lancamento)
      JOIN fornecedores  fo USING(cd_fornecedor)
      JOIN contas        co USING(cd_conta)
      JOIN contas_grupos cg USING(cd_grupo)
      JOIN empresas em using(cd_empresa)
      LEFT JOIN lancamentos_formas lf using (cd_forma)
      LEFT JOIN contas_fluxos cf using (cd_fluxo)
      ORDER BY cd_lancamento
      limit 500
      offset (%s)"""


try:

    with SSHTunnelForwarder(
            ('186.251.74.20', 22),
            # ssh_private_key="</path/to/private/ssh/key>",
            # in my case, I used a password instead of a private key
            ssh_username="dicomvix",
            ssh_password="system98",
            remote_bind_address=('localhost', 5432),
            local_bind_address=('localhost', 5422)) as server:

        server.start()
        print("server connected")

        cdm = {
            'database': 'clinux_caldas_novas',
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
            conn = psycopg2.connect(**cdm)
            curscdm = conn.cursor()
            conn2 = psycopg2.connect(**dw)
            print("database connected cdm")
            print("ETL LANCAMENTOS CDM")
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
                curscdm.execute(select, (range,))
                lancamentos = curscdm.fetchall()
                print("{:.0f} / 100".format(pos))
                curscdm.close()
                try:
                    cursdw = conn2.cursor()
                    args_str = b','.join(cursdw.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", lancamento) for lancamento in lancamentos).decode()
                    cursdw.execute("INSERT INTO lancamentos (cd_pagamento, cd_lancamento, dt_lancamento, dt_realizacao, nr_realizado, ds_grupo, ds_fornecedor, ds_forma, ds_empresa,ds_conta) VALUES " +
                                   args_str + "ON CONFLICT (cd_lancamento,cd_pagamento) DO UPDATE SET ds_conta=excluded.ds_conta, nr_realizado = excluded.nr_realizado")
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
