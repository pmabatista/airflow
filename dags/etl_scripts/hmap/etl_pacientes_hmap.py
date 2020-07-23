import psycopg2
import math
from pprint import pprint
from sshtunnel import SSHTunnelForwarder

count = """select count(*)
from pacientes"""
select = """select cd_paciente,
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
                   ds_cidade,
                   ds_estado,
                   ds_profissao,
                   '2'::integer AS cd_empresa
            from pacientes
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
            local_bind_address=('localhost', 5422)) as server:
    
     server.start()
     print("server connected")
     cdm = {
        'database': 'clinux_hmap',
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
         print("database connected hmap")
         print("ETL PACIENTES HMAP")
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
             pacientes = curscdm.fetchall()
             print("{:.0f} / 100".format(pos))
             curscdm.close()
             try:
                 cursdw = conn2.cursor()
                 args_str = b','.join(cursdw.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", paciente) for paciente in pacientes).decode()
                 cursdw.execute("INSERT INTO pacientes (cd_paciente, ds_paciente, dt_nascimento, ds_cpf, ds_sexo, ds_civil, nr_peso, nr_altura, ds_cidade, ds_estado, ds_profissao,cd_empresa) VALUES " +
                 args_str + "ON CONFLICT (cd_paciente,cd_empresa) DO NOTHING")
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