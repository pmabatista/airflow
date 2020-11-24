import psycopg2
import math
import requests
import json
from pprint import pprint

delete = "delete from chamados"
delete2 = "delete from chamados_campos"

try: 
    URL = "http://api.tomticket.com/chamados/db31f7008cc32431128097c14ae14c45/"

    fields = [
    'idchamado',
    'protocolo',
    'titulo',
    'email_cliente',
    'prioridade',
    'tempotrabalho',
    'tempoabertura',
    'data_criacao',
    'deadline',
    'nomecliente',
    'tipochamado',
    'avaliadoproblemaresolvido',
    'avaliadoatendimento',
    'avaliacaocomentario',
    'dataprimeiraresposta',
    'dataencerramento',
    'ultimasituacao',
    'dataultimasituacao',
    'sla_inicio',
    'sla_deadline',
    'sla_inicializacao_cumprido',
    'sla_deadline_cumprido',
    'descsituacao',
    'categoria',
    'departamento',
    'atendente',
    'id_cliente',
    'status',
    'dataultimostatus',
    'nomeorganizacao'
	]

    fields2 = [
    'id',
    'idchamado',
    'label',
    'value'
    ]

    dw = {
        'database': 'dw',
        'user': 'crd',
        'password': 'system98',
        'host': 'localhost',
        'port': 5432
    }

    r = requests.get(url = (URL + '1'))

    data = r.json() 
     
    total_itens = data['total_itens']
    offset = math.ceil(total_itens / 50)


    pprint("Total de chamados: " + str(total_itens))
    pprint("Total de paginas: " + str(offset))
    
    try:
        conn = psycopg2.connect(**dw)
        cursdw = conn.cursor()
        try:
            cursdw.execute(delete)
            print("Tabela Chamados Limpa.")
            cursdw.execute(delete2)
            cursdw.close()
            print("Tabela Chamados Campos Limpa.")
        except Exception as e:
            print(e)
        i = 1
        while (i <= offset):
            pagina = str(i)
            pos = (i / offset) * 100
            print("{:.0f} / 100".format(pos))
            r = requests.get(url = (URL + str(i)))
            data = r.json()
            data = data['data']
            try:
                cursdw = conn.cursor()
                for item in data:
                    my_data = [item[field] for field in fields]
                    insert_query = "INSERT INTO chamados VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s) ON CONFLICT (idchamado) DO NOTHING;"
                    cursdw.execute(insert_query, tuple(my_data))
                    conn.commit()
                    campospersonalizados = item['campospersonalizados']
                    for campo in campospersonalizados:
                        campo['idchamado'] = item['idchamado']
                        my_data = [campo[field] for field in fields2]
                        insert_query = "INSERT INTO chamados_campos VALUES (%s, %s, %s, %s) ON CONFLICT (id,idchamado) DO NOTHING;"
                        cursdw.execute(insert_query, tuple(my_data))
                    conn.commit()
                cursdw.close()
            except Exception as e:
                print(e)
            i = i + 1
        conn.close()
    except Exception as e:
        pprint(e)
except Exception as e:
    print(e)
