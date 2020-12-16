import boto3
import requests
import time
import pytz
import json
from datetime import date, datetime
from operations import Operations
#config DynamoDB
dynamodb = boto3.resource('dynamodb', region_name= 'us-east-1')
tabela = dynamodb.Table('log_sideccr')
#config SQS
sqs = boto3.resource('sqs', region_name='us-east-1')
queue = sqs.get_queue_by_name(QueueName='sideccr')
#Loop
print('PROCESSO INICIADO...' )
while True:
    resp_http = False
    try:
        for message in queue.receive_messages():
                payload = message.body
                payload_dict = json.loads(payload)
                dt_utc = datetime.fromtimestamp(payload_dict['data_hora_fechado'])
                dt_utc = pytz.timezone('America/Sao_Paulo').localize(dt_utc)
                data_hora = int(datetime.now().timestamp())
        #Dict para montar a URL para o SiDeCC-R
                dict_url = {}
                dict_url['usuario'] = payload_dict['dados_sideccr']['usuario']
                dict_url['medidor'] = payload_dict['dados_sideccr']['codigo_medidor']
                dict_url['vazao'] = payload_dict['vazao']/3600
                dict_url['datahora'] = dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ') #.astimezone(pytz.UTC)
                dict_url['chave'] = payload_dict['dados_sideccr']['chave']
                r = requests.get('http://sideccr.daeebmt.sp.gov.br/envia?', params = dict_url)
        # Codigos de resposta do SiDeCC-R e servidor HTTP
                resp_http = r.content.decode('utf-8').split(' ')[1]
                resp_http_msg = r.content.decode('utf-8')
        #Montar o log para o banco de dados G-Hidro
                payload_dict['resposta_sideccr'] = {'mensagem_erro':resp_http_msg, 'codigo_erro':resp_http }
                payload_dict['timestamp'] = payload_dict['data_hora_utlizado']
                payload_dict['vazao'] = str(payload_dict['vazao']/3600)
                Operations.create(tabela,payload_dict)
                #print(payload_dict)
        # Apaga mensagem da fila
                message.delete()
                # Print no console em caso de erro
                if  resp_http != '000' or r.status_code != '200':
                        if not resp_http == False:
                                print('{}\nCódigo HTTP: {}\nResposta do servidor: {}\nTimestamp: {}'.format(r.url, r.status_code, resp_http_msg, data_hora))                        
        time.sleep(2)
    except:
        print('Ocorreu um erro esperando 60 segundos para tentar novamente')
        time.sleep(60)
                      