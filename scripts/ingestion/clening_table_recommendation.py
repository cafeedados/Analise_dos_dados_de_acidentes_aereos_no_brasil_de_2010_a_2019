class Recommendation: 
    def __init__(self):
        import pandas as pd #pandas para tratar o data frame
        import pandera as pa #pandera para validação dos dados
        from datetime import datetime #date time para trazer a captura de data.

        # Estou defininco uma variável que será usada para captura de data para logs.
        log_time = datetime.today().strftime('%Y-%m-%d %H:%M')
        file_name = 'recommendation_2010_2020'

        #  Estou definincdo o dataframe (df_ocurrences), csv tem por separador o ponto e virgula(;), depois estou convertendo em datas as colunas que são datas e definindo dia primeiro. 
        df_recommendation = pd.read_csv('Data/recomendacao_2010_2020.csv', sep=';')

        #removendo nullos e altrando o valor de cada uma das colunas para ver a contagem de nullos usei a função: df.isnull().sum() para ver quais colunas estavam me trazendo valores nullos
        df_recommendation.fillna(value={
            'recomendacao_dia_feedback': '2025-01-01',
            'recomendacao_conteudo': 'not informed',
        }, inplace=True)

        #Para buscar as inconsistências realizei um comando de grupby pela contagem de registros exemplo: df_ocurrences.groupby(['divulgacao_relatorio_numero']).size()
        conditions_dates = ['0000-00-00','0002-11-27', '0002-11-25', '0002-11-26']
        conditions = ['***']
        df_recommendation = df_recommendation.replace(conditions, 'not informed')
        df_recommendation = df_recommendation.replace(conditions_dates, '2025-01-01')

        df_recommendation['recomendacao_dia_assinatura'] = pd.to_datetime(df_recommendation['recomendacao_dia_assinatura'], dayfirst=True)
        df_recommendation['recomendacao_dia_feedback'] = pd.to_datetime(df_recommendation['recomendacao_dia_feedback'], dayfirst=True)
        df_recommendation['recomendacao_dia_encaminhamento'] = pd.to_datetime(df_recommendation['recomendacao_dia_encaminhamento'], dayfirst=True)

        #Criando um schema de validação para garantir que não ficou nenhum dado null
        schema = pa.DataFrameSchema(
            columns = {
                "codigo_ocorrencia4":                  pa.Column(pa.Int),
                "recomendacao_numero":                 pa.Column(pa.String),
                "recomendacao_dia_assinatura":         pa.Column(pa.DateTime),
                "recomendacao_dia_encaminhamento":     pa.Column(pa.DateTime),
                "recomendacao_dia_feedback":           pa.Column(pa.DateTime),
                "recomendacao_conteudo":               pa.Column(pa.String),
                "recomendacao_status":                 pa.Column(pa.String),
                "recomendacao_destinatario_sigla":     pa.Column(pa.String),
                "recomendacao_destinatario":           pa.Column(pa.String),
            }
        )


        #Realizando a validação e gerando log da validação
        try: 
            schema.validate(df_recommendation)  
            log_null_and_datatypes = 'Validado sem numeros nullos, e com tipo de dado correto ({})'.format(log_time)
        except: 
            log_null_and_datatypes = 'Validado sem numeros nullos, e com tipo de dado correto ({})'.format(log_time)


        df_recommendation.to_csv('Data_Trait/Data_csv_files/recommendation_2010_2020.csv', encoding='utf-8')

        print('Arquivo {} processado: {}'.format(file_name, log_null_and_datatypes))