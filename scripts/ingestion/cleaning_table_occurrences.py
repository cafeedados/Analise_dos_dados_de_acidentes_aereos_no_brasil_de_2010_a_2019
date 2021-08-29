class Occurrences:
    def __init__(self):
        import pandas as pd #pandas para tratar o data frame
        import pandera as pa #pandera para validação dos dados
        from datetime import datetime #date time para trazer a captura de data.

        # Estou defininco uma variável que será usada para captura de data para logs.
        log_time = datetime.today().strftime('%Y-%m-%d %H:%M')
        file_name = 'occurrences_2010_2020'        

        #  Estou definincdo o dataframe (df_ocurrences), csv tem por separador o ponto e virgula(;), depois estou convertendo em datas as colunas que são datas e definindo dia primeiro. 
        df_ocurrences = pd.read_csv('Data/ocorrencia_2010_2020.csv', sep=';')

        #removendo nullos e altrando o valor de cada uma das colunas para ver a contagem de nullos usei a função: df.isnull().sum() para ver quais colunas estavam me trazendo valores nullos
        df_ocurrences.fillna(value={
            'ocorrencia_latitude': 'not informed' ,
            'ocorrencia_longitude': 'not informed',
            'ocorrencia_hora': 'not informed',
            'investigacao_aeronave_liberada': 'not informed',
            'investigacao_status': 'not informed',
            'divulgacao_relatorio_numero': 'not informed',
            'divulgacao_dia_publicacao' : '2025-01-01'
        }, inplace=True)

        # Convertendo String em datas, e falando que quero que o primeiro digito é o dia pois ele estava vindo: yyyy-dd-mm após essa clausula vai para yyyy-mm-dd
        df_ocurrences['ocorrencia_dia'] = pd.to_datetime(df_ocurrences['ocorrencia_dia'], dayfirst=True)
        df_ocurrences['divulgacao_dia_publicacao'] = pd.to_datetime(df_ocurrences['divulgacao_dia_publicacao'], dayfirst=True)

        #Criando um schema de validação para garantir que não ficou nenhum dado null
        schema = pa.DataFrameSchema(
            columns = {
                "codigo_ocorrencia" :              pa.Column(pa.Int),
                "codigo_ocorrencia1":              pa.Column(pa.Int),
                "codigo_ocorrencia2":              pa.Column(pa.Int),
                "codigo_ocorrencia3":              pa.Column(pa.Int),
                "codigo_ocorrencia4":              pa.Column(pa.Int),
                "ocorrencia_classificacao":        pa.Column(pa.String),
                "ocorrencia_latitude":             pa.Column(pa.String),
                "ocorrencia_longitude":            pa.Column(pa.String),
                "ocorrencia_cidade":               pa.Column(pa.String),
                "ocorrencia_uf":                   pa.Column(pa.String),
                "ocorrencia_pais":                 pa.Column(pa.String),
                "ocorrencia_aerodromo":            pa.Column(pa.String),
                "ocorrencia_dia":                  pa.Column(pa.DateTime),
                "ocorrencia_hora":                 pa.Column(pa.String),
                "investigacao_aeronave_liberada":  pa.Column(pa.String),
                "investigacao_status":             pa.Column(pa.String),
                "divulgacao_relatorio_numero":     pa.Column(pa.String),
                "divulgacao_relatorio_publicado":  pa.Column(pa.String),
                "divulgacao_dia_publicacao":       pa.Column(pa.DateTime),
                "total_recomendacoes":             pa.Column(pa.Int),
                "total_aeronaves_envolvidas":      pa.Column(pa.Int),
                "ocorrencia_saida_pista":          pa.Column(pa.String),
            }
        )

        #Realizando a validação e gerando log da validação
        try: 
            schema.validate(df_ocurrences)  
            log_null_and_datatypes = 'Validado sem numeros nullos, e com tipo de dado correto ({})'.format(log_time)
        except: 
            log_null_and_datatypes = 'Nao validado, contem numeros nulos ou tipo de dado incorreto({})'.format(log_time) 

        #Para buscar as inconsistências realizei um comando de grupby pela contagem de registros exemplo: df_ocurrences.groupby(['divulgacao_relatorio_numero']).size()
        conditions = ['*', '**', '***', '****', '*****', '*******', '*********', '****_***', '****_****', '+', 'NÃO HÁ', '###!', '####']
        df_ocurrences = df_ocurrences.replace(conditions, 'not informed')

        df_ocurrences.to_csv('Data_Trait/Data_csv_files/occurrences_2010_2020.csv', encoding='utf-8')

        print('Arquivo {} processado: {}'.format(file_name, log_null_and_datatypes))