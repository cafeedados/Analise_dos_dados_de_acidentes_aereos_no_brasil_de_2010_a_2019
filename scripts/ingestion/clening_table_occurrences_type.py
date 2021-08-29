class Ocurrences_type: 
    def __init__(self):

        import pandas as pd #pandas para tratar o data frame
        import pandera as pa #pandera para validação dos dados
        from datetime import datetime #date time para trazer a captura de data.

        # Estou defininco uma variável que será usada para captura de data para logs.
        log_time = datetime.today().strftime('%Y-%m-%d %H:%M')
        file_name = 'ocurrences_type_2010_2020'

        #  Estou definincdo o dataframe (df_ocurrences), csv tem por separador o ponto e virgula(;), depois estou convertendo em datas as colunas que são datas e definindo dia primeiro. 
        df_ocurrences_type = pd.read_csv('Data/ocorrencia_tipo_2010_2020.csv', sep=';')

        #Criando um schema de validação para garantir que não ficou nenhum dado null
        schema = pa.DataFrameSchema(
            columns = {
                "codigo_ocorrencia1":            pa.Column(pa.Int),
                "ocorrencia_tipo":               pa.Column(pa.String),
                "ocorrencia_tipo_categoria":     pa.Column(pa.String),
                "taxonomia_tipo_icao":           pa.Column(pa.String)
            }
        )

        #Realizando a validação e gerando log da validação
        try: 
            schema.validate(df_ocurrences_type)  
            log_null_and_datatypes = 'Validado sem numeros nullos, e com tipo de dado correto ({})'.format(log_time)
        except: 
            log_null_and_datatypes = 'Validado sem numeros nullos, e com tipo de dado correto ({})'.format(log_time)

        #Para buscar as inconsistências realizei um comando de grupby pela contagem de registros exemplo: df_ocurrences.groupby(['divulgacao_relatorio_numero']).size()
        conditions = ['*** COLISÃO EM VOO COM OBSTÁCULO (excluir)']
        df_ocurrences_type = df_ocurrences_type.replace(conditions, 'COLISÃO EM VOO COM OBSTÁCULO')


        df_ocurrences_type.to_csv('Data_Trait/Data_csv_files/ocurrences_type_2010_2020.csv', encoding='utf-8')

        print('Arquivo {} processado: {}'.format(file_name, log_null_and_datatypes))

        