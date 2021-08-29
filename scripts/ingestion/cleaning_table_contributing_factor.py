class Contributing_factor: 
    def __init__(self):
        import pandas as pd #pandas para tratar o data frame
        import pandera as pa #pandera para validação dos dados
        from datetime import datetime #date time para trazer a captura de data.

        # Estou defininco uma variável que será usada para captura de data para logs.
        log_time = datetime.today().strftime('%Y-%m-%d %H:%M')

        file_name = 'contributing_factor_2010_2020'

        #  Estou definincdo o dataframe (df_ocurrences), csv tem por separador o ponto e virgula(;), depois estou convertendo em datas as colunas que são datas e definindo dia primeiro. 
        df_contributing_factor = pd.read_csv('Data/fator_contribuinte_2010_2020.csv', sep=';')


        schema = pa.DataFrameSchema(
            columns = {    
                    "codigo_ocorrencia3":      pa.Column(pa.Int),
                    "fator_nome":              pa.Column(pa.String),
                    "fator_aspecto":           pa.Column(pa.String),
                    "fator_condicionante":     pa.Column(pa.String),
                    "fator_area":              pa.Column(pa.String)
            }
        )

        #Realizando a validação e gerando log da validação
        try: 
            schema.validate(df_contributing_factor)  
            log_null_and_datatypes = 'Validado sem numeros nullos, e com tipo de dado correto ({})'.format(log_time)
        except: 
            log_null_and_datatypes = 'Nao validado, contem numeros nulos ou tipo de dado incorreto({})'.format(log_time) 


        conditions = ['***', '****', '*****', '*******', '*********', '****_***']
        df_contributing_factor = df_contributing_factor.replace(conditions, 'not informed')

        df_contributing_factor.to_csv('Data_Trait/Data_csv_files/contributing_factor_2010_2020.csv', encoding='utf-8')

        print('Arquivo {} processado: {}'.format(file_name, log_null_and_datatypes))

