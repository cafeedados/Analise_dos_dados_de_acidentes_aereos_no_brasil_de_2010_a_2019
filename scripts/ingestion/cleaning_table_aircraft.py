class Aircraft:
    def __init__(self):
        import pandas as pd #pandas para tratar o data frame
        import pandera as pa #pandera para validação dos dados
        from datetime import datetime #date time para trazer a captura de data.

        # Estou defininco uma variável que será usada para captura de data para logs.
        log_time = datetime.today().strftime('%Y-%m-%d %H:%M')

        file_name = 'aircraft_2010_2020'

        #  Estou definincdo o dataframe (df_ocurrences), csv tem por separador o ponto e virgula(;), depois estou convertendo em datas as colunas que são datas e definindo dia primeiro. 
        df_aircraft = pd.read_csv('Data/aeronave_2010_2020.csv', sep=';')


        #removendo nullos e altrando o valor de cada uma das colunas para ver a contagem de nullos usei a função: df.isnull().sum() para ver quais colunas estavam me trazendo valores nullos
        df_aircraft.fillna('not informed', inplace=True)

        schema = pa.DataFrameSchema(
            columns = {    
                    "codigo_ocorrencia2":             pa.Column(pa.Int),
                    "aeronave_matricula":             pa.Column(pa.String),
                    "aeronave_operador_categoria":    pa.Column(pa.String),
                    "aeronave_tipo_veiculo":          pa.Column(pa.String),
                    "aeronave_fabricante":            pa.Column(pa.String),
                    "aeronave_modelo":                pa.Column(pa.String),
                    "aeronave_tipo_icao":             pa.Column(pa.String),
                    "aeronave_motor_tipo":            pa.Column(pa.String),
                    "aeronave_motor_quantidade":      pa.Column(pa.String),
                    "aeronave_pmd":                   pa.Column(pa.Int),
                    "aeronave_pmd_categoria":         pa.Column(pa.Int),
                    "aeronave_assentos":              pa.Column(pa.String),
                    "aeronave_ano_fabricacao":        pa.Column(pa.String),
                    "aeronave_pais_fabricante":       pa.Column(pa.String),
                    "aeronave_pais_registro":         pa.Column(pa.String),
                    "aeronave_registro_categoria":    pa.Column(pa.String),
                    "aeronave_registro_segmento":     pa.Column(pa.String),
                    "aeronave_voo_origem":            pa.Column(pa.String),
                    "aeronave_voo_destino":           pa.Column(pa.String),
                    "aeronave_fase_operacao":         pa.Column(pa.String),
                    "aeronave_tipo_operacao":         pa.Column(pa.String),
                    "aeronave_nivel_dano":            pa.Column(pa.String),
                    "aeronave_fatalidades_total":     pa.Column(pa.Int),

            }
        )

        #Realizando a validação e gerando log da validação
        try: 
            schema.validate(df_aircraft)  
            log_null_and_datatypes = 'Validado sem numeros nullos, e com tipo de dado correto ({})'.format(log_time)    
        except: 
            log_null_and_datatypes = 'Nao validado, contem numeros nulos ou tipo de dado incorreto({})'.format(log_time) 

        conditions = ['***', '****', '*****', '*******', '*********', '****_***']
        df_aircraft = df_aircraft.replace(conditions, 'not informed')

        df_aircraft.to_csv('Data_Trait/Data_csv_files/aircraft_2010_2020.csv', encoding='utf-8')
        
        print('Arquivo {} processado: {}'.format(file_name, log_null_and_datatypes))
       
        




