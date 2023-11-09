import cx_Oracle
import pandas as pd

# Configuração da conexão Oracle
oracle_username = 'tasy'
oracle_password = 'aloisk'
hostname = '172.25.1.50'
port = '1521'
service_name = 'HOM'
oracle_connection_string = f'oracle+cx_oracle://{oracle_username}:{oracle_password}@{hostname}:{port}/{service_name}'

# Caminho para o arquivo CSV
csv_file = 'cargatuss.csv'


# Função para inserir os dados no banco de dados Oracle
def inserir_dados_no_oracle(data):
    try:
        conn = cx_Oracle.connect(oracle_username, oracle_password, oracle_connection_string)
        cursor = conn.cursor()

        for i, row in data.iterrows():
            if i % 100 == 0:
                conn.commit()

            NR_SEQUENCIA = row['NR_SEQUENCIA']
            NR_SEQ_CARGA_TUSS = row['NR_SEQ_CARGA_TUSS']
            DT_ATUALIZACAO = row['DT_ATUALIZACAO']
            NM_USUARIO = row['NM_USUARIO']
            CD_MATERIAL = row['CD_MATERIAL']
            DS_MATERIAL = row['DS_MATERIAL']
            CD_REF_FABRICANTE = row['CD_REF_FABRICANTE']
            DS_FABRICANTE = row['DS_FABRICANTE']
            DT_INICIO_VIGENCIA = row['DT_INICIO_VIGENCIA']
            DT_FINAL_VIGENCIA = row['DT_FINAL_VIGENCIA']
            DS_APRESENTACAO = row['DS_APRESENTACAO']
            DS_LABORATORIO = row['DS_LABORATORIO']
            IE_TIPO_ITEM = row['IE_TIPO_ITEM']
            CD_MATERIAL_TUSS = row['CD_MATERIAL_TUSS']
            NR_REGISTRO_ANVISA = row['NR_REGISTRO_ANVISA']
            DS_GRAU_RISCO = row['DS_GRAU_RISCO']
            DT_FIM_IMPLANTACAO = row['DT_FIM_IMPLANTACAO']
            DS_GRUPO_FARMACOLOGICO = row['DS_GRUPO_FARMACOLOGICO']
            DS_CLASSE_FARMACOLOGICA = row['DS_CLASSE_FARMACOLOGICA']
            DT_INICIO_VIGENCIA_REF = row['DT_INICIO_VIGENCIA_REF']
            DT_FIM_VIGENCIA_REF = row['DT_FIM_VIGENCIA_REF']
            DT_ATUALIZACAO_NREC = row['DT_ATUALIZACAO_NREC']
            NM_USUARIO_NREC = row['NM_USUARIO_NREC']
            NM_TECNICO = row['NM_TECNICO']

            sql = "INSERT INTO TASY.TUSS_MATERIAL_ITEM (NR_SEQUENCIA, NR_SEQ_CARGA_TUSS, DT_ATUALIZACAO, NM_USUARIO, CD_MATERIAL, DS_MATERIAL, CD_REF_FABRICANTE, DS_FABRICANTE, DT_INICIO_VIGENCIA, DT_FINAL_VIGENCIA, DS_APRESENTACAO, DS_LABORATORIO, IE_TIPO_ITEM, CD_MATERIAL_TUSS, NR_REGISTRO_ANVISA, DS_GRAU_RISCO, DT_FIM_IMPLANTACAO, DS_GRUPO_FARMACOLOGICO, DS_CLASSE_FARMACOLOGICA, DT_INICIO_VIGENCIA_REF, DT_FIM_VIGENCIA_REF, DT_ATUALIZACAO_NREC, NM_USUARIO_NREC, NM_TECNICO) " \
                  "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24)"
            cursor.execute(sql, (NR_SEQUENCIA, NR_SEQ_CARGA_TUSS, DT_ATUALIZACAO, NM_USUARIO, CD_MATERIAL, DS_MATERIAL, CD_REF_FABRICANTE, DS_FABRICANTE, DT_INICIO_VIGENCIA, DT_FINAL_VIGENCIA, DS_APRESENTACAO, DS_LABORATORIO, IE_TIPO_ITEM, CD_MATERIAL_TUSS, NR_REGISTRO_ANVISA, DS_GRAU_RISCO, DT_FIM_IMPLANTACAO, DS_GRUPO_FARMACOLOGICO, DS_CLASSE_FARMACOLOGICA, DT_INICIO_VIGENCIA_REF, DT_FIM_VIGENCIA_REF, DT_ATUALIZACAO_NREC, NM_USUARIO_NREC, NM_TECNICO))

        conn.commit()
        conn.close()

    except Exception as e:
        return f"Erro na linha {i}: {e}"

    return "Inserção concluída com sucesso"


# Carregar o CSV em um DataFrame
df = pd.read_csv(csv_file)

# Realizar a inserção no banco de dados e tratar erros
erros = []
for i, chunk in enumerate(pd.np.array_split(df, len(df) // 100)):
    result = inserir_dados_no_oracle(chunk)
    if "Erro" in result:
        erros.append(result)

# Salvar relatório de erros em um arquivo .txt
with open('erros.txt', 'w') as file:
    file.write("Registros com erro:\n")
    for erro in erros:
        file.write(erro + "\n")

print(f"Registros inseridos com sucesso: {len(df) - len(erros)}")
print(f"Registros com erro: {len(erros)}")
