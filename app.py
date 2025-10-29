import os
import pandas as pd
from datetime import datetime
from flask import Flask, render_template, request, send_file
import io
import json # Necessário para passar os dados do gráfico

# ----------------------------------------------------
# CONFIGURAÇÃO DE PASTAS E PREFIXOS
# ----------------------------------------------------
# Obtém o caminho do diretório onde este script (app.py) está a ser executado
diretorio_base = os.path.dirname(os.path.abspath(__file__))
# Aponta para a subpasta 'dados'
CAMINHO_PASTA = os.path.join(diretorio_base, 'dados')

PREFIXO = 'cda_fi_BLC'
COLUNA_FILTRO = 'DENOM_SOCIAL' 

app = Flask(__name__)
DF_UNICO = None 

# ----------------------------------------------------
# MAPEAMENTO DE COLUNAS (Como você definiu)
# ----------------------------------------------------
# Chave: Nome Original, Valor: Nome Novo
COLUNAS_FINAL_MAP = {
    'CD_ATIVO_BV_MERC': 'Ativos',
    'EMISSOR': 'Emissor do Ativo',
    'Perc_Pos_Final': 'Posição_Final',
    'VL_MERC_POS_FINAL': 'Valor de Mercado',
    'TP_APLIC': 'Tipo de Aplicação',
    'CNPJ_FUNDO_CLASSE': 'CNPJ do Fundo'
}

# ----------------------------------------------------
# FUNÇÃO DE CARREGAMENTO DE DADOS (Inalterada)
# ----------------------------------------------------
def carregar_dados_consolidados():
    global DF_UNICO
    if DF_UNICO is not None:
        return DF_UNICO

    lista_de_dataframes = []
    codificacoes = ['latin-1', 'windows-1252']

    for nome_arquivo in os.listdir(CAMINHO_PASTA):
        caminho_completo = os.path.join(CAMINHO_PASTA, nome_arquivo)
        
        if os.path.isfile(caminho_completo) and nome_arquivo.startswith(PREFIXO) and nome_arquivo.endswith('.csv'):
            df_lido_sucesso = False
            for encoding in codificacoes:
                try:
                    df_temp = pd.read_csv(caminho_completo, encoding=encoding, sep=';', decimal=',')
                    df_temp['Arquivo_Origem'] = nome_arquivo
                    lista_de_dataframes.append(df_temp)
                    df_lido_sucesso = True
                    break
                except Exception:
                    continue
            if not df_lido_sucesso:
                print(f"❌ Erro: Não foi possível ler o arquivo {nome_arquivo}.")

    if not lista_de_dataframes:
        raise Exception("Nenhum arquivo CSV compatível encontrado na pasta.")

    DF_UNICO = pd.concat(lista_de_dataframes, ignore_index=True)
    DF_UNICO = DF_UNICO.dropna(subset=[COLUNA_FILTRO]) 
    return DF_UNICO

# ----------------------------------------------------
# FUNÇÃO CENTRAL DE FILTRAGEM (CORRIGIDA)
# Retorna dados BRUTOS (números)
# ----------------------------------------------------
def preparar_dados_filtrados_brutos(df_completo, fundo_escolhido):
    """Filtra o DF, calcula o percentual e retorna os dados brutos (sem formatação)."""
    
    # 1. Filtra o DataFrame
    df_filtrado = df_completo[df_completo[COLUNA_FILTRO] == fundo_escolhido].copy()
    
    coluna_valor = 'VL_MERC_POS_FINAL'
    if coluna_valor not in df_filtrado.columns:
        # Se não houver a coluna, retorna dataframe vazio
        return pd.DataFrame()
        
    # 2. Garante que a coluna de valor é numérica (float)
    df_filtrado[coluna_valor] = (
        df_filtrado[coluna_valor]
        .astype(str)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
        .astype(float)
        .fillna(0)
    )
    
    # 3. Calcula o total do fundo
    total_fundo = df_filtrado[coluna_valor].sum()
    
    # 4. Calcula a nova coluna de percentual (como float)
    if total_fundo != 0:
        df_filtrado['Perc_Pos_Final'] = (df_filtrado[coluna_valor] / total_fundo) * 100
    else:
        df_filtrado['Perc_Pos_Final'] = 0.0

    return df_filtrado

# ----------------------------------------------------
# ROTAS DO FLASK (Lógica da Aplicação Web)
# ----------------------------------------------------

@app.route('/', methods=['GET', 'POST'])
def index():
    try:
        df = carregar_dados_consolidados()
    except Exception as e:
        return render_template('erro.html', mensagem=f"Erro ao carregar dados: {e}")

    if COLUNA_FILTRO not in df.columns:
        return render_template('erro.html', mensagem=f"A coluna de filtro ('{COLUNA_FILTRO}') não foi encontrada.")

    fundos_unicos = sorted(df[COLUNA_FILTRO].dropna().unique())
    
    if request.method == 'POST':
        fundo_escolhido = request.form.get('fundo_selecionado')
        
        if fundo_escolhido and fundo_escolhido in fundos_unicos:
            
            # 1. Pega os dados brutos (com números float)
            df_raw = preparar_dados_filtrados_brutos(df, fundo_escolhido)
            
            if df_raw.empty:
                return render_template('erro.html', mensagem=f"Não foram encontrados dados para o fundo {fundo_escolhido}.")

            # --- PREPARAÇÃO DOS DADOS PARA OS GRÁFICOS ---

            # 2. Gráfico 1: Por Ativo (Top 10 + Outros)
            # Garante que 'CD_ATIVO_BV_MERC' existe antes de usar
            if 'CD_ATIVO_BV_MERC' in df_raw.columns:
                df_top_assets = df_raw.nlargest(10, 'Perc_Pos_Final')
                outros_perc = df_raw[~df_raw.index.isin(df_top_assets.index)]['Perc_Pos_Final'].sum()
                
                chart1_labels = df_top_assets['CD_ATIVO_BV_MERC'].tolist() + ['Outros']
                chart1_data = df_top_assets['Perc_Pos_Final'].tolist() + [outros_perc]
                
                chart_data_ativos = {
                    "labels": chart1_labels,
                    "data": chart1_data
                }
            else:
                chart_data_ativos = {"labels": [], "data": []}

            # 3. Gráfico 2: Por Tipo de Aplicação
            if 'TP_APLIC' in df_raw.columns:
                df_grouped_type = df_raw.groupby('TP_APLIC')['Perc_Pos_Final'].sum().reset_index()
                
                chart_data_tipo = {
                    "labels": df_grouped_type['TP_APLIC'].tolist(),
                    "data": df_grouped_type['Perc_Pos_Final'].tolist()
                }
            else:
                chart_data_tipo = {"labels": [], "data": []}

            # --- PREPARAÇÃO DOS DADOS PARA A TABELA (Formatação) ---
            
            # 4. Seleciona apenas as colunas que queremos mostrar
            colunas_selecionadas = [col for col in COLUNAS_FINAL_MAP.keys() if col in df_raw.columns]
            df_tabela = df_raw[colunas_selecionadas].copy()

            # 5. Formata os números como texto para exibição
            if 'Perc_Pos_Final' in df_tabela.columns:
                df_tabela['Perc_Pos_Final'] = df_tabela['Perc_Pos_Final'].map('{:,.4f}%'.format)
            if 'VL_MERC_POS_FINAL' in df_tabela.columns:
                # Formato brasileiro (R$ 1.234,56)
                df_tabela['VL_MERC_POS_FINAL'] = df_tabela['VL_MERC_POS_FINAL'].apply(
                    lambda x: f"R$ {x:,.2f}".replace(',', '_TEMP_').replace('.', ',').replace('_TEMP_', '.')
                )

            # 6. Renomeia as colunas para os nomes bonitos
            df_tabela = df_tabela.rename(columns=COLUNAS_FINAL_MAP)

            # 7. Converte para HTML
            tabela_html = df_tabela.to_html(classes='table table-striped table-hover', index=False)
            
            return render_template('resultado.html', 
                                   fundo=fundo_escolhido, 
                                   total_posicoes=len(df_tabela),
                                   tabela_html=tabela_html,
                                   # Envia os dados dos gráficos para o HTML
                                   chart_data_ativos=json.dumps(chart_data_ativos),
                                   chart_data_tipo=json.dumps(chart_data_tipo)
                                  )

        else:
            return render_template('erro.html', mensagem="Fundo selecionado inválido ou não encontrado.")

    # Método GET
    return render_template('index.html', fundos=fundos_unicos)


@app.route('/download/<fundo>', methods=['POST'])


def download(fundo):
    """Exporta o DataFrame Filtrado para Excel (com números)."""
    try:
        df = carregar_dados_consolidados()
        
        # 1. Pega os dados brutos (com números float)
        df_raw = preparar_dados_filtrados_brutos(df, fundo)
        
        if df_raw.empty:
            return "Nenhum dado para este fundo.", 404
        
        # 2. Seleciona as colunas
        colunas_selecionadas = [col for col in COLUNAS_FINAL_MAP.keys() if col in df_raw.columns]
        df_exportar = df_raw[colunas_selecionadas].copy()
        
        # 3. Renomeia as colunas
        df_exportar = df_exportar.rename(columns=COLUNAS_FINAL_MAP)
            
        # Cria um buffer de memória para o arquivo Excel
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter') 
        df_exportar.to_excel(writer, index=False, sheet_name='Posicoes')
        writer.close()
        output.seek(0)

        # Cria o nome do arquivo seguro para download
        fundo_simples = "".join(c for c in fundo if c.isalnum() or c.isspace())[:30].replace(' ', '_')
        nome_arquivo_saida = f'POSICOES_{fundo_simples}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        
        return send_file(output, 
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                         as_attachment=True,
                         download_name=nome_arquivo_saida)
                         
    except Exception as e:
        return f"Erro na exportação do download: {e}", 500


if __name__ == '__main__':
    try:
        print("Preparando dados...")
        carregar_dados_consolidados()
        print("\n=======================================================")
        print("✅ Aplicação Web de Filtro Iniciada")
        print(f"Acesse o aplicativo em: http://127.0.0.1:5000/")
        print("=======================================================")
        app.run(debug=True)
    except Exception as e:

        print(f"\nFATAL: Não foi possível iniciar a aplicação. Erro: {e}")
