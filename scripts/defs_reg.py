### ----------------- BIBLIOTECAS ---------------

import os
import re
import keyring
import pyodbc
import pandas as pd
from datetime import datetime, date
from tqdm import tqdm
from itertools import product
import numpy as np
import io
from IPython.utils import io as ipython_io
from dateutil.relativedelta import relativedelta
from defs_utils import remove_test_patients, process_dataframe, comorb_ls, comorb_rn
import tempfile
import requests
from redcap import Project
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE  # já vem pronta
import xml.etree.ElementTree as ET
from collections import defaultdict
import json
import time


### ----------------- FUNÇOES ---------------

# --------- EXPORTANDO INSTRUMENTOS REDCAP ----------
def exportar_instrumento_redcap(nome_instrumento, api_url, api_key, campos_especificos=None):
    """
    Exporta um instrumento específico do REDCap e filtra pelo repeat_instrument.
    
    Args:
        nome_instrumento (str): O nome técnico do formulário no REDCap (ex: 'odontologia').
        api_url (str): URL da API.
        api_key (str): Token de acesso.
        campos_especificos (list): (Opcional) Lista de campos extras. Por padrão traz record_id + form.
    
    Returns:
        pd.DataFrame: DataFrame processado e filtrado.
    """
    
    # Configuração básica do payload
    data = {
        'token': api_key,
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'forms[0]': nome_instrumento, # Solicita o formulário inteiro
        'fields[0]': 'record_id',     # Garante que o record_id venha junto
        'returnFormat': 'json'
    }

    # Se quiser campos específicos além do formulário, adiciona aqui
    if campos_especificos:
        for i, campo in enumerate(campos_especificos, start=1):
            data[f'fields[{i}]'] = campo

    try:
        # Envia a requisição
        response = requests.post(api_url, data=data)
        
        if response.status_code != 200:
            print(f"❌ Erro na API para '{nome_instrumento}': {response.text}")
            return pd.DataFrame() # Retorna vazio em caso de erro

        # Cria o DataFrame
        df = pd.DataFrame(response.json())
        
        # Se o DataFrame estiver vazio, retorna logo
        if df.empty:
            print(f"⚠️ Aviso: Nenhum dado encontrado para '{nome_instrumento}'.")
            return df

        # --- APLICAÇÃO DO FILTRO SOLICITADO ---
        # Verifica se é um instrumento de repetição antes de filtrar
        if 'redcap_repeat_instrument' in df.columns:
            df_filtrado = df[df['redcap_repeat_instrument'] == nome_instrumento].copy()
            
            # Se o filtro zerou o dataframe (ex: dados existem, mas não como repetição),
            # pode ser que o dado esteja na linha base (sem repeat_instrument preenchido).
            # Mas seguindo sua regra estrita:
            df = df_filtrado
        
        # Opcional: Já padronizar o record_id para Int64 como conversamos antes
        if 'record_id' in df.columns:
            df['record_id'] = df['record_id'].astype('Int64')

        print(f"✅ '{nome_instrumento}' : {len(df)} registros.")
        return df

    except Exception as e:
        print(f"❌ Erro crítico ao processar '{nome_instrumento}': {e}")
        return pd.DataFrame()


def _to_float_series(s: pd.Series, preserve_precision: bool = True) -> pd.Series:
    """
    Normaliza strings numéricas para float com limpeza robusta.
    
    Args:
        preserve_precision: Se True, mantém precisão original (sem arredondamento automático)
    """
    if s is None:
        return s
    
    ss = (s.astype(str)
           .str.upper()
           .str.replace('&GT;', '>', regex=False)
           .str.replace('&LT;', '<', regex=False)
           .str.replace('<', '', regex=False)
           .str.replace('>', '', regex=False)
           .str.replace('"', '', regex=False)
           .str.replace('%', '', regex=False)
           .str.replace(' ', '', regex=False)
           .str.replace(',', '.', regex=False))
    
    # Remove caracteres não numéricos (exceto ponto e sinal)
    ss = ss.str.replace(r'[^0-9\.\-]', '', regex=True)

# # ----------------------------------------------
# # Função Mestre de Mapeamento transformar um formato "Longo" (onde a resposta está em uma linha) para um formato "Largo" (onde a resposta vira coluna)
# # ----------------------------------------------
# def processar_mapeamento_eav(df_bruto, dict_ls, dict_rn, chaves_agrupamento=None):
#     """
#     Função baseada estritamente na lógica original de separação Grupo A e B.
#     """
#     # Usando EXATAMENTE as mesmas chaves do seu pivot original
#     if chaves_agrupamento is None:
#         chaves_agrupamento = ['CD_ATENDIMENTO', 'CD_PACIENTE', 'DH_DOCUMENTO', 'CD_DOCUMENTO']
        
#     df = df_bruto.copy()
    
#     # 1. Inversão do dicionário (Identificador -> Coluna REDCap)
#     map_ident_para_coluna = {item: col for col, lista in dict_ls.items() for item in lista}

#     # 2. Flag
#     mask_mapeados = df['DS_IDENTIFICADOR'].isin(map_ident_para_coluna)

#     # 3. Grupo A: itens codificados via dict_rn
#     df_a = df[mask_mapeados].copy()
#     df_a['COLUNA'] = df_a['DS_IDENTIFICADOR'].map(map_ident_para_coluna)
#     df_a['VALOR'] = df_a['DS_IDENTIFICADOR'].map(dict_rn)

#     # 4. Grupo B: texto/booleanos
#     df_b = df[~mask_mapeados].copy()
#     df_b['COLUNA'] = df_b['DS_IDENTIFICADOR']

#     map_gen = {'SIM': 1, 'S': 1, 'TRUE': 1, 'NAO': 0, 'N': 0, 'FALSE': 0}

#     def map_boolean_preservando_tipo(v):
#         if pd.isna(v):
#             return np.nan
#         s = str(v).strip().upper()
#         if s in map_gen:
#             return map_gen[s]
#         return v

#     df_b['VALOR'] = df_b['DS_RESPOSTA'].apply(map_boolean_preservando_tipo)

#     # 5. Junta tudo
#     df_unificado = pd.concat([df_a, df_b], ignore_index=True)
    
#     # 6. ORDENAÇÃO ORIGINAL: Fundamental para o aggfunc='last' funcionar direito
#     if 'DH_DOCUMENTO' in df_unificado.columns:
#         df_unificado = df_unificado.sort_values(['CD_PACIENTE', 'DH_DOCUMENTO'], ascending=False)
        
#     # 7. PIVOTAGEM ORIGINAL
#     df_pivot = df_unificado.pivot_table(
#         index=chaves_agrupamento,
#         columns='COLUNA',
#         values='VALOR',
#         aggfunc='last' 
#     ).reset_index()
    
#     # 8. O EXTERMINADOR DE ".0" (Correção do comportamento do Pandas)
#     # Colunas que misturam números com NaN viram float (ex: 1.0). Vamos limpar isso.
#     cols_valores = [c for c in df_pivot.columns if c not in chaves_agrupamento]
    
#     for col in cols_valores:
#         # Verifica se a coluna virou float por causa do pivot
#         if pd.api.types.is_float_dtype(df_pivot[col]):
#             # Transforma 1.0 em 1, mas deixa NaN quieto
#             df_pivot[col] = df_pivot[col].apply(
#                 lambda x: int(x) if pd.notnull(x) and float(x).is_integer() else x
#             )
#             # Transforma em object para o Python não tentar forçar .0 de novo
#             df_pivot[col] = df_pivot[col].astype(object)

#     return df_pivot

# ----------------------------------------------
# ANTROPOMETRIA - Peso, Altura, IMC, Classificação
# ----------------------------------------------
def processar_antropometria(df, col_peso='peso', col_altura='altura'):
    """
    Processa IMC e Classificação Corporal de forma genérica.
    
    Argumentos:
        df: DataFrame contendo os dados.
        col_peso: Nome da coluna de peso no df original.
        col_altura: Nome da coluna de altura no df original.
    """
    # Cria uma cópia para evitar SettingWithCopyWarning
    df = df.copy()

    # Se as colunas não existirem, retorna o df original sem erro
    if col_peso not in df.columns or col_altura not in df.columns:
        print(f"Aviso: Colunas {col_peso} ou {col_altura} não encontradas.")
        return df

    # 1. Normalização da Altura (3 dígitos, cm)
    # Ex: '1.7' -> '170' | '170' -> '170'
    df['altura'] = (
        df[col_altura]
        .astype(str)
        .str.replace(',', '', regex=False)
        .str.replace('.', '', regex=False)
        .replace({'nan': '', 'None': ''})
        .apply(lambda x: x.ljust(3, '0')[:3] if x not in ['', 'None'] else '')
    )
    
    # 2. Tratamento do Peso
    df['peso'] = (
        df[col_peso]
        .astype(str)
        .str.replace(',', '.', regex=False) # Troca vírgula decimal por ponto
        .replace({'nan': '', 'None': ''})
        .str.strip()
    )
    
    # Converte para numérico para cálculos
    peso_val = pd.to_numeric(df['peso'], errors='coerce')
    altura_m = pd.to_numeric(df['altura'], errors='coerce') / 100.0
    
    # 3. Cálculo do IMC
    # Usamos o nome padrão 'imc' para a saída, independente da entrada
    df['imc'] = np.where(altura_m > 0, peso_val / (altura_m ** 2), np.nan)

    # 4. Classificação do IMC (Texto)
    def categorizar_imc(imc):
        if pd.isna(imc) or np.isinf(imc) or imc <= 0: 
            return ""
        if imc < 18.5: return "Magreza grau I"
        if imc < 25.0: return "Eutrofia"
        if imc < 30.0: return "Pré-obesidade"
        if imc < 35.0: return "Obesidade moderada (grau I)"
        if imc < 40.0: return "Obesidade severa (grau II)"
        return "Obesidade muito severa (grau III)"
    
    df['imc_classificacao'] = df['imc'].apply(categorizar_imc)

    # 5. Avaliação Corporal e Massa Crítica
    if 'avaliacao_corporal' not in df.columns:
        df['avaliacao_corporal'] = ""

    mask_aval_vazia = df['avaliacao_corporal'].fillna('').astype(str).str.strip() == ""

    # Regras baseadas no IMC (1=Obeso, 2=Desnutrido/Caquético)
    df.loc[mask_aval_vazia & (df['imc'] > 30), 'avaliacao_corporal'] = 1
    df.loc[mask_aval_vazia & (df['imc'] < 18) & (df['imc'] > 0), 'avaliacao_corporal'] = 2

    df['massa_corporal_critica'] = np.where(
        df['avaliacao_corporal'].fillna('').astype(str).str.strip() == "", 
        0, 
        1
    )

    # 6. Limpeza Final: Garante tipos para o REDCap (sem .0)
    cols_to_fix = ['avaliacao_corporal', 'massa_corporal_critica', 'imc']
    for col in cols_to_fix:
        # Transformamos em string limpa se for exportar ou mantemos object
        df[col] = df[col].astype(object)

    return df




### ----------------------------------------------------- 
# Mapeamento de comorbidades
### ----------------------------------------------------- 

# 2. Função de mapeamento corrigida
def gerar_comorb_mapeado(df_sql):
    df_sql = df_sql.copy()
    
    # 1. Padronização do ID
    if 'record_id' not in df_sql.columns:
        df_sql = df_sql.rename(columns={'CD_PACIENTE': 'record_id'})
        
    df_sql['record_id'] = (
        df_sql['record_id']
        .astype(str)
        .str.replace(r'\.0$', '', regex=True)
        .str.strip()
    )
    df_sql = df_sql.set_index('record_id')
    
    res = {}

    def get_clean(col):
        if col not in df_sql.columns:
            return pd.Series(pd.NA, index=df_sql.index)
        s = df_sql[col].astype(str).str.strip().str.upper()
        return s.replace({'NAN': pd.NA, 'NONE': pd.NA, '': pd.NA, 'NULL': pd.NA})

    # --- MAPEAMENTOS ---
    # Usamos o .map e depois forçamos a tipagem nullable
    #res['tabagismo'] = get_clean('TABAGISMO').map({'NAO': 0, 'EXTBG': 1, 'SIM': 2})
    res['dislipidemia'] = get_clean('DLP').map({'NAO': 0, 'SIM': 1})
    #res['hipertensao'] = get_clean('HAS').map({'NAO': 0, 'SIM': 1})
    #res['diabetes'] = get_clean('DM').map({'NAO': 0, 'DMID': 1, 'DMNID': 1})
    #res['drc_clcr'] = get_clean('DRC').map({'NAO': 0, 'SIM': 2, 'DIALITICO': 4})
    #res['dpoc'] = get_clean('DPOC').map({'NAO': 0, 'SIM': 1})
    #res['avc_ait_previo'] = get_clean('AVC').map({'NAO': 0, 'SIM': 1})
    #res['angina'] = get_clean('ANGINA').map({'NAO': 0, 'SIM': 1})
    
    # Angina CCS
    #ang_tipos = get_clean('ANGINA TIPOS')
    #cond_ccs = [
    #    ang_tipos.str.contains('CCS1', na=False),
    #    ang_tipos.str.contains('CCS2', na=False),
    #    ang_tipos.str.contains('CCS3', na=False),
    #    ang_tipos.str.contains('CCS4', na=False)
    #]
    #res['ccs'] = pd.Series(np.select(cond_ccs, [1, 2, 3, 4], default=pd.NA), index=df_sql.index)

    # NYHA
    #res['classe_nyha'] = get_clean('DISPNEIA TIPOS').map({'DISPNEIAI': 1, 'DISPNEIAII': 2, 'DISPNEIAIII': 3, 'DISPNEIAIV': 4})
    #
    #res['carga_sintomas_sincope'] = get_clean('SINCOPE').map({'NAO': 0, 'SIM': 1})
    #res['palpitacoes'] = get_clean('PALPITACAO').map({'NAO': 0, 'SIM': 1})
    #res['arritmias'] = get_clean('ARRITMIA').map({'NAO': 0, 'SIM': 1})

    ## Arritmias / MP - Trocado para astype('Int64')
    #arr_tipos = get_clean('ARRITMIA TIPOS')
    #res['possui_mp_trc_cdi'] = (arr_tipos == 'MP').astype('Int64')
    #res['outra_cirurgia_anterior___2'] = (arr_tipos == 'MP').astype('Int64')
    #
    # # Valvulopatias
    # def is_sim(col): return get_clean(col) == 'SIM'
    # c_eao, c_iao = is_sim('EAO'), is_sim('IAO')
    # c_emi, c_imi = is_sim('EMI'), is_sim('IMI')
    # c_etr, c_itri = is_sim('ETRI'), is_sim('ITRI')

    # res['valvulopatia_aortica'] = (c_eao | c_iao).astype('Int64')
    # res['valvulopatia_mitral'] = (c_emi | c_imi).astype('Int64')
    # res['valvulopatia_tricuspide'] = (c_etr | c_itri).astype('Int64')

    # res['tipo_valvulopatia_ao'] = pd.Series(np.select([c_eao & c_iao, c_eao, c_iao], [3, 1, 2], default=pd.NA), index=df_sql.index)
    # res['tipo_valvulopatia_mi'] = pd.Series(np.select([c_emi & c_imi, c_emi, c_imi], [3, 1, 2], default=pd.NA), index=df_sql.index)
    # res['tipo_valvulopatia_tr'] = pd.Series(np.select([c_etr & c_itri, c_etr, c_itri], [3, 1, 2], default=pd.NA), index=df_sql.index)

    # # Checkboxes Origem - Loop para garantir Int64
    # vp_tipos = get_clean('VALVOPATIA TIPOS')
    # vp_prot = get_clean('VALVOPATIA PROTESE')
    
    # for i, suffix in enumerate(['_1', '_2', '_3'], 1):
    #     v_col = ['valvulopatia_aortica', 'valvulopatia_mitral', 'valvulopatia_tricuspide'][i-1]
    #     res[f'valvula_nativa{suffix}'] = ((vp_tipos == 'NATIVA') & (res[v_col] == 1)).astype('Int64')
    #     res[f'protese_mecanica{suffix}'] = ((vp_prot == 'MECANICA') & (res[v_col] == 1)).astype('Int64')
    #     res[f'protese_biologica{suffix}'] = ((vp_prot == 'BIOLOGICA') & (res[v_col] == 1)).astype('Int64')
    
    # Coronárias
    #res['dac_exame'] = get_clean('DAC').map({'NAO': 0, 'SIM': 1})
    #dac_tipos = get_clean('DAC TIPOS')
    #res['intervencoes_cardiacas_previas'] = dac_tipos.isin(['ICPPREVIA', 'RMPREVIA']).astype('Int64')
    #res['outra_cirurgia_anterior___1'] = (dac_tipos == 'ICPPREVIA').astype('Int64')
    #res['tipo_de_cirurgia_anterior___1'] = (dac_tipos == 'RMPREVIA').astype('Int64')

    # --- CONVERSÃO FINAL DE SEGURANÇA ---
    df_final = pd.DataFrame(res, index=df_sql.index)
    
    # Forçamos todas as colunas a serem Int64 antes do reset_index
    for col in df_final.columns:
        df_final[col] = pd.to_numeric(df_final[col], errors='coerce').astype('Int64')

    return df_final.reset_index()


### ----------------------------------------------------- 
### ----------------------------------------------------- 
def organizar_dados_paciente(df, chaves_agrupamento, colunas_manter=None, preencher_nulos_com=None):
    """
    Consolida múltiplas linhas de um mesmo paciente/atendimento em uma linha única.
    Resolve problemas de formato longo (EAV) pegando o primeiro valor não-nulo de cada coluna.

    Args:
        df: DataFrame original.
        chaves_agrupamento: Lista de colunas que identificam a linha única (ex: ['CD_PACIENTE', 'DH_DOCUMENTO']).
        colunas_manter: (Opcional) Lista de colunas específicas que você quer que sobrem no DataFrame final.
        preencher_nulos_com: (Opcional) Valor para substituir os NaNs finais (ex: 0, "", ou manter None se omitido).
    """
    # 1. Validação de segurança
    chaves_presentes = [c for c in chaves_agrupamento if c in df.columns]
    if not chaves_presentes:
        print("Erro: Nenhuma chave de agrupamento encontrada no DataFrame.")
        return df

    # 2. Filtragem de colunas (Se o usuário pediu colunas específicas)
    if colunas_manter:
        # Garante que as chaves de agrupamento não sejam removidas no filtro
        colunas_finais = list(set(chaves_presentes + [c for c in colunas_manter if c in df.columns]))
        df_temp = df[colunas_finais].copy()
    else:
        df_temp = df.copy()

    # 3. O Coração da Otimização: Agrupamento Vetorizado
    # O .first() do Pandas automaticamente pega o primeiro valor que não é nulo dentro do grupo.
    # Isso substitui milhares de linhas de loops 'for'.
    df_consolidado = df_temp.groupby(chaves_presentes, dropna=False).first().reset_index()

    # 4. Tratamento de Nulos Pós-Agrupamento (Opcional)
    if preencher_nulos_com is not None:
        # Pega todas as colunas exceto as chaves de agrupamento
        cols_valores = [c for c in df_consolidado.columns if c not in chaves_presentes]
        df_consolidado[cols_valores] = df_consolidado[cols_valores].fillna(preencher_nulos_com)

    return df_consolidado




# def organizar_dados_paciente(df, chaves_agrupamento=None, colunas_fixas=None, colunas_desejadas=None):
#     """
#     Função unificada e otimizada para organizar dados de pacientes.
    
#     Parâmetros:
#     - df: DataFrame de entrada
#     - chaves_agrupamento: lista de colunas para agrupar (ex: ['CD_ATENDIMENTO', 'record_id', 'DH_DOCUMENTO'])
#     - colunas_fixas: lista de colunas extras fixas a incluir no agrupamento (ex: ['NM_PRESTADOR'])
#     - colunas_desejadas: lista específica de colunas a manter (ignora as chaves; usa .first() nelas)
    
#     Se colunas_desejadas=None, mantém todas as colunas.
#     """
#     # Definir chaves padrão baseadas no seu contexto de fisioterapia/REDcap
#     if chaves_agrupamento is None:
#         chaves_agrupamento = ['CD_ATENDIMENTO', 'record_id', 'DH_DOCUMENTO']
    
#     # Verificar se todas as chaves existem
#     chaves_faltando = [k for k in chaves_agrupamento if k not in df.columns]
#     if chaves_faltando:
#         raise ValueError(f"Chaves faltando no DataFrame: {chaves_faltando}")
    
#     # Colunas fixas extras (não variam por grupo)
#     if colunas_fixas is None:
#         colunas_fixas = []
#     else:
#         colunas_fixas = [c for c in colunas_fixas if c in df.columns and c not in chaves_agrupamento]
    
#     todas_chaves = chaves_agrupamento + colunas_fixas
    
#     # Se colunas_desejadas fornecida, filtrar DataFrame primeiro para eficiência
#     if colunas_desejadas is not None:
#         # Garantir que chaves estão incluídas
#         colunas_validas = list(set(chaves_agrupamento + [c for c in colunas_desejadas if c in df.columns]))
#         df = df[colunas_validas].copy()
#     else:
#         colunas_desejadas = [c for c in df.columns if c not in todas_chaves]
    
#     # Agrupamento vetorizado: .first() pega primeiro não-NaN automaticamente!
#     df_organizado = df.groupby(todas_chaves, dropna=False, as_index=False).first()
    
#     # Reordenar colunas: chaves primeiro, depois desejadas
#     cols_finais = todas_chaves + sorted([c for c in colunas_desejadas if c in df_organizado.columns])
#     df_organizado = df_organizado[cols_finais]
    
#     # Substituir restantes NaN por 0 apenas nas colunas desejadas (como no seu 1º código)
#     for col in colunas_desejadas:
#         if col in df_organizado.columns:
#             df_organizado[col] = df_organizado[col].fillna(0)
    
#     return df_organizado


# # ------------- COMO USAR-----------------------

# # Informando colunas desejadas para avaliar
# cols_df = []

# df_organizado = organizar_dados_paciente(
#     df, 
#     chaves_agrupamento=['CD_ATENDIMENTO', 'record_id', 'DH_DOCUMENTO'],
#     colunas_desejadas=cols_df[3:]  # só as variáveis clínicas
# )

# # Informando colunas chaves para agrupar
# df_organizado = organizar_dados_paciente(
#     df,
#     chaves_agrupamento=['CD_DOCUMENTO', 'CD_PACIENTE', 'CD_ATENDIMENTO', 'DH_DOCUMENTO'],
#     colunas_fixas=['NM_PRESTADOR']
#     # Sem colunas_desejadas: mantém todas!
# )

### ----------------------------------------------------- 
### ----------------------------------------------------- 
# def organizar_df_otimizado(df):
#     """
#     Agrupa os dados pelo ID do Documento.
#     Se houver linhas duplicadas para o mesmo documento (ex: estrutura EAV),
#     pega o primeiro valor não nulo encontrado.
#     """
#     # 1. Definir as colunas que identificam unicamente o formulário
#     # Adicionamos CD_DOCUMENTO para garantir que os 5 docs do dia não se misturem
#     chaves_agrupamento = ['CD_DOCUMENTO', 'CD_PACIENTE', 'CD_ATENDIMENTO', 'DH_DOCUMENTO']
    
#     # Adicione colunas que não variam por documento, mas você quer manter (ex: prestador)
#     colunas_extras_fixas = ['NM_PRESTADOR']
#     for col in colunas_extras_fixas:
#         if col in df.columns and col not in chaves_agrupamento:
#             chaves_agrupamento.append(col)

#     # 2. Agrupamento Otimizado (Vectorized)
#     # O .first() ignora NaNs automaticamente. Ele pega o primeiro valor válido.
#     # Isso substitui todo aquele seu loop 'for' manual.
#     df_organizado = df.groupby(chaves_agrupamento, as_index=False).first()
    
#     return df_organizado


### ----------------------------------------------------- 
### ----------------------------------------------------- 
# Normalizador genérico de número: limpa lixos, troca vírgula por ponto e tenta converter
def normalizar_series_numericas(s: pd.Series) -> pd.Series:
    s = s.astype(str)
    s = (s.str.replace('"', '', regex=False)
           .str.replace(r'\\', '', regex=True)
           .str.replace('%', '', regex=False)
           .str.replace('-', '', regex=False)
           .str.replace(' ', '', regex=False)
           .str.replace(',', '.', regex=False)
           .str.replace('&gt;', '>', regex=False)
           .str.replace('>', '', regex=False)
           .str.strip())
    # Remove strings que são só pontos: '.', '..', '...'
    s = s.apply(lambda x: '' if re.fullmatch(r'\.+', x or '') else x)
    # Zera tokens de nulo conhecidos
    null_tokens = {'nan', 'NaN', 'None', '<NA>', '&lt;NA&gt;', 'NAT', 'pd.NA', ''}
    s = s.apply(lambda x: '' if str(x).strip() in null_tokens else str(x).strip())
    # Converte para numérico onde possível
    out = pd.to_numeric(s, errors='coerce')
    return out

### ----------------------------------------------------- 
### ----------------------------------------------------- 
def vincular_referencia_temporal(
    df_principal, 
    df_referencia, 
    col_data_principal='DH_DOCUMENTO',
    col_data_referencia='data_cirurgia',
    col_instancia='redcap_repeat_instance',
    col_id='record_id',
    col_atend_principal=None,  # ex: 'CD_ATENDIMENTO'
    col_atend_referencia=None,  # ex: 'cd_atendimento'
    tolerancia_dias=90,
    direction='nearest'
):
    """
    Vincula instância e data_cirurgia de df_referencia ao df_principal.
    Prioridade: 1) Match exato por ID + atendimento (se cols informadas).
                2) Match temporal via merge_asof (data_cirurgia como marco).
    Regras de instância: ref > original > '1'.
    
    Retorna df_principal com colunas adicionadas/atualizadas: data_cirurgia, redcap_repeat_instance.
    """
    # 1. Cópias seguras e padronização (IDs/decimais/strings)
    df_main = df_principal.copy()
    df_ref = df_referencia.copy()
    
    # Datas como datetime
    df_main[col_data_principal] = pd.to_datetime(df_main[col_data_principal], errors='coerce')
    df_ref[col_data_referencia] = pd.to_datetime(df_ref[col_data_referencia], errors='coerce')
    
    # Padronizar ID e atendimento (remove .0, strip)
    for col in [col_id] + ([col_atend_principal, col_atend_referencia] if col_atend_principal else []):
        if col and col in df_main.columns:
            df_main[col] = df_main[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        if col and col in df_ref.columns:
            df_ref[col] = df_ref[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
    
    # Separar main sem data (não processam)
    df_main_valid = df_main[df_main[col_data_principal].notna()].copy()
    df_main_sem_data = df_main[df_main[col_data_principal].isna()].copy()
    
    df_ref_valid = df_ref[df_ref[col_data_referencia].notna()].copy()
    cols_ref = [col_id, col_data_referencia, col_instancia]
    if col_atend_referencia:
        cols_ref.append(col_atend_referencia)
    
    df_result = df_main_valid.copy()
    
    # 2. FASE 1: Match EXATO por atendimento (se aplicável)
    usar_atend = col_atend_principal and col_atend_referencia and col_atend_principal in df_main_valid.columns
    if usar_atend and not df_main_valid.empty and not df_ref_valid.empty:
        df_ref_dedup = df_ref_valid.drop_duplicates(subset=[col_id, col_atend_referencia])
        df_merged_atend = pd.merge(
            df_main_valid, df_ref_dedup[cols_ref],
            left_on=[col_id, col_atend_principal], right_on=[col_id, col_atend_referencia],
            how='left', suffixes=('', '_ref')
        )
        mask_match = df_merged_atend[f'{col_data_referencia}_ref'].notna()
        df_result = df_merged_atend[~mask_match][df_main_valid.columns].copy()  # Sem match vão para temporal
        df_exato = df_merged_atend[mask_match].copy()
    else:
        df_exato = pd.DataFrame()
    
    # 3. FASE 2: Match TEMPORAL nos restantes
    df_para_temporal = df_result if 'df_result' in locals() else df_main_valid
    if not df_para_temporal.empty and not df_ref_valid.empty:
        df_para_temporal = df_para_temporal.sort_values(col_data_principal)
        df_temporal = pd.merge_asof(
            df_para_temporal, df_ref_valid[cols_ref],
            left_on=col_data_principal, right_on=col_data_referencia,
            by=col_id, direction=direction, tolerance=pd.Timedelta(days=tolerancia_dias),
            suffixes=('', '_ref')
        )
    else:
        df_temporal = df_para_temporal.copy()
    
    # 4. Concat e resolver instância (prioridade ref > original > 1)
    df_final = pd.concat([df_exato, df_temporal, df_main_sem_data], ignore_index=True)
    
    def resolver_instancia(row):
        ref_inst = row.get(f'{col_instancia}_ref')
        if pd.notna(ref_inst):
            return str(int(float(ref_inst)))
        orig_inst = row.get(col_instancia)
        if pd.notna(orig_inst):
            return str(int(float(orig_inst)))
        return '1'
    
    df_final[col_instancia] = df_final.apply(resolver_instancia, axis=1)
    df_final[f'{col_data_referencia}'] = df_final[f'{col_data_referencia}_ref']  # Traz data_cirurgia
    
    # Limpa auxiliares
    df_final.drop(columns=[f'{col_instancia}_ref', f'{col_data_referencia}_ref'], inplace=True, errors='ignore')
    
    return df_final


### ----------------------------------------------------- 
### ----------------------------------------------------- 
def classificar_eventos_flexivel(
    df_entrada,
    tipo_fluxo='cirurgia_fisio',  # 'cirurgia_fisio' | 'geronto_odonto_psico' | custom
    col_data_doc='DH_DOCUMENTO',
    col_instancia='redcap_repeat_instance',
    col_atend=None,  # 'CD_ATENDIMENTO' para isolar
    col_setor='CD_DOCUMENTO',  # ou 'NM_SETOR'
    col_data_marco1=None,  # ex: 'data_cirurgia' ou 'data_internacao'
    col_data_marco2=None,   # ex: 'data_alta' para pós-alta
    evento_pre='pre_internacao',
    evento_internacao='durante_internacao',
    evento_pos_intern='pos_internacao',
    evento_amb='ambulatorio'
):
    """
    Classifica para QUALQUER fluxo clínico:
    - 'cirurgia_fisio': Pré-cirurgia → UTI/ENF → AMB (seu caso original).
    - 'geronto_odonto_psico': Pré-internação → Internação → Pós-intern → AMB.
    - Custom: passe marcos (data_cirurgia/alta) e eventos.
    
    Detecta fase por: atendimento → data_marco → setor.
    """
    df = df_entrada.copy()
    df[col_data_doc] = pd.to_datetime(df[col_data_doc], errors='coerce')
    
    # Datas marco (opcional)
    if col_data_marco1: df[col_data_marco1] = pd.to_datetime(df[col_data_marco1], errors='coerce')
    if col_data_marco2: df[col_data_marco2] = pd.to_datetime(df[col_data_marco2], errors='coerce')
    
    tem_doc = df[col_data_doc].notna()
    mask_atend = pd.Series([True] * len(df), index=df.index)
    if col_atend and col_atend in df:
        mask_atend = df[col_atend].notna()
    
    # Máscaras setor (padrão fisio, override por tipo)
    def mascara_setor(vals):
        if isinstance(vals, (list, tuple)):
            return pd.to_numeric(df[col_setor], errors='coerce').isin(vals)
        return df[col_setor].astype(str).str.contains(vals, case=False, na=False)
    
    # Config por tipo_fluxo
    if tipo_fluxo == 'cirurgia_fisio':
        # UTI/ENF/AMB como original
        is_uti = mascara_setor((1140, 1145, 1137))
        is_enf = mascara_setor((1136, 1141))
        is_amb = mascara_setor('AMBULATORIO')
        tem_marco1 = df[col_data_marco1].notna() if col_data_marco1 else pd.Series([False]*len(df))
        
        conds = [
            tem_doc & ~tem_marco1 & mask_atend,  # Pré-cirurgia
            tem_marco1 & (df[col_data_doc] < df[col_data_marco1]) & mask_atend,  # Pré real
            tem_marco1 & (df[col_data_doc] >= df[col_data_marco1]) & is_uti & mask_atend,
            tem_marco1 & (df[col_data_doc] >= df[col_data_marco1]) & is_enf & mask_atend,
            tem_marco1 & (df[col_data_doc] >= df[col_data_marco1]) & is_amb & mask_atend
        ]
        escolhas = [evento_pre, evento_pre, evento_internacao, evento_pos_intern, evento_amb]
        
    elif tipo_fluxo == 'geronto_odonto_psico':
        # Fluxo completo: pré → intern → pós-intern → amb (usa data_internacao + data_alta)
        tem_intern = df[col_data_marco1].notna() if col_data_marco1 else pd.Series([False]*len(df))
        tem_alta = df[col_data_marco2].notna() if col_data_marco2 else pd.Series([False]*len(df))
        is_amb = mascara_setor('AMBULATORIO')
        
        conds = [
            tem_doc & ~tem_intern & mask_atend,  # Pré-internação
            tem_intern & (df[col_data_doc] < df[col_data_marco1]) & mask_atend,  # Pré-intern real
            tem_intern & (df[col_data_marco1] <= df[col_data_doc]) & (~tem_alta | (df[col_data_doc] < df[col_data_marco2])) & mask_atend,  # Durante intern
            tem_alta & (df[col_data_doc] >= df[col_data_marco2]) & ~is_amb & mask_atend,  # Pós-alta internado
            is_amb & mask_atend  # Ambulatorial (qualquer tempo)
        ]
        escolhas = [evento_pre, evento_pre, evento_internacao, evento_pos_intern, evento_amb]
        
    else:  # Custom: use marcos informados
        conds = [
            tem_doc & mask_atend,  # Default pré se sem marcos
            (df[col_data_marco1] <= df[col_data_doc]) & mask_atend  # Pós-marco1
        ]
        escolhas = [evento_pre, evento_pos_intern]
    
    df['redcap_event_name'] = np.select(conds, escolhas, default=evento_pre)
    df[col_instancia] = pd.to_numeric(df[col_instancia], errors='coerce').fillna(1).astype(int)
    
    return df


### ----------------------------------------------------- 
### ----------------------------------------------------- 
import pandas as pd
import numpy as np

def vincular_instancias_temporal(
    df_principal, 
    df_referencia, 
    col_data_principal='DH_DOCUMENTO', 
    col_data_referencia='data_cirurgia',
    col_instancia='redcap_repeat_instance',
    col_id='record_id',
    tolerancia_dias=90,
    direction='nearest',
    col_atend_principal=None, 
    col_atend_referencia=None
):
    """
    Vincula instâncias do REDCap garantindo que MÚLTIPLAS CIRURGIAS na mesma
    internação sejam respeitadas temporalmente (Fim do bloqueio por keep='last').
    """

    # ------------------------------------------------------------
    # 1) Cópias e Tipagem Segura
    # ------------------------------------------------------------
    df_main = df_principal.copy()
    df_ref = df_referencia.copy()

    def _norm_id(s: pd.Series) -> pd.Series:
        return s.astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # Normaliza IDs
    df_main[col_id] = _norm_id(df_main[col_id])
    df_ref[col_id]  = _norm_id(df_ref[col_id])

    # Normaliza atendimentos e cria uma coluna unificada para o merge
    usar_atendimento = (col_atend_principal is not None and col_atend_referencia is not None)
    if usar_atendimento:
        if col_atend_principal in df_main.columns:
            df_main['_match_atend'] = _norm_id(df_main[col_atend_principal])
        else:
            df_main['_match_atend'] = ''
            
        if col_atend_referencia in df_ref.columns:
            df_ref['_match_atend'] = _norm_id(df_ref[col_atend_referencia])
        else:
            df_ref['_match_atend'] = ''

    # Ajuste de Fuso Horário
    def _to_naive_dt(s: pd.Series) -> pd.Series:
        s = pd.to_datetime(s, errors='coerce')
        try:
            if hasattr(s.dt, 'tz') and s.dt.tz is not None:
                s = s.dt.tz_localize(None)
        except Exception:
            pass
        return s

    df_main[col_data_principal] = _to_naive_dt(df_main[col_data_principal])
    df_ref[col_data_referencia] = _to_naive_dt(df_ref[col_data_referencia])

    # ------------------------------------------------------------
    # 2) Separação por presença de data
    # ------------------------------------------------------------
    df_main_validos   = df_main.dropna(subset=[col_id, col_data_principal]).copy()
    df_main_sem_data  = df_main[df_main[col_data_principal].isna()].copy()
    df_ref_validos    = df_ref.dropna(subset=[col_id, col_data_referencia]).copy()

    cols_ref_merge = [col_id, col_data_referencia, col_instancia]
    if 'redcap_event_name' in df_ref_validos.columns:
        cols_ref_merge.append('redcap_event_name')

    # ------------------------------------------------------------
    # 3) FUNÇÃO CENTRAL: Merge Temporal Inteligente
    # ------------------------------------------------------------
    def _merge_asof_seguro(left, right, by_cols):
        """
        Executa merge_asof agrupando pelos by_cols (Ex: ID + Atendimento).
        Se houver múltiplas cirurgias, ele acha a mais próxima. NADA é apagado.
        """
        if left.empty or right.empty:
            return left.copy()

        # O merge_asof exige que ambos estejam perfeitamente ordenados
        left2 = left.sort_values([col_id, col_data_principal], kind='mergesort')
        right2 = right.sort_values([col_id, col_data_referencia], kind='mergesort')
        
        # Garante que não pediremos colunas duplicadas no merge
        right_cols = list(set(cols_ref_merge + by_cols))

        try:
            out = pd.merge_asof(
                left2,
                right2[right_cols],
                left_on=col_data_principal,
                right_on=col_data_referencia,
                by=by_cols, # A mágica acontece aqui (Agrupa sem deletar)
                direction=direction,
                tolerance=pd.Timedelta(days=tolerancia_dias),
                suffixes=('', '_ref')
            )
            return out
        except ValueError:
            # Fallback seguro caso haja erro de indexação bizarro do Pandas
            chunks = []
            for group_keys, gleft in left2.groupby(by_cols, sort=False):
                # Isola a referência equivalente
                if isinstance(group_keys, tuple):
                    mask = np.ones(len(right2), dtype=bool)
                    for col, val in zip(by_cols, group_keys):
                        mask &= (right2[col] == val)
                    gright = right2[mask]
                else:
                    gright = right2[right2[by_cols[0]] == group_keys]

                if gright.empty:
                    chunks.append(gleft.assign(**{
                        col_data_referencia: pd.NaT,
                        f'{col_instancia}_ref': pd.NA
                    }))
                else:
                    merged = pd.merge_asof(
                        gleft.sort_values(col_data_principal, kind='mergesort'),
                        gright[right_cols].sort_values(col_data_referencia, kind='mergesort'),
                        left_on=col_data_principal,
                        right_on=col_data_referencia,
                        direction=direction,
                        tolerance=pd.Timedelta(days=tolerancia_dias),
                        suffixes=('', '_ref')
                    )
                    chunks.append(merged)
            return pd.concat(chunks, ignore_index=True)

    # ------------------------------------------------------------
    # 4) FASE 1 — Temporal travado no ATENDIMENTO
    # ------------------------------------------------------------
    df_resolvidos_exato = pd.DataFrame()
    df_para_temporal = df_main_validos.copy()

    if usar_atendimento and not df_main_validos.empty and not df_ref_validos.empty:
        # AGORA SIM: Faz o link temporal, mas OBRIGA a ser na mesma internação
        df_tentativa = _merge_asof_seguro(df_main_validos, df_ref_validos, by_cols=[col_id, '_match_atend'])

        mask_match = df_tentativa[col_data_referencia].notna()
        df_resolvidos_exato = df_tentativa[mask_match].copy()
        
        # O que falhou no match de atendimento (Ex: data muito fora do range) vai pra fase 2
        df_para_temporal = df_main_validos.loc[~mask_match].copy()

    # ------------------------------------------------------------
    # 5) FASE 2 — Temporal travado apenas no PACIENTE (Fallback Geral)
    # ------------------------------------------------------------
    if not df_para_temporal.empty and not df_ref_validos.empty:
        df_resolvidos_temporal = _merge_asof_seguro(df_para_temporal, df_ref_validos, by_cols=[col_id])
    else:
        df_resolvidos_temporal = df_para_temporal.copy()

    # ------------------------------------------------------------
    # 6) Reconstrução e Resolução da Instância
    # ------------------------------------------------------------
    df_final = pd.concat([df_resolvidos_exato, df_resolvidos_temporal, df_main_sem_data], ignore_index=True)

    def resolver_instancia(row):
        inst_ref = row.get(f'{col_instancia}_ref')
        if pd.notna(inst_ref):
            try: return str(int(float(inst_ref)))
            except: pass

        inst_ori = row.get(col_instancia)
        if pd.notna(inst_ori):
            try: return str(int(float(inst_ori)))
            except: pass

        return '1'

    df_final[col_instancia] = df_final.apply(resolver_instancia, axis=1)

    # Faxina das colunas auxiliares
    cols_limpeza = [f'{col_instancia}_ref', '_match_atend']
    df_final.drop(columns=[c for c in cols_limpeza if c in df_final.columns], inplace=True, errors='ignore')

    return df_final


### ----------------------------------------------------- 
### ----------------------------------------------------- 
def vincular_cirurgia_por_atendimento(
    df_itens, 
    df_cirurgia, 
    col_id='record_id',
    col_atendimento_item='CD_ATENDIMENTO',   # Atendimento na tabela de fisio
    col_atendimento_cirurgia='cd_atendimento', # Atendimento na tabela de cirurgia (atenção ao case sensitive)
    col_data_item='DH_DOCUMENTO',
    col_data_cirurgia='data_cirurgia',
    col_instancia_cirurgia='redcap_repeat_instance'
):
    """
    Vincula os dados da cirurgia (Data e Instância) ao dataframe de itens (Fisio)
    usando a chave composta: record_id + CD_ATENDIMENTO.
    """
    
    # --- 1. Cópias de segurança e Padronização ---
    df_main = df_itens.copy()
    df_ref = df_cirurgia.copy()

    # Converter datas
    df_main[col_data_item] = pd.to_datetime(df_main[col_data_item], errors='coerce')
    df_ref[col_data_cirurgia] = pd.to_datetime(df_ref[col_data_cirurgia], errors='coerce')

    # Limpar e padronizar IDs e Atendimentos para string (remove decimais .0 se houver)
    for df_temp, c_id, c_atend in [(df_main, col_id, col_atendimento_item), 
                                   (df_ref, col_id, col_atendimento_cirurgia)]:
        df_temp[c_id] = df_temp[c_id].astype(str).str.replace(r'\.0$', '', regex=True)
        df_temp[c_atend] = df_temp[c_atend].astype(str).str.replace(r'\.0$', '', regex=True)

    # --- 2. Seleção de colunas úteis da cirurgia ---
    # Queremos trazer a data da cirurgia e a instância correta
    cols_ref = [col_id, col_atendimento_cirurgia, col_data_cirurgia, col_instancia_cirurgia]
    
    # Remove duplicatas na tabela de cirurgia (caso haja linhas sujas para o mesmo atendimento)
    # Assume-se que 1 Atendimento = 1 Cirurgia Principal
    df_ref_unique = df_ref[cols_ref].drop_duplicates(subset=[col_id, col_atendimento_cirurgia])

    # --- 3. Merge (Left Join) ---
    # Mantemos todas as evoluções (left), trazendo dados da cirurgia onde der match
    df_final = pd.merge(
        df_main,
        df_ref_unique,
        left_on=[col_id, col_atendimento_item],
        right_on=[col_id, col_atendimento_cirurgia],
        how='inner',
        suffixes=('', '_cirurgia')
    )

    # Se a coluna de atendimento tiver nomes diferentes, remove a duplicada criada pelo merge
    if col_atendimento_item != col_atendimento_cirurgia:
        df_final.drop(columns=[col_atendimento_cirurgia], inplace=True, errors='ignore')

    return df_final

def classificar_eventos_por_atendimento(
    df_entrada, 
    evento_pre, 
    evento_uti, 
    evento_enfermaria,
    evento_amb,
    col_data_referencia='data_cirurgia',   
    col_data_documento='DH_DOCUMENTO',
    col_instancia='redcap_repeat_instance',
    # --- Parâmetros Flexíveis ---
    col_criterio_local='CD_DOCUMENTO',     # Pode ser 'CD_DOCUMENTO' ou 'NM_SETOR'
    val_uti=(1140, 1145, 1137),            # Pode ser tupla de IDs ou string 'UTI'
    val_enfermaria=(1136, 1141),           # Pode ser tupla de IDs ou string 'ENF'
    val_amb='AMBULATORIO'				   # Pode ser tupla de IDs ou string 'AMBULATORIO'
):
    """
    Classifica em Pré ou Pós (UTI/Enfermaria) ou Ambulatório (Alta/Follow-up) de forma flexível.
    Default (caso não case com nada): evento_pre.
    Aceita classificação por IDs (usando lista/tupla) ou por texto (usando string parcial).
    """
    df = df_entrada.copy()

    # 1. Garantia de datetime
    df[col_data_documento] = pd.to_datetime(df[col_data_documento], errors='coerce')
    df[col_data_referencia] = pd.to_datetime(df[col_data_referencia], errors='coerce')
    
    # Validação
    if col_criterio_local not in df.columns:
        raise ValueError(f"A coluna '{col_criterio_local}' não existe no DataFrame.")

    # 2. Função interna para criar máscara
    def criar_mascara(coluna, valores):
        if valores is None:
            return pd.Series([False] * len(df), index=df.index)
        
        if isinstance(valores, (list, tuple, set)):
            return pd.to_numeric(coluna, errors='coerce').isin(valores)
        elif isinstance(valores, str):
            return coluna.astype(str).str.contains(valores, case=False, na=False, regex=False)
        else:
            return pd.Series([False] * len(df), index=df.index)

    # 3. Criação das Máscaras Booleanas
    is_uti = criar_mascara(df[col_criterio_local], val_uti)
    is_enf = criar_mascara(df[col_criterio_local], val_enfermaria)
    is_amb = criar_mascara(df[col_criterio_local], val_amb)
    
    tem_cirurgia = df[col_data_referencia].notna()
    tem_doc = df[col_data_documento].notna()

    # 4. Condições (Prioridade Sequencial)
    condicoes = [
        # 1. Sem data de cirurgia -> Pré
        (tem_doc & ~tem_cirurgia),

        # 2. Pré-operatório Real (Data Doc < Data Cirurgia) -> Pré
        (tem_cirurgia & (df[col_data_documento] < df[col_data_referencia])),

        # 3. Pós-operatório UTI
        (tem_cirurgia & (df[col_data_documento] >= df[col_data_referencia]) & is_uti),

        # 4. Pós-operatório Enfermaria
        (tem_cirurgia & (df[col_data_documento] >= df[col_data_referencia]) & is_enf),

        # 5. Pós-operatório Ambulatório
        (tem_cirurgia & (df[col_data_documento] >= df[col_data_referencia]) & is_amb)
    ]

    # 5. Escolhas correspondentes
    escolhas = [
        evento_pre,        
        evento_pre,        
        evento_uti,        
        evento_enfermaria, 
        evento_amb         
    ]

    # Aplica a seleção
    # CORREÇÃO: Default agora é o evento_pre (cobre casos indefinidos)
    df['redcap_event_name'] = np.select(condicoes, escolhas, default=evento_pre)

    # --- Tratamento da Instância ---
    df[col_instancia] = df[col_instancia].fillna(1).astype(int)

    return df


### ----------------------------------------------------- 
### ----------------------------------------------------- 
def classificar_eventos_redcap(df_entrada, evento_antes, evento_depois, 
                               col_data_referencia='data_cirurgia', 
                               col_data_documento='DH_DOCUMENTO'):
    """
    Classifica o evento do REDCap baseando-se na comparação entre duas datas.

    Parâmetros:
    -----------
    df_entrada : pd.DataFrame
        DataFrame contendo as colunas de datas.
    evento_antes : str
        Nome do evento se (Data Documento < Data Referência) ou se Data Referência for nula.
    evento_depois : str
        Nome do evento se (Data Documento >= Data Referência).
    col_data_referencia : str (Padrão: 'data_cirurgia')
        Nome da coluna que serve de marco divisor (ex: cirurgia, alta).
    col_data_documento : str (Padrão: 'DH_DOCUMENTO')
        Nome da coluna com a data do documento/atendimento a ser classificado.
    """
    
    # Cria cópia para segurança
    df_final = df_entrada.copy()
    
    # 1. Validação básica: Verifica se as colunas existem
    if col_data_documento not in df_final.columns:
        raise ValueError(f"A coluna '{col_data_documento}' não foi encontrada no DataFrame.")
    if col_data_referencia not in df_final.columns:
        # Nota: Se a coluna de referência não existir, o código quebraria. 
        # Aqui assumimos que ela deve existir, mesmo que tenha valores nulos.
        raise ValueError(f"A coluna de referência '{col_data_referencia}' não foi encontrada.")

    # 2. Conversão de Datas (Garante datetime)
    df_final[col_data_documento] = pd.to_datetime(df_final[col_data_documento], errors='raise')
    df_final[col_data_referencia] = pd.to_datetime(df_final[col_data_referencia], errors='raise')

    # 3. Lógica de Classificação
    def definir_evento(row):
        # Sem data do documento -> Impossível classificar
        if pd.isnull(row[col_data_documento]):
            return np.nan 

        # Sem data de referência -> Assume evento ANTERIOR (Regra de negócio padrão)
        if pd.isnull(row[col_data_referencia]):
            return evento_antes
        
        # --- COMPARAÇÃO ---
        
        # Caso A: Documento aconteceu ANTES do marco de referência
        if row[col_data_documento] < row[col_data_referencia]:
            return evento_antes
        
        # Caso B: Documento aconteceu DEPOIS ou no MESMO MOMENTO
        else:
            return evento_depois

    # 4. Aplicação
    df_final['redcap_event_name'] = df_final.apply(definir_evento, axis=1)

    return df_final


### ----------------------------------------------------- 
### ----------------------------------------------------- 
def limpar_dados_para_importacao(df_input, cols_decimais=None):
    """
    LIMPA e NORMALIZA DataFrame para importação no REDCap.
    
    REGRAS:
    - Tudo sai como STRING
    - Decimais apenas nas colunas explicitadas
    - Inteiros permanecem inteiros (sem .0)
    - 'altura' é tratada como cm (remove ponto e vírgula)
    - Remove caracteres ilegais
    - Padroniza nulos
    """
    if cols_decimais is None:
        cols_decimais = []

    df = df_input.copy()

    # --------------------------------------------------
    # 1️⃣ Padronização inicial de nulos
    # --------------------------------------------------
    df = df.replace(
        ['None', 'nan', 'NaN', 'NaT', '<NA>', ' ', ''],
        np.nan
    )

    # Regex de caracteres ilegais para REDCap
    illegal_chars_re = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")

    # --------------------------------------------------
    # 2️⃣ Tratamento especial: ALTURA (cm inteiro)
    # --------------------------------------------------
    if 'altura' in df.columns:
        df['altura'] = (
            df['altura']
            .astype(str)
            .str.strip()
            .str.replace(',', '.', regex=False)
            .str.replace('[^0-9.]', '', regex=True)
            .replace('', np.nan)
            .astype(float)
            .mul(100)                # converte para cm
            .round(0)
            .astype('Int64')
        )

    # --------------------------------------------------
    # 3️⃣ Tratamento de colunas DECIMAIS conhecidas
    # --------------------------------------------------
    for col in cols_decimais:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(',', '.', regex=False)
                .replace('', np.nan)
                .pipe(pd.to_numeric, errors='coerce')
                .round(2)
            )

    # --------------------------------------------------
    # 4️⃣ Demais colunas: numérico simples (inteiros)
    # --------------------------------------------------
    for col in df.columns:
        if col in cols_decimais or col == 'altura':
            continue

        df[col] = pd.to_numeric(df[col], errors='ignore')

    # --------------------------------------------------
    # 5️⃣ Limpeza textual final + conversão para string
    # --------------------------------------------------
    for col in df.columns:
        df[col] = df[col].astype(str)

        df[col] = (
            df[col]
            .str.replace(';', '-', regex=False)
            .str.replace(illegal_chars_re, '', regex=True)
            .replace(['nan', 'None', '<NA>'], '')
        )

        # Remove .0 residuais (segurança extra)
        if col not in cols_decimais:
            df[col] = df[col].str.replace(r'\.0$', '', regex=True)

    return df


### ----------------------------------------------------- 
### ----------------------------------------------------- 
import pandas as pd

def filtrar_registros_redcap(df_local, df_api, nome_col_status):
    """
    Função universal de filtragem e blindagem.
    Adapta-se automaticamente às colunas disponíveis e resolve conflitos de nomes (sufixos).
    """
    if df_local.empty:
        return df_local

    df_local = df_local.copy()
    df_api = df_api.copy()

    # 1. Identificar colunas disponíveis
    colunas_chave_ideais = ['record_id', 'cd_atendimento', 'redcap_event_name', 'redcap_repeat_instance']
    chaves_reais = [col for col in colunas_chave_ideais if col in df_local.columns and col in df_api.columns]

    if nome_col_status not in df_api.columns:
        df_api[nome_col_status] = '1'

    # 2. Padronização
    for d in [df_local, df_api]:
        for col in chaves_reais:
            if col == 'redcap_repeat_instance':
                d[col] = d[col].fillna('1').astype(str)
            else:
                d[col] = d[col].astype(str)

    # 3. Gerar chaves
    def gerar_chave(df):
        return list(zip(*[df[col] for col in chaves_reais]))

    chaves_no_redcap = set(gerar_chave(df_api))
    df_local['_chave'] = gerar_chave(df_local)

    # 4. Novos
    df_novos = df_local[~df_local['_chave'].isin(chaves_no_redcap)].copy()

    # 5. Existentes
    df_existentes = df_local[df_local['_chave'].isin(chaves_no_redcap)].copy()

    # 6. BLINDAGEM (Ajustada para evitar erro de sufixo no merge)
    if not df_existentes.empty:
        # Adicionamos sufixo para evitar o _x e _y caso a coluna já exista no df_local
        df_check = pd.merge(
            df_existentes, 
            df_api[chaves_reais + [nome_col_status]], 
            on=chaves_reais, 
            how='left',
            suffixes=('', '_REDCAP') # O dado local mantém o nome, o da API ganha sufixo
        )
        
        # Descobre qual nome o pandas deu para o status da API no merge
        status_col_api = nome_col_status + '_REDCAP' if f"{nome_col_status}_REDCAP" in df_check.columns else nome_col_status
        
        # Limpa o status
        status = df_check[status_col_api].astype(str).str.replace('.0', '', regex=False)
        
        # Só deixa passar se status for '1'
        mask_permitidos = (status == '1')
        df_existentes_liberados = df_check[mask_permitidos].copy()
        
        # Remove a coluna de status auxiliar
        if status_col_api in df_existentes_liberados.columns:
            del df_existentes_liberados[status_col_api]
    else:
        df_existentes_liberados = pd.DataFrame()

    # 7. Consolidação
    df_final = pd.concat([df_novos, df_existentes_liberados], ignore_index=True)
    if '_chave' in df_final.columns: del df_final['_chave']

    print(f"\n✅ FILTRAGEM CONCLUÍDA ({nome_col_status})")
    print(f"   - Chaves utilizadas: {chaves_reais}")
    print(f"   - Total novos: {len(df_novos)}")
    # --- Substitua as linhas 1286 até 1289 por isto: ---
    print(f"   - Atualizações (Status 1): {len(df_existentes_liberados)}")

    # Resgate seguro do record_id para o print
    if not df_existentes_liberados.empty and 'record_id' in df_existentes_liberados.columns:
        print(f"   - Registros: {df_existentes_liberados['record_id'].tolist()}")
    else:
        print(f"   - Registros: []")

    print(f"   - Ignorados (Bloqueio): {len(df_existentes) - len(df_existentes_liberados)}")
        
    

    return df_final


#  def filtrar_registros_concluidos(df_novos_dados, df_status_redcap, col_status_form):
#     """
#     Remove do DataFrame de novos dados os registros que já estão com status:
#     - '2' (Completo)
#     - '0' (Incompleto - Já iniciado manualmente)
    
#     Só permite atualização se o status for '1' (Unverified) ou se o registro for novo (NaN).
#     """
    
#     # 1. Preparação
#     df_novos = df_novos_dados.copy()
#     df_status = df_status_redcap.copy()
    
#     # Garante tipagem de string para o merge
#     df_novos['record_id'] = df_novos['record_id'].astype(str)
#     df_status['record_id'] = df_status['record_id'].astype(str)
    
#     # Define colunas de junção (record_id + evento se houver)
#     chaves_merge = ['record_id']
#     if 'redcap_event_name' in df_novos.columns and 'redcap_event_name' in df_status.columns:
#         chaves_merge.append('redcap_event_name')
        
#     # 2. Cruzamento (Merge Left)
#     df_merge = pd.merge(
#         df_novos,
#         df_status[chaves_merge + [col_status_form]],
#         on=chaves_merge,
#         how='left'
#     )
    
#     # 3. Lógica de Blindagem Ajustada
#     # Normaliza a coluna de status para string limpa ('2.0' vira '2')
#     status_normalizado = df_merge[col_status_form].astype(str).str.replace('.0', '', regex=False)
    
#     # DEFINIÇÃO DO BLOQUEIO:
#     # Bloqueia se for '2' (Completo) OU '0' (Incompleto)
#     # Apenas '1' (Unverified) ou 'nan' (Novo) passarão.
#     valores_bloqueados = ['0', '2']
    
#     condicao_blindagem = status_normalizado.isin(valores_bloqueados)
    
#     # Aplica o filtro (Inverte a condição com ~ para pegar os NÃO bloqueados)
#     df_filtrado = df_merge[~condicao_blindagem].copy()
    
#     # Limpeza final (remove coluna auxiliar de status)
#     if col_status_form in df_filtrado.columns:
#         del df_filtrado[col_status_form]
        
#     # --- Relatório ---
#     total_entrada = len(df_novos)
#     total_saida = len(df_filtrado)
#     bloqueados = total_entrada - total_saida
    
#     print(f"--- RELATÓRIO DE BLINDAGEM ({col_status_form}) ---")
#     print(f"Entrada: {total_entrada}")
#     print(f"Bloqueados (Status 0 ou 2): {bloqueados}")
#     print(f"Liberados (Novos ou Status 1): {total_saida}")
    
#     return df_filtrado

# def filtrar_novos_registros(df_novo, chaves_proibidas):
#     """
#     Filtra registros que já existem no REDCap para evitar duplicidade.
#     Assume que chaves_proibidas é um set de tuplas (record_id, event, instance).
#     """
#     if df_novo.empty:
#         return df_novo
    
#     df_novo = df_novo.copy()
    
#     # Padronização para comparação (String é mais seguro para a API)
#     df_novo['record_id'] = df_novo['record_id'].astype(str)
#     df_novo['redcap_repeat_instance'] = df_novo['redcap_repeat_instance'].astype(str)
    
#     # Criamos a máscara de filtragem
#     # Nota: O zip deve bater com a estrutura do seu 'set' de registros_existentes
#     filtro = []
#     ignorado_count = 0
    
#     for rid, evt, inst in zip(df_novo['record_id'], 
#                                df_novo['redcap_event_name'], 
#                                df_novo['redcap_repeat_instance']):
        
#         # A chave de busca deve ter a mesma 'forma' da chave_proibida
#         chave_busca = (rid, evt, inst)
        
#         if chave_busca not in chaves_proibidas:
#             filtro.append(True)
#         else:
#             filtro.append(False)
#             ignorado_count += 1
    
#     df_filtrado = df_novo[filtro].copy()
    
#     print(f"📊 Filtro aplicado:")
#     print(f"   - Registros originais no DataFrame: {len(df_novo)}")
#     print(f"   - Registros ignorados (já existem no REDCap): {ignorado_count}")
#     print(f"   - Novos registros para importar: {len(df_filtrado)}")
    
#     return df_filtrado


### ----------------------------------------------------- 
### ----------------------------------------------------- 
def unificar_colunas_sim_nao(df, col_destino, col_nao, col_sim, valor_positivo='SIM'):
    """
    Consolida duas colunas (Sim/Nao) em uma única (0/1).
    
    Lógica:
    - Se col_sim == 'SIM' -> 1
    - Se col_nao == 'SIM' -> 0
    - Se ambas vazias -> NaN
    - Se ambas 'SIM' (erro de cadastro) -> Prioriza 1 (ou você pode alterar a ordem)
    """
    
    # Condições (A ordem importa: a primeira que for True vence)
    condicoes = [
        df[col_sim] == valor_positivo,  # Prioridade 1: Se for SIM, vale 1
        df[col_nao] == valor_positivo   # Prioridade 2: Se for NÃO, vale 0
    ]
    
    valores = [1, 0]
    
    # Aplica a lógica
    df[col_destino] = np.select(condicoes, valores, default=np.nan)
    
    return df


### ----------------------------------------------------- 
### ----------------------------------------------------- 
# Função auxiliar para concatenar textos ignorando nulos e vazios
def concatenar_campos(row, lista_campos, separador=" -- "):
    """
    Concatena valores de múltiplas colunas em uma única string para a linha atual.
    """
    partes = []
    for prefixo, coluna in lista_campos:
        if coluna in row.index:
            valor = str(row[coluna]).strip()
            # Ignora nulos do pandas e strings vazias
            if valor and valor.lower() not in ["nan", "none", "", "<na>"]:
                partes.append(f"{prefixo}{valor}")
    
    return separador.join(partes)


def agregar_por_chaves(df, chaves_agrupamento, col_origem, col_destino, separador=" // "):
    """
    Agrupa os dados pelas chaves informadas e concatena os textos únicos.
    
    Parâmetros:
    - chaves_agrupamento: Lista de colunas (Ex: ['cd_paciente', 'cd_atendimento'] ou adicionando 'redcap_event_name')
    - col_origem: A coluna temporária que tem o texto da linha.
    - col_destino: O nome da coluna final que vai para o REDCap.
    """
    # 1. Filtra apenas quem tem texto válido na coluna de origem
    mask = df[col_origem].astype(str).str.strip().replace(['nan', 'None', ''], pd.NA).notna()
    df_valido = df[mask].copy()
    
    if df_valido.empty:
        df[col_destino] = ""
        return df
        
    # 2. Agrupa apenas pelas chaves que você escolheu na hora
    resumo = df_valido.groupby(chaves_agrupamento)[col_origem].apply(
        lambda x: separador.join(sorted(set(x)))
    ).reset_index(name=col_destino)
    
    # 3. Junta o resultado de volta ao DataFrame original
    df_final = pd.merge(df, resumo, on=chaves_agrupamento, how='left')
    
    # Preenche com vazio onde não houve match
    df_final[col_destino] = df_final[col_destino].fillna("")
    
    return df_final


# def adicionar_textos_concatenados_por_fase(
#     df, 
#     cols_agrupamento, # AGORA É UMA LISTA: ['cod_atendimento', 'redcap_event_name']
#     mapa_regras, 
#     separador=' // '
# ):
#     """
#     1. Gera os textos concatenados agrupando por Atendimento E Fase.
#     2. Devolve o df original com as novas colunas, respeitando o evento.
#     """
    
#     # Lista para guardar os resumos gerados
#     lista_series = []
    
#     for col_final, (col_bool, col_texto) in mapa_regras.items():
        
#         # 1. Filtra apenas onde a booleana é verdadeira
#         # Isso garante que só vamos pegar o texto se ele realmente existiu naquela fase
#         mask = (df[col_bool] == 1) | (df[col_bool] == '1')
#         df_temp = df[mask].copy()
        
#         # 2. Limpa texto
#         df_temp[col_texto] = df_temp[col_texto].astype(str).fillna('')
#         df_temp = df_temp[df_temp[col_texto].str.strip() != '']
        
#         # 3. Agrupa por ID E FASE (Isola o contexto)
#         # Ex: Paciente 100 na Fase 4 -> "Dobuta"
#         # Ex: Paciente 100 na Fase 5 -> "Dipirona" (sem misturar com Dobuta)
#         serie_agrupada = df_temp.groupby(cols_agrupamento)[col_texto].apply(
#             lambda x: separador.join(sorted(set(x)))
#         )
#         serie_agrupada.name = col_final
#         lista_series.append(serie_agrupada)
    
#     # 4. Se não houver regras, devolve original
#     if not lista_series:
#         return df 
        
#     # 5. Cria o DataFrame de resumo (Multi-Index: Atendimento + Evento)
#     df_resumo = pd.concat(lista_series, axis=1).reset_index()
    
#     # 6. Merge de volta usando AS DUAS CHAVES
#     # O texto da Fase 4 só será colado nas linhas da Fase 4
#     df_final = pd.merge(
#         df, 
#         df_resumo, 
#         on=cols_agrupamento, 
#         how='left'
#     )
    
#     return df_final


### ----------------------------------------------------- 
### ----------------------------------------------------- 
# Função para limpar: converte 1.0 -> '1', 0.0 -> '0', e mantém vazios como vazios
def limpar_formato_redcap(val):
    if pd.isna(val) or str(val).strip() == '':
        return ''
    try:
        # Converte para float primeiro (para pegar '1.0'), depois int, depois string
        return str(int(float(val)))
    except:
        return str(val)


### ----------------------------------------------------- 
### ----------------------------------------------------- 
def extrair_instrumento(df, lista_cols, nome_instrumento):
    """
    Extrai colunas específicas para instrumento REDCap com formatação padrão.
    """
    # Filtra apenas colunas que existem
    cols_existentes = [c for c in lista_cols if c in df.columns]
    
    # Sub-df com colunas base + específicas
    sub_df = df[cols_base + cols_existentes].copy()
    
    # Colunas obrigatórias REDCap
    sub_df['redcap_repeat_instrument'] = nome_instrumento
    sub_df[f'{nome_instrumento}_complete'] = 0  # 2=Complete
    
    # Ordem REDCap padrão
    cols_finais = (['record_id', 'redcap_event_name', 'redcap_repeat_instrument', 'redcap_repeat_instance'] + 
                   cols_existentes + [f'{nome_instrumento}_complete'])
    return sub_df[cols_finais]


### ----------------------------------------------------- 
### ----------------------------------------------------- 
def mesclar_preservando_redcap(lista_datasets, chaves):
    """
    lista_datasets: Lista de tuplas [(df_redcap_1, df_sql_1), (df_redcap_2, df_sql_2), ...]
    chaves: Lista de colunas de identificação
    """

    resultados = []

    for df_redcap, df_sql in lista_datasets:
        # 1. Cópias seguras
        df_r = df_redcap.copy()
        df_s = df_sql.copy()

        # 2. Garantir tipos iguais nas chaves
        for c in chaves:
            if c in df_r.columns:
                df_r[c] = df_r[c].astype(str)
            if c in df_s.columns:
                df_s[c] = df_s[c].astype(str)

        # 3. Definir chaves válidas
        chaves_existentes = [
            c for c in chaves 
            if c in df_r.columns and c in df_s.columns
        ]

        df_base = df_r.set_index(chaves_existentes)
        df_novos = df_s.set_index(chaves_existentes)

        # 4. Garantir que ambos tenham mesmas colunas
        for col in df_novos.columns:
            if col not in df_base.columns:
                df_base[col] = pd.NA

        for col in df_base.columns:
            if col not in df_novos.columns:
                df_novos[col] = pd.NA

        # 5. Atualização segura: valor novo sobrescreve se não for NaN nem vazio
        for col in df_novos.columns:
            mask = (
                df_novos[col].notna() &
                (df_novos[col].astype(str).str.strip() != "")
            )
            df_base.loc[mask, col] = df_novos.loc[mask, col]

        df_final = df_base.reset_index()

        resultados.append(df_final)

    return resultados


### ----------------------------------------------------- 
### ----------------------------------------------------- 
def aplicar_cheque_seguranca_prioritaria(df_redcap, df_sql, cols_exames):
    rc = df_redcap.copy()
    sql = df_sql.copy()
    
    # 1. TRATAMENTO DE CHAVES E DATAS ANTES DO MERGE
    keys = ['record_id', 'redcap_event_name', 'redcap_repeat_instance']
    
    for df in [rc, sql]:
        # Normaliza chaves como string limpa
        for k in keys:
            if k in df.columns:
                df[k] = df[k].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        
        # FORÇA dt_lab PARA DATETIME (Garante que ambos falem a mesma língua)
        if 'dt_lab' in df.columns:
            df['dt_lab'] = pd.to_datetime(df['dt_lab'], errors='coerce')

    # 2. OUTER MERGE
    merged = rc.merge(sql[keys + cols_exames], on=keys, how='outer', suffixes=('', '_sql'))

    # 3. LOGICA DE COMPARAÇÃO COLUNA POR COLUNA
    for col in cols_exames:
        col_sql = f"{col}_sql"
        if col_sql not in merged.columns:
            continue
            
        # TRATAMENTO ESPECIAL PARA A COLUNA DE DATA
        if col == 'dt_lab':
            # Se o REDCap está vazio, usa a data do SQL
            merged['dt_lab'] = merged['dt_lab'].fillna(merged['dt_lab_sql'])
            continue

        # TRATAMENTO PARA CAMPOS NUMÉRICOS/BINÁRIOS (hb_pre, creatinina, etc.)
        v_rc = pd.to_numeric(merged[col], errors='coerce')
        v_sql = pd.to_numeric(merged[col_sql], errors='coerce')

        # Regra: Upgrade (0->1) e Preenchimento de vazios
        temp_res = np.fmax(v_rc.fillna(-1), v_sql.fillna(-1))
        merged[col] = temp_res.replace(-1, np.nan)
        merged[col] = merged[col].fillna(v_sql)

    # 4. FORMATAÇÃO FINAL PARA O REDCAP
    # Converte dt_lab de volta para date (sem H:M:S) para o importador
    if 'dt_lab' in merged.columns:
        merged['dt_lab'] = pd.to_datetime(merged['dt_lab']).dt.date

    # Retorna apenas colunas do REDCap
    return merged[df_redcap.columns].copy()


### ----------------------------------------------------- 
### ----------------------------------------------------- 
# Leitura do CSV
# Nota: O REDCap costuma exportar com codificação 'utf-8', 
# mas se der erro de caracteres, tente 'latin1'
def carregar_e_limpar_redcap(caminho_csv, separador='|'):
    # 1. Carregar a base (lendo record_id como string para garantir)
    try:
        # Tentamos ler normalmente primeiro
        df = pd.read_csv(caminho_csv, sep=separador, low_memory=False, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(caminho_csv, sep=separador, low_memory=False, encoding='latin1')

    print(f"Base carregada: {df.shape[1]} colunas detectadas.")

    # 2. Percorrer todas as colunas automaticamente
    for col in df.columns:
        # Se for uma coluna de números decimais (onde aparece o .0)
        if df[col].dtype == 'float64':
            # Removemos os nulos temporariamente para checar se o que sobrou é inteiro
            valores_nao_nulos = df[col].dropna()
            
            # Se a coluna não estiver totalmente vazia e todos os valores forem "redondos"
            if not valores_nao_nulos.empty and valores_nao_nulos.apply(lambda x: float(x).is_integer()).all():
                # Converte para Int64 (Inteiro do Pandas que aceita NaN e não põe .0)
                df[col] = df[col].astype('Int64')
        
        # Garante que o record_id e campos de ID fiquem sem .0 e como texto
        if 'id' in col.lower() or 'record' in col.lower():
            # Remove .0 se o pandas tiver interpretado como float e transforma em string
            df[col] = df[col].astype(str).str.replace('\.0$', '', regex=True)

    return df


### ----------------------------------------------------- 
### ----------------------------------------------------- 
import xml.etree.ElementTree as ET

def xml_to_dataframe(caminho_xml):
    # Namespaces padrão do REDCap
    ns = {'odm': 'http://www.cdisc.org/ns/odm/v1.3'}
    
    tree = ET.parse(caminho_xml)
    root = tree.getroot()
    
    lista_registros = []
    
    # Percorre cada paciente (SubjectData)
    for subject in root.findall('.//odm:SubjectData', ns):
        row = {'record_id': subject.get('StudySubjectID')}
        
        # Percorre todos os campos (ItemData) de cada paciente
        for item in subject.findall('.//odm:ItemData', ns):
            var_nome = item.get('ItemOID')
            valor = item.get('Value')
            row[var_nome] = valor
            
        lista_registros.append(row)
    
    return pd.DataFrame(lista_registros)


### ----------------------------------------------------- 
### ----------------------------------------------------- 






### ----------------- FORMAS DE USO ---------------

## FORMAS DE USAR A FUNÇÃO
# # 1. SEMPRE vincule primeiro (mesma função anterior)
#df_vinculado = vincular_referencia_temporal(df_fisio, df_cirurgia, 
#                                           col_atend_principal='CD_ATENDIMENTO')

# # 2. AGORA use a UNIFICADA para QUALQUER tipo
# # Fisio (comportamento original):
# df_fisio_final = classificar_eventos_flexivel(
#     df_vinculado,
#     tipo_fluxo='cirurgia_fisio',  # ← Seu caso original
#     col_data_marco1='data_cirurgia'
# )

# # Geronto (novo fluxo completo):
# df_geronto_final = classificar_eventos_flexivel(
#     df_geronto_vinc,
#     tipo_fluxo='geronto_odonto_psico',
#     col_data_marco1='data_internacao',
#     col_data_marco2='data_alta'
# )

# # 1. FISIO/CIRURGIA (seu caso original)
# df_fisio_class = classificar_eventos_flexivel(
#     df_fisio_vinculada,
#     tipo_fluxo='cirurgia_fisio',
#     col_data_marco1='data_cirurgia',
#     col_atend='CD_ATENDIMENTO'
# )

# # 2. GERONTO (pré-intern → intern → pós-alta → amb)
# df_geronto_class = classificar_eventos_flexivel(
#     df_geronto_vinc,
#     tipo_fluxo='geronto_odonto_psico',
#     col_data_marco1='data_internacao',  # Ou data_admissao
#     col_data_marco2='data_alta',
#     col_atend='CD_ATENDIMENTO',
#     evento_pre='geronto_pre_arm_1',
#     evento_internacao='geronto_intern_arm_1',
#     evento_pos_intern='geronto_posalta_arm_1',
#     evento_amb='geronto_amb_arm_1'
# )

# # 3. ODONTO/PSICO (custom, só ambulatorial pós-alta)
# df_odonto_class = classificar_eventos_flexivel(
#     df_odonto,
#     tipo_fluxo='custom',
#     col_data_marco1='data_alta',
#     evento_pre='odonto_pre',
#     evento_pos_intern='odonto_posalta_amb'
# )

### ----------------- BASE SINTETICA ---------------

# import random
# from datetime import datetime, timedelta

# def gerar_base_tcc_redcap(caminho_dicionario, n_registros=150):
#     # 1. Carregar o dicionário (CSV com separador ;)
#     try:
#         df_dic = pd.read_csv(caminho_dicionario, sep=';')
#     except Exception as e:
#         print(f"Erro ao ler o dicionário: {e}")
#         return None

#     def parse_choices(choice_str):
#         """Extrai os códigos numéricos de strings como '0, Nao | 1, Sim'"""
#         if pd.isna(choice_str) or not str(choice_str).strip() or '[' in str(choice_str):
#             return None
        
#         # Divide por '|' e pega o valor antes da vírgula
#         parts = str(choice_str).split('|')
#         keys = []
#         for p in parts:
#             if ',' in p:
#                 key = p.split(',')[0].strip()
#                 keys.append(key)
#         return keys if keys else None

#     data = []
    
#     # 2. Geração de registros
#     for i in range(1, n_registros + 1):
#         registro = {}
        
#         for _, row in df_dic.iterrows():
#             var_name = row['Variable / Field Name']
#             field_type = row['Field Type']
#             choices_raw = row['Choices, Calculations, OR Slider Labels']
#             validation = row['Text Validation Type OR Show Slider Number']
            
#             # Identificador único
#             if var_name == 'record_id':
#                 registro[var_name] = i
#                 continue
            
#             # Lógica baseada nas Escolhas (Choices)
#             choices = parse_choices(choices_raw)
#             if choices:
#                 registro[var_name] = random.choice(choices)
            
#             # Lógica baseada no Tipo de Campo e Validação
#             elif field_type == 'yesno':
#                 registro[var_name] = random.choice(['0', '1'])
            
#             elif field_type == 'text':
#                 if validation == 'date_dmy':
#                     # Gera datas aleatórias entre 1950 e 2024
#                     inicio = datetime(1950, 1, 1)
#                     fim = datetime(2024, 1, 1)
#                     dt = inicio + timedelta(days=random.randint(0, (fim - inicio).days))
#                     registro[var_name] = dt.strftime('%d/%m/%Y')
#                 elif validation in ['integer', 'number']:
#                     # Tenta pegar min/max do dicionário ou usa padrão
#                     try:
#                         vmin = int(row['Text Validation Min']) if not pd.isna(row['Text Validation Min']) else 0
#                         vmax = int(row['Text Validation Max']) if not pd.isna(row['Text Validation Max']) else 100
#                     except: vmin, vmax = 0, 100
#                     registro[var_name] = random.randint(vmin, vmax)
#                 else:
#                     registro[var_name] = f"Dado_{i}"
            
#             elif field_type == 'calc':
#                 registro[var_name] = round(random.uniform(10, 50), 1) # Ex: IMC
            
#             else:
#                 registro[var_name] = np.nan
        
#         data.append(registro)

#     df_result = pd.DataFrame(data)

#     # 3. Simulação de Missing Values (15% de falha em colunas que não são ID)
#     for col in df_result.columns:
#         if col != 'record_id':
#             df_result.loc[df_result.sample(frac=0.15).index, col] = np.nan

#     return df_result

# # Execução
# caminho_dic = r'C://Users/priscilla.sequetin/Downloads/RegValvDANTE_DataDictionary_2026-02-04.csv'
# df_teste = gerar_base_tcc_redcap(caminho_dic, 200)

# # Salvar e conferir
# if df_teste is not None:
    # df_teste.to_csv(r'C://Users/priscilla.sequetin/Downloads/base_teste_sintetica.csv', index=False)
    # print(f"Base gerada com {df_teste.shape[0]} registros e {df_teste.shape[1]} colunas.")
    # print(df_teste[['record_id', 'genero', 'etnia', 'classe_nyha']].head())




# from pathlib import Path

# # =========================
# # 1) Leitura dos .parquet
# # =========================
# pasta_perfil = Path(r'C://Users/priscilla.sequetin/Documents/BasesDashs/perfil_pq')

# dfs_perfil = []
# for caminho in pasta_perfil.glob('atend_*.parquet'):
#     try:
#         df_tmp = pd.read_parquet(caminho)
#         dfs_perfil.append(df_tmp)
#         print(f"✅ Lido com sucesso: {caminho.name}")
#     except Exception as e:
#         print(f"❌ Erro ao ler {caminho.name}: {e}")

# if dfs_perfil:
#     df_perfil = pd.concat(dfs_perfil, ignore_index=True, sort=False)
#     print(f"\n🚀 Total de registros concatenados: {len(df_perfil)}")
#     print(f"Colunas encontradas: {df_perfil.columns.tolist()}")
# else:
#     raise RuntimeError("⚠️ Nenhum arquivo .parquet encontrado no padrão 'atend_*.parquet'.")

# # =========================
# # 2) Seleção de colunas (somente as existentes)
# # =========================
# cols_desejadas = [
#     'ANO', 'CD_ATENDIMENTO', 'CD_PACIENTE', 'DH_DOCUMENTO', 'TABAGISMO', 'AVC', 'HAS', 
#     'DM', 'DLP', 'DRC', 'DPOC', 'DAOP', 'DAC', 'DAC TIPOS', 'ICC', 'ICC TIPOS', 
#     'ARRITMIA', 'ARRITMIA TIPOS', 'ANGINA', 'ANGINA TIPOS', 'EDEMA', 
#     'DOR TORACICA', 'DOR TORACICA TIPOS', 'SINCOPE', 'DISPNEIA', 'DISPNEIA TIPOS', 
#     'PALPITACAO', 'VALVOPATIA', 'VALVOPATIA TIPOS', 'VALVOPATIA PROTESE', 
#     'TAVI PREVIA', 'IAO', 'EAO', 'IMI', 'EMI', 'ITRI', 'ETRI', 'TONTURA', 
#     'CLASSIFICACAO PS'
# ]
# cols_ok = [c for c in cols_desejadas if c in df_perfil.columns]
# aCom1 = df_perfil.copy()
# aCom2 = aCom1.loc[:, cols_ok].copy()

# # =========================
# # 3) Filtragem por listas (robusta em string)
# # =========================
# def _norm_id_list(seq):
#     if seq is None:
#         return []
#     return [str(x).replace(r'\.0', '').strip() for x in seq]

# lista_check_cir1 = _norm_id_list(regvalv_pcte.get('record_id') if isinstance(regvalv_pcte, pd.DataFrame) else regvalv_pcte)
# lista_check_cir2 = _norm_id_list(dVal4.get('record_id') if isinstance(dVal4, pd.DataFrame) else dVal4)


# lista_ftrisk = list(set(lista_check_cir1) | set(lista_check_cir2))
# print(f"✅ Total record_id para filtrar: {len(lista_ftrisk)}")

# # Filtra CD_PACIENTE como string normalizada
# aCom2['CD_PACIENTE'] = aCom2['CD_PACIENTE'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
# aCom2 = aCom2[aCom2['CD_PACIENTE'].isin(lista_ftrisk)].copy()

# # =========================
# # 4) Datas, ano e ordenação
# # =========================
# aCom2['DH_DOCUMENTO'] = pd.to_datetime(aCom2['DH_DOCUMENTO'], errors='coerce')
# aCom2['ANO'] = pd.to_numeric(aCom2['ANO'], errors='coerce').astype('Int64')

# # Ordena por paciente e data
# aCom2 = aCom2.sort_values(['CD_PACIENTE', 'DH_DOCUMENTO'], ascending=True, kind='mergesort')

# # Agrupa por PACIENTE e ANO e pega a última linha (mais recente no ano)
# aCom3 = aCom2.groupby(['CD_PACIENTE', 'ANO'], as_index=False).last()

# # =========================
# # 5) Função de transformação p/ REDCap
# # =========================
# def criar_redcap_completo(df_original: pd.DataFrame) -> pd.DataFrame:
#     df_temp = df_original.copy()

#     # record_id
#     if 'CD_PACIENTE' in df_temp.columns:
#         df_temp['CD_PACIENTE'] = df_temp['CD_PACIENTE'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
#         record_id = df_temp['CD_PACIENTE']
#         print(f'✅ record_id preservado: {len(record_id)} registros')
#     else:
#         record_id = pd.Series(range(len(df_temp)), index=df_temp.index)
#         print('⚠️ CD_PACIENTE não encontrada, usando índice')

#     # LIMPEZA de valores string (preserva colunas chaves)
#     colunas_preservadas = ['CD_PACIENTE', 'ANO']
#     for col in df_temp.select_dtypes('object').columns:
#         if col not in colunas_preservadas:
#             df_temp[col] = (
#                 df_temp[col]
#                 .str.strip()
#                 .str.upper()
#                 .str.replace('[-_/]', '', regex=True)
#             )

#     # -------- FATORES DE RISCO --------
#     cols_ftrisk = {}
#     cols_ftrisk['tabagismo'] = df_temp.get('TABAGISMO', pd.Series(index=df_temp.index)).map({
#         'NAO': 0, 'EXTBG': 1, 'SIM': 2
#     })
#     cols_ftrisk['hipertensao'] = df_temp.get('HAS', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})
#     cols_ftrisk['diabetes']    = df_temp.get('DM', pd.Series(index=df_temp.index)).map({'NAO': 0, 'DMID': 1, 'DMNID': 1})
#     cols_ftrisk['tipo_diabetes'] = df_temp.get('DM', pd.Series(index=df_temp.index)).map({'DMID': 1, 'DMNID': 2})
#     cols_ftrisk['diabetes_controle'] = df_temp.get('DM', pd.Series(index=df_temp.index)).map({'DMID': 3, 'DMNID': 2})
#     cols_ftrisk['dislipidemia'] = df_temp.get('DLP', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})
#     cols_ftrisk['doenca_renal_cronica'] = df_temp.get('DRC', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1, 'DIALITICO': 1})
#     cols_ftrisk['drc_clcr'] = df_temp.get('DRC', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 2, 'DIALITICO': 4})
#     cols_ftrisk['dpoc'] = df_temp.get('DPOC', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})
#     cols_ftrisk['avc_ait_previo'] = df_temp.get('AVC', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})
#     cols_ftrisk['angina'] = df_temp.get('ANGINA', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})

#     def map_ccs(valor):
#         if pd.isna(valor):
#             return np.nan
#         valor = str(valor)
#         if 'CCS1' in valor: return 1
#         if 'CCS2' in valor: return 2
#         if 'CCS3' in valor: return 3
#         if 'CCS4' in valor: return 4
#         return np.nan

#     cols_ftrisk['ccs'] = df_temp.get('ANGINA TIPOS', pd.Series(index=df_temp.index)).apply(map_ccs)

#     nyha_map = {'DISPNEIAI': 1, 'DISPNEIAII': 2, 'DISPNEIAIII': 3, 'DISPNEIAIV': 4}
#     cols_ftrisk['carga_sintomas_dispneia'] = df_temp.get('DISPNEIA TIPOS', pd.Series(index=df_temp.index)).map(nyha_map)
#     cols_ftrisk['classe_nyha'] = cols_ftrisk['carga_sintomas_dispneia']

#     cols_ftrisk['carga_sintomas_sincope'] = df_temp.get('SINCOPE', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})
#     cols_ftrisk['palpitacoes'] = df_temp.get('PALPITACAO', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})

#     dor_map = {'NAO': 0, 'TIPOC': 1, 'TIPOD': 1, 'TIPOB': 2, 'TIPOA': 3}
#     cols_ftrisk['carga_sintomas_dor_toracica'] = df_temp.get('DOR TORACICA TIPOS', pd.Series(index=df_temp.index)).map(dor_map)
#     cols_ftrisk['carga_sintomas_edema'] = df_temp.get('EDEMA', pd.Series(index=df_temp.index)).map({'SIM': 2, 'NAO': 0})

#     # -------- PRÉ-OP --------
#     cols_preop = {}
#     cols_preop['arritmias'] = df_temp.get('ARRITMIA', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})
#     # MP / TV baseados nos tipos de arritmia
#     arr_tipos = df_temp.get('ARRITMIA TIPOS', pd.Series(index=df_temp.index))
#     cols_preop['possui_mp_trc_cdi'] = (arr_tipos == 'MP').where(arr_tipos.notna()).astype('Int64')
#     cols_preop['estado_pr_operat_rio_cr_ti'] = (arr_tipos == 'TV').where(arr_tipos.notna()).astype('Int64')

#     cols_preop['valvulopatia_aortica'] = df_temp.get('IAO', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})
#     cols_preop['valvulopatia_mitral']  = df_temp.get('IMI', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})

#     # Radiobox mitrada aórtica
#     vp_tipos = df_temp.get('VALVOPATIA TIPOS', pd.Series(index=df_temp.index))
#     vp_prot  = df_temp.get('VALVOPATIA PROTESE', pd.Series(index=df_temp.index))

#     cols_preop['valvula_nativa___2'] = (
#         ((vp_tipos == 'NATIVA') | (df_temp.get('IAO', pd.Series(index=df_temp.index)) == 'SIM'))
#         .where(vp_tipos.notna() | df_temp.get('IAO', pd.Series(index=df_temp.index)).notna())
#         .astype('Int64')
#     )
#     cols_preop['protese_mecanica___2'] = (
#         ((vp_prot == 'MECANICA') | (df_temp.get('IAO', pd.Series(index=df_temp.index)) == 'SIM'))
#         .where(vp_prot.notna() | df_temp.get('IAO', pd.Series(index=df_temp.index)).notna())
#         .astype('Int64')
#     )
#     cols_preop['protese_biologica___2'] = (
#         ((vp_prot == 'BIOLOGICA') | (df_temp.get('IAO', pd.Series(index=df_temp.index)) == 'SIM'))
#         .where(vp_prot.notna() | df_temp.get('IAO', pd.Series(index=df_temp.index)).notna())
#         .astype('Int64')
#     )

#     # -------- CORONÁRIAS --------
#     cols_coron = {}
#     cols_coron['dac_exame'] = df_temp.get('DAC', pd.Series(index=df_temp.index)).map({'NAO': 0, 'SIM': 1})

#     dac_tipos = df_temp.get('DAC TIPOS', pd.Series(index=df_temp.index))
#     cols_ftrisk['intervencoes_cardiacas_previas'] = dac_tipos.isin(['ICPPREVIA', 'RMPREVIA']).where(dac_tipos.notna()).astype('Int64')
#     cols_ftrisk['tipo_intervencao_previa___1'] = (dac_tipos == 'ICPPREVIA').where(dac_tipos.notna()).astype('Int64')
#     cols_ftrisk['tipo_intervencao_previa___2'] = (dac_tipos == 'RMPREVIA').where(dac_tipos.notna()).astype('Int64')

#     # -------- DF FINAL --------
#     df_redcap = pd.concat(
#         [
#             pd.Series(record_id, name='record_id', index=df_temp.index),
#             df_temp[['ANO']].astype('Int64') if 'ANO' in df_temp.columns else pd.DataFrame(index=df_temp.index),
#             pd.DataFrame(cols_ftrisk, index=df_temp.index),
#             pd.DataFrame(cols_preop, index=df_temp.index),
#             pd.DataFrame(cols_coron, index=df_temp.index),
#         ],
#         axis=1
#     )

#     # Tipos (mantém NaN com Int64)
#     cols_ignorar = ['record_id', 'ANO']
#     for col in df_redcap.columns:
#         if col not in cols_ignorar and pd.api.types.is_numeric_dtype(df_redcap[col]):
#             df_redcap[col] = df_redcap[col].astype('Int64')

#     print('✅ DF REDCap FINAL CRIADO!')
#     print(f'📊 Shape: {df_redcap.shape}')
#     print(f'🎯 record_id únicos: {df_redcap["record_id"].nunique()}')
#     if 'ANO' in df_redcap.columns:
#         print(f'📅 Anos processados: {sorted([a for a in df_redcap["ANO"].dropna().unique().tolist()])}')
#     return df_redcap

# # EXECUTA
# aCom4 = criar_redcap_completo(aCom3)

# # =========================
# # 6) Vincular por record_id + ANO com regvalv_cirurgia
# # =========================
# aCom4['record_id'] = aCom4['record_id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

# regvalv_cirurgia['record_id'] = (
#     regvalv_cirurgia['record_id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
# )
# regvalv_cirurgia['data_cirurgia'] = pd.to_datetime(regvalv_cirurgia['data_cirurgia'], errors='coerce')
# regvalv_cirurgia['ANO'] = regvalv_cirurgia['data_cirurgia'].dt.year.astype('Int64')

# cols_ref = ['record_id', 'ANO', 'redcap_event_name', 'redcap_repeat_instance']
# for c in cols_ref:
#     if c not in regvalv_cirurgia.columns:
#         regvalv_cirurgia[c] = pd.NA

# aCom5 = regvalv_cirurgia[cols_ref].merge(
#     aCom4,
#     on=['record_id', 'ANO'],
#     how='left'
# )

# # Instância (string)
# aCom5['redcap_repeat_instance'] = (
#     aCom5['redcap_repeat_instance']
#         .fillna(1)
#         .apply(lambda x: str(int(float(x))) if pd.notna(x) else '1')
# )


# # ----------------------------------------------
# # TIPO VALVOPATIAS Estenose, Insuficiencia, Dupla Lesao
# # ----------------------------------------------

# def transformar_colunas_valvulopatias(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Cria novas colunas para tipo de valvulopatia (Estenose, Insuficiência, Dupla Lesão)
#     baseado nos checkboxes de gravidade (Moderada/Importante).
#     """
#     df = df.copy()

#     # --------------------------------------------------
#     # 1️⃣ Mapeamento de Sufixos (Conforme dVal2.columns)
#     # --------------------------------------------------
#     # Aórtica = ___1 | Mitral = ___2 | Tricúspide = ___3
#     SUFIXO = {'ao': '1', 'mi': '2', 'tr': '3'}

#     def criar_tipo_valvulopatia(df_in: pd.DataFrame, valvula: str) -> None:
#         idx = df_in.index
#         sfx = SUFIXO[valvula]

#         # --- Lógica para ESTENOSE ---
#         # Verifica se é Moderada OU Importante
#         estenose_cols = [
#             f'estenose_importante___{sfx}',
#             f'estenose_moderada___{sfx}'
#         ]
#         # Filtra apenas as que existem no DF
#         estenose_cols = [c for c in estenose_cols if c in df_in.columns]
        
#         if estenose_cols:
#             # Considera positivo se o valor for 1 ou '1'
#             cond_estenose = df_in[estenose_cols].isin([1, '1']).any(axis=1)
#         else:
#             cond_estenose = pd.Series(False, index=idx)

#         # --- Lógica para INSUFICIÊNCIA ---
#         # Verifica se é Moderada OU Importante
#         insuf_cols = [
#             f'insuficiencia_importante___{sfx}',
#             f'insuficiencia_moderada___{sfx}'
#         ]
#         insuf_cols = [c for c in insuf_cols if c in df_in.columns]
        
#         if insuf_cols:
#             # Considera positivo se o valor for 1 ou '1'
#             cond_insuf = df_in[insuf_cols].isin([1, '1']).any(axis=1)
#         else:
#             cond_insuf = pd.Series(False, index=idx)

#         # --- Aplicação das Regras (1=Estenose, 2=Insuficiência, 3=Dupla Lesão) ---
#         condicoes = [
#             (cond_estenose & cond_insuf), # Dupla Lesão (Prioridade na verificação)
#             (cond_estenose & ~cond_insuf),# Apenas Estenose
#             (cond_insuf & ~cond_estenose) # Apenas Insuficiência
#         ]
#         valores = [3, 1, 2]

#         # np.select gera o resultado. Default é vazio para não marcar quem não tem a doença.
#         resultado = np.select(condicoes, valores, default=np.nan)
        
#         # Salva como object para evitar o ".0"
#         col_name = f'tipo_valvulopatia_{valvula}'
#         df_in[col_name] = pd.Series(resultado, index=idx)
        
#         # Limpeza para manter o formato REDCap (remove .0 e mantém nulo como vazio)
#         df_in[col_name] = df_in[col_name].apply(lambda x: str(int(x)) if pd.notnull(x) else "")
#         df_in[col_name] = df_in[col_name].astype(object)

#     # Executa para as três válvulas
#     for v in ['ao', 'mi', 'tr']:
#         criar_tipo_valvulopatia(df, v)

#     return df
