from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import URL, Engine
import pandas as pd
import numpy as np

### Busca para comorbidades e outros dados para diversas fichas.
def doc_eletronico(
    connection: str,
    docs: list[int] | int,
    ids: list[int] | int | pd.Series | np.ndarray | None = None,
) -> pd.DataFrame:
    """
    Fetch structured and free-text answers from MV/PEP electronic document tables for one or more
    document templates (CD_DOCUMENTO), optionally filtered by patient IDs (CD_PACIENTE), and
    return a curated wide DataFrame with harmonized comorbidity/clinical fields.

    The function queries both:
      - `FT_DOC_ELETRONICO` (coded/structured answers linked to `dim_campo_documento`)
      - `ft_doc_eletronico_texto` (free-text answers linked to `dim_campo_documento`)

    It supports large `IN (...)` filters by automatically splitting `ids` into chunks to respect
    SQL Server's parameter limit (max ~2100 parameters per statement) when using expanding bind
    parameters.

    Args:
        connection (str): 
            ODBC connection string for SQL Server (used with `mssql+pyodbc`), e.g.
            ``"DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;DATABASE=...;UID=...;PWD=...;..."``.
            Must not be empty.

        docs (list[int] | int):
            One or more document template codes (CD_DOCUMENTO) to query. Accepts a single `int`
            or a list of ints. `NaN` values are ignored and values are converted to `int`.
            Duplicates may be removed.

        ids (list[int] | int | None, optional): 
            Optional patient filter (CD_PACIENTE). If provided, only rows matching these patient
            IDs are returned. Accepts a single `int`, a list of ints, or `None` for no filtering.
            `NaN` values are ignored and values are converted to `int`. Duplicates may be removed.
            When the total number of parameters would exceed SQL Server limits, the list is
            automatically split into chunks and queried in multiple passes. 
            - Defaults to None.

    Returns:
        pd.DataFrame: 
            A wide (pivoted) DataFrame keyed by:
            `CD_PACIENTE`, `CD_ATENDIMENTO`, `DH_DOCUMENTO`,
            containing curated columns representing comorbidities and other clinical fields
            extracted from both structured and text document sources. The exact set of columns
            depends on the requested `docs` and on the internal metadata mappings.

    Raises:
        ValueError:
            If `connection` is empty, if `docs` is empty after normalization, or if `docs` is too
            large to fit within the SQL Server parameter limit even after chunking.

        TypeError:
            If `docs` or `ids` are not of the supported types, or if list elements cannot be
            safely converted to integers.

        RuntimeError:
            If the database query fails, or if the function detects metadata codes returned by
            the query that are not covered by the configured mapping dictionaries (guard rail
            to catch unexpected schema/content changes).
    """    

    def query_sql_to_dataframe(
        engine: Engine, 
        query: str, 
        params: dict
    ) -> pd.DataFrame:
        """
        Execute a SQL query and return the result as a DataFrame.
        """
        if "ids" in params:
            stmt = text(query).bindparams(
                bindparam("docs", expanding=True),
                bindparam("ids", expanding=True),
            )
        else:
            stmt = text(query).bindparams(
                bindparam("docs", expanding=True),
            )
        try:
            with engine.connect() as connection:
                return pd.read_sql(stmt, connection, params=params)
        except Exception as ex:
            raise RuntimeError(f"Error querying the database: {ex}") from ex

    if not connection:
        raise ValueError(
            f'''`connection` não pode ser vazio! Passar uma {str} equivalente à:
            connection = (
                "DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={{server}};"
                f"DATABASE={{database}};"
                f"UID={{username}};PWD={{password}};"
                "Encrypt=yes;TrustServerCertificate=yes;"
            )
            '''
        )
    engine = create_engine(URL.create("mssql+pyodbc", query={"odbc_connect": connection}))

    if not docs:
        raise ValueError(
            """`docs` (CD_DOCUMENTO) não pode ser vazio! Exemplos:
                    1070, # Ficha Padrão Ambulatorial
                    1068, # CENSUS (Atendimento Hipertensão)
                    1066, # Internação
                    1061, # PS
                    1043, # Resumo de obito
                    1037  # Resumo de Alta
            """,
        )
    elif isinstance(docs, int):
        docs = [docs]
    elif isinstance(docs, list):
        docs = list(set([int(doc) for doc in docs if pd.notna(doc)]))
    else:
        raise TypeError(f"`docs` (CD_DOCUMENTO) precisa ser {list[int]} ou {int}. Recebido: {type(docs)}.")
    
    if ids is not None:
        if isinstance(ids, int):
            ids = [ids]
        #Passa valores da lista para int, sem duplicar valores e exclue na.
        elif isinstance(ids, (list, pd.Series, np.ndarray)):
            try:
                ids = list(set([int(i) for i in ids if pd.notna(i)]))
            except Exception as e:
                raise TypeError(f"All values in `ids` must be convertible to {int}.\n{e}")
        else:
            raise TypeError(f"`ids` (CD_PACIENTE) precisa ser {list[int]}, {int}, {pd.Series}, {np.ndarray} ou {None}. Recebido: {type(ids)}.")
        
        if len(ids) <= 0:
            raise ValueError(f"`ids` não possui conteúdo: {ids}")

        # No SQL Server uma instrução pode ter no máximo 2100 parâmetros.
        if len(ids) + len(docs) >= 2100:
            chunk_size = 2000 - len(docs) #2000 para margem de segurança
            if chunk_size <= 0:
                raise ValueError("lista de `docs` (fichas) grande demais para caber no limite de parâmetros.")
            ids = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]
        else:
            ids = [ids]

    query_comorbidades = f"""
        SET DATEFORMAT ymd;
        SELECT
            fde.CD_PACIENTE,
            fde.CD_ATENDIMENTO,
            fde.DH_DOCUMENTO,
            dcd.CD_METADADO,
            fde.resposta
        FROM FT_DOC_ELETRONICO fde
        LEFT JOIN dim_campo_documento dcd ON fde.NK_CD_CAMPO = dcd.NK_CD_CAMPO
        WHERE
            fde.CD_DOCUMENTO IN :docs
            {f"AND fde.CD_PACIENTE IN :ids" if ids else ""}
    """

    print("Extraindo dados de FT_DOC_ELETRONICO...", end="\r", flush=True)
    if not ids:
        df_comorbidades = query_sql_to_dataframe(engine, query_comorbidades, params={'docs': docs})
    else:
        chunk_comorbidades = []
        for i, chunk in enumerate(ids, start=1):
            chunk_comorbidades.append(query_sql_to_dataframe(engine, query_comorbidades, params={'docs': docs, 'ids': chunk}))
            print(f"Extraindo dados de FT_DOC_ELETRONICO... Progresso: {i}/{len(ids)} ({i/len(ids):.1%})", end="\r", flush=True)
        df_comorbidades = pd.concat(chunk_comorbidades)

    print(f"Tratando dados de FT_DOC_ELETRONICO... {' '*50}", end="\r", flush=True)
    df_comorbidades.drop_duplicates(subset=['CD_PACIENTE', 'CD_ATENDIMENTO', 'DH_DOCUMENTO', 'CD_METADADO'], ignore_index=True, inplace=True)

    comorb_rb = {} # Radiobutton
    comorb_cb = {} # Checkbox

    if any(doc in docs for doc in [1051,1061,1064,1066,1068,1070,1110,1125,1130]):
        comorb_rb['TABAGISMO'] = [387394, 387396, 439922, 439924, 439920]

    if any(doc in docs for doc in [935,942,943,1061,1064,1066,1068,1070,1110,1130]):
        comorb_rb['AVC'] = [410895, 410897]
        comorb_cb['AVC_CEBEBRO_VASCULAR'] = [391027]
        comorb_cb['AVC_PREVIO'] = [423873]

    if any(doc in docs for doc in [935,942,943,1031,1051,1061,1064,1066,1068,1070,1072,1125,1130]):
        comorb_rb['HAS'] =  [410904, 410906]
        comorb_cb['HAS'] =  [387349] #SIM/NAO

    if any(doc in docs for doc in [1051,935,1110,1125,856,857,858,859,876,877,879,887,888,929,934,941,942,943,954,984,1029,1039,1044,1047,1048,1049,1061,1064,1066,1068,1070,1130,1031,1072]): 
        comorb_rb['DM'] = [464663, 415610, 410908, 410910, 410899, 396925, 396927, 410900, 432674, 432676]
        comorb_cb['DM'] = [387352, 395551]

    if any(doc in docs for doc in [888,1110]):
        comorb_rb['INSULINA'] = [396971, 396973, 460297, 460298] 

    if any(doc in docs for doc in [1061,1064,1066,1068,1070,1125,1130, 942,943,1072]):
        comorb_rb['DLP'] = [445374, 445373]
        comorb_cb['DLP'] = [387353]

    if any(doc in docs for doc in [1061,1064,1066,1068,1070,1110,1130]):
        comorb_rb['DAOP'] = [446429, 446430]

    if any(doc in docs for doc in [951,955,957,958,959,960,968,979,988,990,1061,1064,1066,1068,1070,1130,942,943]):
        comorb_rb['ARRITMIA'] = [414259, 414262]
        comorb_cb['ARRITMIA'] = [391029]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,929,934,941,942,943,954,984,1029,1039,1044,1047,1048,1049,1061,1064,1066,1068,1070,1130]):
        comorb_rb['VALVOPATIA_TIPOS'] = [391799, 391792]

    if any(doc in docs for doc in [1061,1064,1066,1068,1070,1130,942,943,1072]):
        comorb_rb['ETRI'] = [445470, 445468]
        comorb_cb['ETRI'] = [390913] #SIM/NAO
        comorb_rb['ITRI'] = [445466, 445463]
        comorb_cb['ITRI'] = [390912] #SIM/NAO
        comorb_rb['EMI'] = [445461, 445459]
        comorb_cb['EMI'] = [390977] #SIM/NAO
        comorb_rb['IAO'] = [445449, 445447]
        comorb_cb['IAO'] = [390910] #SIM/NAO
        comorb_rb['EAO'] = [445453, 445451]
        comorb_cb['EAO'] = [390911] #SIM/NAO
        comorb_rb['IMI'] = [445457, 445455]
        comorb_cb['IMI'] = [390976] #SIM/NAO
        
        
        comorb_rb['VALVOPATIA_PROTESE'] = [446330, 446332, 445475, 391793, 391794]
        comorb_rb['VALVOPATIA'] = [445416, 445413]
        comorb_cb['VALVOPATIA'] = [387669]
        comorb_cb['PLURIVALVAR'] = [391795]
        comorb_rb['IC'] = [445363, 445361]
        comorb_cb['IC'] = [391028]
        comorb_rb['DAC'] = [445355, 445353]
        comorb_cb['DAC'] = [390178]
        comorb_rb['DRC'] = [445382, 445380, 446326]
        comorb_cb['DRC'] = [390900]
        comorb_rb['ALERGIA'] = [411336, 411337]


    if any(doc in docs for doc in [1061,942,943]):
        comorb_rb['TONTURA'] = [445500, 445498]
        comorb_cb['TONTURA'] = [387461]
        comorb_rb['CLASSIFICACAO_PS'] = [452354, 452356, 452358, 452360, 452362, 452365, 452367, 452369, 444238]
        comorb_rb['ANGINA'] = [445492, 445490]
        comorb_cb['ANGINA'] = [387449]

    if any(doc in docs for doc in [1061,1051,942,943]):
        comorb_rb['PALPITACAO'] = [440035, 440033]
        comorb_cb['PALPITACAO'] = [387463] 
        comorb_rb['DISPNEIA_TIPOS'] = [440021, 440023, 440025, 440027, 387476, 387478, 387480, 387482] 
        comorb_rb['DISPNEIA'] = [440017, 440015] 
        comorb_rb['DOR_TORACICA'] = [440031, 440029] 
        comorb_cb['DOR_TORACICA'] = [408129] 

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,929,934,941,942,943,1029,1039,1044,1047,1048,1049,1061]):
        comorb_rb['DOR_TORACICA_TIPOS'] = [408125, 408126, 408127, 408128] 
        comorb_rb['ANGINA_TIPOS'] = [391032, 391026, 391033, 391034]

    if any(doc in docs for doc in [914,917,922,924,928,933,935,1061,942,943]):
        comorb_rb['EDEMA'] = [402270, 402272] 
        comorb_cb['EDEMA'] = [387447]

    if any(doc in docs for doc in [1061,1051,1015,942,943]):
        comorb_rb['SINCOPE'] = [430175, 430177] 
        comorb_cb['SINCOPE'] = [387473] 

    if any(doc in docs for doc in [1068]):
        comorb_cb['TFGe_NAO_AVALIADO'] = [467824] # RB/DS    SIM/NAO

    if any(doc in docs for doc in [1068,1110]):
        comorb_rb['ALCOOL_SEMANA'] = [460318, 460319, 460320, 460321]
        comorb_cb['HAS_SECUNDARIA_AFASTADA'] = [460245]
        comorb_cb['HAS_SECUNDARIA_PESQUISA_NAO_REALIZADA'] = [460246] 
        comorb_cb['HAS_SECUNDARIA_EM_RASTREAMENTO'] = [460244]
        comorb_rb['HAS_SECUNDARIA_TIPOS'] = [460248, 460253, 460250, 460251, 465266, 465267, 465268] 
        comorb_rb['ESCOLARIDADE'] = [465261, 460268, 460269, 460270, 460271, 460272, 460273]
        comorb_rb['HIPERTROFIA_DE_VE'] = [460346, 460347, 460348]
        comorb_cb['FEVE_MAIS_RECENTE_NAO_AVALIADA'] = [460354] # RB/DS   SIM/NAO
        comorb_cb['IMVE_MAIS_RECENTE_NAO_AVALIADA'] = [460313] # RB/DS   SIM/NAO
        comorb_rb['ALBUMINURIA>30'] = [460356, 460357, 460358]
        comorb_rb['FUNDO_OLHO'] = [460362, 460363, 460364, 460365, 460366]
        #comorb_rb['PA_CONSULTORIO_MEDIDA_1'] = [] # está na ft_doc_eletronico_texto 'DS_CONSULTORIO_MEDIDA_1'
        comorb_rb['PA_CONSULTORIO_MEDIDA_2'] = [460384, 460385]
        comorb_rb['PA_CONSULTORIO_MEDIDA_3'] = [460394, 460395]
        comorb_cb['PA_NENHUMA_FORA_DO_CONSULTORIO'] = [466709]  #SIM/NAO
        comorb_rb['META_PRESSORICA'] = [460432, 460434, 460435, 460436]
        comorb_rb['DENERVACAO_RENAL'] = [460437, 460439]
        comorb_rb['ANGIOPLASTIA_DE_ART_RENAL'] = [460438, 460440]
        comorb_rb['DIURETICO'] = [460454, 460455, 460456]
        #comorb_cb['FUROSEMIDA'] = [460458] está na ft_doc_eletronico_texto
        comorb_rb['IECA_BRA'] = [460472, 460473, 460474, 460475, 460476, 460477, 460478]
        comorb_rb['BCC'] = [460494, 460495, 460496, 460497, 460498]
        comorb_rb['BB'] = [460512, 460513, 460514, 460515, 460516, 460517]
        comorb_rb['ALFA_AGONISTAS_CENTRAIS'] = [460466, 460467]
        comorb_rb['POUPADOR_DE_K'] = [460480, 460481]
        comorb_rb['VASODILATADORES_DIRETOS'] = [460500, 460501]
        comorb_rb['ALFA_BLOQUEADORES'] = [460519, 460520]
        comorb_rb['MORISKY_1'] = [460532, 460533]
        comorb_rb['MORISKY_2'] = [460534, 460535]
        comorb_rb['MORISKY_3'] = [460536, 460537]
        comorb_rb['MORISKY_4'] = [460538, 460539]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,929,934,941,942,943,954,984,1029,1039,1044,1047,1048,1049,1061,1064,1066,1068,1070,1130]):
        comorb_cb['SCA_PREVIA'] = [390901] #SIM/NAO
        comorb_cb['ICP_PREVIA'] = [390902] #SIM/NAO
        comorb_cb['ISOGENICA'] = [390903] #SIM/NAO
        comorb_cb['VALVULAR'] = [390904] #SIM/NAO
        comorb_cb['FER'] = [390906] #SIM/NAO
        comorb_cb['FEP'] = [390905] #SIM/NAO
        comorb_cb['TPSV'] = [390907] #SIM/NAO
        comorb_cb['WPW'] = [390973] #SIM/NAO
        

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,929,934,941,942,943,954,983,983,984,1029,1039,1044,1047,1048,1049,1061,1064,1066,1068,1070,1130]):
        comorb_cb['ANGINA_ESTAVEL'] = [387449] #SIM/NAO

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,929,934,941,942,943,954,980,984,1029,1039,1044,1047,1048,1049,1061,1064,1066,1068,1070,1130]):
        comorb_cb['RM_PREVIA'] = [390951] #SIM/NAO

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,929,934,941,942,943,954,984,1029,1039,1041,1044,1047,1048,1049,1061,1064,1066,1068,1070,1130]):
        comorb_cb['CHAGAS'] = [387661] #SIM/NAO

    if any(doc in docs for doc in [1061,1064,1066,1068,1070,1130]):
        comorb_cb['ISQUEMICA'] = [445366] #SIM/NAO
        comorb_cb['EV'] = [445408,390908] #SIM/NAO

    if any(doc in docs for doc in [857,858,859,876,877,879,887,888,929,934,941,942,943,954,984,1029,1031,1039,1044,1047,1048,1049,1061,1064,1066,1068,1070,1130]):
        comorb_cb['FA'] = [390978] #SIM/NAO

    if any(doc in docs for doc in [942,943,954,980,984,1000,1029,1039,1044,1047,1048,1049,1061,1064,1066,1068,1070,1130]):
        comorb_cb['TV'] = [390909] #SIM/NAO

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,929,934,941,942,943,954,971,980,984,1029,1039,1044,1047,1048,1049,1061,1064,1066,1068,1070,1130]):
        comorb_cb['MP'] = [391798] #SIM/NAO

    if any(doc in docs for doc in [1061,1064,1066,1068,1070,1110,1125,1130,942,943,1031,1072]):
        comorb_rb['DPOC'] = [445384, 445385]
        comorb_cb['DPOC'] = [387673]

    if any(doc in docs for doc in [1070,1130]):
        comorb_cb['OUTROS_ARRITMIA'] = [453835] #SIM/NAO
        comorb_cb['OUTROS_IC'] = [453834] #SIM/NAO
        comorb_cb['SINCOPE'] = [464681] #SIM/NAO Está na fdet e fde

    if any(doc in docs for doc in [1037]):
        comorb_rb['CONDICOES_DE_ALTA'] = [433482,433484,433486,456426,456427,456428]
        comorb_rb['ALTA'] = [433468,433470,433472,433474,433476,433478,433480,456419,456416,456414,456423]

    if any(doc in docs for doc in [1043]):
        comorb_cb['OBITO_APOS_48_HORAS'] = [434567]
        comorb_cb['OBITO_ANTES_DE_48_HORAS'] = [434565]
        comorb_cb['NECRO'] = [434569]
        comorb_cb['IML'] = [434571]
        comorb_cb['SVO'] = [434573]
        comorb_cb['ATESTADO_OBITO'] = [434575]

    if any(doc in docs for doc in [1157, 1158]):
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_AVC'] = [492530, 492528]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_AVC_TIPO'] = [492534, 492536]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_AVC_DEFICIT_SEQUELA'] = [492538, 492540]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_SANGRAMENTO_ASSOCIADO_QUEDA_DE_HB'] = [492542, 492544]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_LESAO_RENAL_AGUDA'] = [492551, 492553]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_LESAO_RENAL_AGUDA_ESTAGIO_AKIN'] = [492555, 492557, 492559]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_NECESSIDADE_DE_TSR'] = [492563, 492561]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_TSR_DEFINITIVA_TEMPORARIA'] = [492570, 492567]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_NOVO_MARCA_PASSO_PERMANENTE'] = [492846, 492572]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_INFARTO_POS_PROCEDIMENTO'] = [492577, 492579]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_DISFUNCAO_VALVAR_PRECOCE_TIPOS'] = [492587, 492589, 492591, 492593]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_RE_INTERVENCAO_OU_CIRURGIA_NAO_PLANEJADA'] = [492599, 492597]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_INFECCAO_DO_SITIO_CIRURGICO'] = [492620, 492622]
        comorb_cb['COMPLICACOES_DURANTE_RECUPERACAO_TRATAMENTO_CLINICO_CIRURGICO'] = [492624]
        comorb_cb['COMPLICACOES_DURANTE_RECUPERACAO_NECESSIDADE_DE_RESSUTURA'] = [492626]
        comorb_cb['COMPLICACOES_DURANTE_RECUPERACAO_ACOMETIMENTO_OSSEO'] = [492628]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_OUTRA_INFECCAO'] = [492630, 492632]
        comorb_cb['COMPLICACOES_DURANTE_RECUPERACAO_INFECCAO_GASTROINTESTINAL'] = [492634]
        comorb_cb['COMPLICACOES_DURANTE_RECUPERACAO_INFECCAO_URINARIO'] = [492636]
        comorb_cb['COMPLICACOES_DURANTE_RECUPERACAO_INFECCAO_RESPIRATORIO'] = [492638]
        comorb_cb['COMPLICACOES_DURANTE_RECUPERACAO_INFECCAO_CORRENTE_SANGUINEA'] = [492640]
        comorb_cb['COMPLICACOES_DURANTE_RECUPERACAO_INFECCAO_CUTANEO'] = [492642]
        comorb_cb['COMPLICACOES_DURANTE_RECUPERACAO_CHOQUE_SEPTICO'] = [492644]
        comorb_cb['OUTRAS_COMPLICACOES_OUTRAS'] = [492688]
        comorb_rb['OUTRAS_COMPLICACOES_NOVA_ARRITMIA'] = [492664, 492666]
        comorb_cb['OUTRAS_COMPLICACOES_TV'] = [492678]
        comorb_cb['OUTRAS_COMPLICACOES_FV'] = [492680]
        comorb_cb['OUTRAS_COMPLICACOES_FA_FLUTTER'] = [492682]
        comorb_rb['OUTRAS_COMPLICACOES_ALTA_RESPOSTA_VENTRICULAR'] = [492684, 492686]
        comorb_rb['EVENTOS_TROMBOEMBOLICOS_TEV_TVP_TEP'] = [492692, 492690]
        comorb_rb['DERRAME_PLEURAL_PERICARDICO_SIGNIFICATIVO'] = [492698, 492700]
        comorb_rb['NECESSIDADE_DE_DRENAGEM'] = [492704, 492702]

    if any(doc in docs for doc in [1157]):
        comorb_rb['READMISSAO_EM_UTI'] = [492518, 492520]
        comorb_rb['COMPLICACOES_DURANTE_RECUPERACAO_DISFUNCAO_VALVAR_PRECOCE'] = [492583, 492585]
        comorb_rb['OUTRAS_COMPLICACOES_DELIRIUM'] = [492648, 492650]
        comorb_rb['OUTRAS_COMPLICACOES_REINTUBACAO_NAO_PLANEJADA'] = [492654, 492652]
        comorb_rb['OUTRAS_COMPLICACOES_TRAQUEOSTOMIA'] = [492658, 492656]
        comorb_rb['OUTRAS_COMPLICACOES_CHOQUE_CARDIOGENICO'] = [492670, 492668]
        comorb_rb['OUTRAS_COMPLICACOES_EDEMA_PULMONAR'] = [492662, 492660]
        comorb_rb['OUTRAS_COMPLICACOES_RELACIONADAS_A_ACESSOS_CATETERES'] = [492674, 492672]
        comorb_rb['PARALISIA_NERVO_FRENICO'] = [492694, 492696]
        comorb_cb['STATUS_VITAL_VIVO'] = [492708]
        comorb_cb['STATUS_VITAL_DOMICILIO'] = [492712]
        comorb_cb['STATUS_VITAL_CUIDADOS_DOMICILIARES_ESPECIALIZADOS'] = [492714]
        comorb_cb['STATUS_VITAL_CENTRO_DE_REABILITACAO'] = [492716]
        comorb_cb['STATUS_VITAL_OUTRO_HOSPITAL_INSTITUICAO'] = [492718]
        comorb_cb['STATUS_VITAL_OBITO'] = [492722]
        comorb_rb['OBITO_CAUSA_CARDIOVASCULAR'] = [492727, 492729]

    if any(doc in docs for doc in [1158]):
        comorb_rb['METODO_DE_CONTATO'] = [493065, 493063, 493067]
        comorb_rb['STATUS_VITAL'] = [493077, 493079]
        comorb_rb['LOCAL_DO_OBITO'] = [493083, 493085, 493089]
        comorb_rb['PACIENTE_FOI_AO_PRONTO_SOCORRO_DESDE_A_ALTA'] = [493094, 493092]
        comorb_rb['PACIENTE_FOI_REINTERNADO_DESDE_A_ALTA'] = [493098, 493100]
        comorb_rb['DISFUNCAO_VALVAR_POS_ALTA_HOSPITALAR'] = [493110, 493112]
        comorb_rb['READMISSAO_NAO_PLANEJADA_EM_UTI'] = [493118, 493120]
        comorb_rb['EXACERBACAO_DA_INSUFICIENCIA_CARDIACA'] = [493128, 493130]
        comorb_rb['COMPLICACOES_RELACIONADAS_A_MEDICACOES'] = [493126, 493124]
        comorb_rb['CLASSIFICACAO_FUNCIONAL_NYHA_NO_SEGUIMENTO'] = [493132, 493134, 493136, 493138]
        comorb_cb['TELEFONE'] = [497740]
        comorb_cb['PACIENTE'] = [497742]
        comorb_cb['FAMILIAR'] = [497744]
        comorb_cb['MEDICO_ASSISTENTE'] = [497746]

    if any(doc in docs for doc in [904]):
        comorb_rb['TREINAMENTO'] = [398889, 398891]

    if any(doc in docs for doc in [901,902,903,906]):
        comorb_rb['INTERRUPCAO_DO_TESTE'] = [398551, 398553]

    if any(doc in docs for doc in [895]):
        comorb_rb['RETORNOU_AO_TESTE'] = [398352, 398353]

    if any(doc in docs for doc in [903,904]):
        comorb_cb['DIFICULDADE_DE_COMPREENSAO_DO_TESTE'] = [398817]

    if any(doc in docs for doc in [895,900,901,902,906]):
        comorb_cb['BRADICARDIA'] = [398374]

    if any(doc in docs for doc in [887,888,895,900,901,902,903,904,906,929,934,941,942,943,980,1013,1029,1039,1044,1047,1048,1049]):
        comorb_cb['DISPNEIA_'] = [390981]

    if any(doc in docs for doc in [895,900,901,902,906,1013]):
        comorb_cb['TAQUICARDIA'] = [398372]

    if any(doc in docs for doc in [903,904,981]):
        comorb_cb['TOSSE'] = [398884]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,895,900,901,902,903,904,906,929,934,941,942,943,980,1014,1028,1029,1039,1044,1047,1048,1049]):
        comorb_cb['TONTURA'] = [387461]

    if any(doc in docs for doc in [895,900,901,902,906]):
        comorb_cb['CANSACO_EM_MMII'] = [398381]
        comorb_cb['PALIDEZ'] = [398364]

    if any(doc in docs for doc in [895,900,901,906]):
        comorb_cb['DOR_EM_MM_II'] = [398384]

    if any(doc in docs for doc in [895,900,901,902,903,904,906,1013]):
        comorb_cb['CIANOSE'] = [398370]

    if any(doc in docs for doc in [902]):
        comorb_cb['MEMBRO_SUPERIOR_DIREITO'] = [398578]
        comorb_cb['MEMBRO_SUPERIOR_ESQUERDO'] = [398580]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,891,929,934,941,942,943,954,984,1029,1031,1039,1041,1044,1047,1048,1049,1072]):
        comorb_cb['TABAGISMO'] = [390980]

    if any(doc in docs for doc in [881,884,885,914,1041]):
        comorb_cb['HAS'] = [395553]

    if any(doc in docs for doc in [856,932,1041]):
        comorb_cb['ALCOOL'] = [392024]

    if any(doc in docs for doc in [879,880,928,1034,1041]):
        comorb_cb['DIABETES'] = [387659]

    if any(doc in docs for doc in [901]):
        comorb_cb['MAO_DOMINANTE_ESQUERDA'] = [398461]
        comorb_cb['MAO_DOMINANTE_DIREITA'] = [398543]

    if any(doc in docs for doc in [905]):
        comorb_cb['NORMOTONIA'] = [398950]
        comorb_cb['HIPOTONIA'] = [398951]
        comorb_cb['HIPERTONIA'] = [398970]

    if any(doc in docs for doc in [1041]):
        comorb_cb['AVALIACAO_FISIOTERAPIA'] = [434277]
        comorb_cb['REAVALIACAO_FISIOTERAPIA'] = [434279]
        comorb_cb['DISLIPIDEMIA'] = [387665]
        comorb_cb['ESTRESSE'] = [434296]
        comorb_cb['HF_ICO'] = [434306]
        comorb_cb['ATIVO'] = [434298]

    if any(doc in docs for doc in [880,914,970,974,976,977,978,981,982,985,987,1041,1072]):
        comorb_cb['OBESIDADE'] = [387685, 398636]

    if any(doc in docs for doc in [1041,1072]):
        comorb_cb['AVE'] = [387675]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,891,929,934,941,942,943,954,984,1029,1039,1041,1044,1047,1048,1049]):
        comorb_cb['SEDENTARISMO'] = [387406]
    
    if any(doc in docs for doc in [942,943,1072]):
        comorb_cb['ATIVIDADE_FISICA'] = [387411]
        comorb_cb['NEOPLASIA'] = [392718]

    if any(doc in docs for doc in [907,914,915,916,1035,1041]):
        comorb_cb['ICC'] = [399249]

    if any(doc in docs for doc in [914,1041,938,1115,943]):
        comorb_cb['IAM'] = [402224]
        comorb_cb['SAFENECTOMIA'] = [411476]
        comorb_cb['ANEURISMECTOMIA'] = [413710]
        comorb_cb['SIMPATECTOMIA'] = [411478]
        comorb_cb['FASCIOTOMIA'] = [411482]
        comorb_cb['ANEURISMA_ABDOMINAL'] = [411494]
        comorb_cb['ENXERTO_ARTERIAL'] = [411501]
        comorb_cb['PSEUDO_ANEURISMA_FEMURAL'] = [411508]
        comorb_cb['PSEUDO_ANEURISMA_BRAQUIAL'] = [411526]
        comorb_cb['ENDARTERECTOMIA'] = [411533]
        comorb_cb['IMPLANTE_DESFIBRILADOR'] = [411550]
        comorb_cb['IMPLANTE_MP_DEFINITIVO'] = [411557]
        comorb_cb['REVASC_MIOCARDIO'] = [411606]
        comorb_cb['CONGENITO_E_VALVULAR'] = [411610]
        comorb_rb['PELE'] = [411628, 411630]
        comorb_rb['JEJUM'] = [411661, 411663]
        comorb_rb['ACESSO_VENOSO_PERIFERICO'] = [411665, 411667]
        comorb_rb['LOCAL_ACESSO_VENOSO_PERIFERICO'] = [411675, 411678, 411680, 411682]
        comorb_rb['PROCEDIMENTO_CIRURGICO'] = [413700, 413701]
        comorb_rb['PROCEDIMENTO_ASSOCIADO'] = [413704, 413705]
        comorb_cb['INTERVENCAO_VAO'] = [413711]
        comorb_cb['INTERVENCAO_VMI'] = [413712]
        comorb_cb['MAMARIA'] = [413713]
        comorb_cb['SAFENA'] = [413714]
        comorb_cb['RADIAL'] = [413713]
        

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,897,907,910,911,912,913,915,916,918,929,934,941,942,943,951,954,955,971,980,982,988,990,1018,1019,1021,1029,1035,1039,1041,1044,1047,1048,1049]):
        comorb_cb['FC'] = [390919]

    if any(doc in docs for doc in [535,536,537,547,549,550,566,568,569,651,673,729,1041]):
        comorb_cb['SE_O_PACIENTE_TEVE_ALGUMA_DOENCA_REUMATICA'] = [77014]

    if any(doc in docs for doc in [57,1041,942,943,1072]):
        comorb_cb['EXTABAGISTA'] = [84051, 390983]
    
    if any(doc in docs for doc in [943,942,1072,241,710,891,893,894]):
        comorb_cb['ETILISMO'] = [453000, 387408]
    
    if any(doc in docs for doc in [895,900,901,902,903,904,906]):
        comorb_cb['QUEDA_DE_SUTURACAO'] = [398368]

    if any(doc in docs for doc in [1138]):
        comorb_rb['RISCO_QUEDA'] = [468968, 468970, 411513, 411518]
        comorb_rb['GLICEMIA'] = [468954, 468956]
        comorb_rb['NOVA_DOR'] = [468962, 468964]
        comorb_rb['APTO_PARA_REALIZAR_EXERCICIOS'] = [468978, 468980]
        comorb_rb['TREINAMENTO_MUSCULAR_RESPIRATORIO'] = [468984, 468986]
        comorb_rb['ATINGIU_FC_DE_TREINO'] = [468994, 468996]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_SINCOPE'] = [469005]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_DISPNEIA'] = [469009]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_TONTURA'] = [469011]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_PALIDEZ'] = [469013]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_QUEDA_DE_SPO2'] = [469015]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_CANSACO_MMII'] = [469021]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_DOR_MMII'] = [469023]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_QUEDA'] = [469025]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_OUTROS'] = [469027]
        comorb_cb['NAO_INTERROMPEU'] = [483354]

    if any(doc in docs for doc in [1138,1142]):
        comorb_cb['MEMBRO_DOMINANTE_DIREITO'] = [469059]
        comorb_cb['MEMBRO_DOMINANTE_ESQUERDO'] = [469061]
        comorb_rb['RISCO_CLINICO_INTERNACAO_INTERVENCAO_DESCOMPENSACAO'] = [471133, 471135, 471137]
        comorb_rb['RISCO_CLINICO_CAPACIDADE_FUNCIONAL'] = [471139, 471141, 471143]
        comorb_rb['RISCO_CLINICO_SINAIS_SINTOMAS_ISQUEMIA_MIOCARDICA'] = [471145, 471147, 471149]
        comorb_rb['RISCO_CLINICO_SINTOMATOLOGIA'] = [471151, 471153, 471155]
        comorb_rb['RISCO_CLINICO_OUTRAS_CARACTERISTICAS_CLINICAS'] = [471157, 471159, 471161]

    if any(doc in docs for doc in [1138,1142,1163]):
        comorb_rb['TESTE_DE_RM_MEMBRO_SUPERIOR'] = [470969, 470971]
        comorb_rb['TESTE_DE_RM_MEMBRO_INFERIOR'] = [470979, 470983]
        comorb_cb['NAO_REALIZOU_PES_UNIDOS_EM_PARALELO'] = [471052]
        comorb_cb['NAO_REALIZOU_PE_PARCIALMENTE_A_FRENTE'] = [471058]
        comorb_cb['NAO_REALIZOU_PE_A_FRENTE'] = [471064]
        comorb_cb['NAO_REALIZOU_PRIMEIRA_TENTATIVA_TESTE_VELOCIDADE_MARCHA'] = [471074]
        comorb_cb['NAO_REALIZOU_SEGUNDA_TENTATIVA_TESTE_VELOCIDADE_MARCHA'] = [471076]
        comorb_rb['AVAL_DE_GANHOS_DE_SAUDE_MOBILIDADE'] = [471095, 471097, 471099]
        comorb_rb['AVAL_DE_GANHOS_DE_SAUDE_CUIDADOS_PESSOAIS'] = [471101, 471103, 471105]
        comorb_rb['AVAL_DE_GANHOS_DE_SAUDE_ATIVIDADES_HABITUAIS'] = [471107, 471109, 471111]
        comorb_rb['AVAL_DE_GANHOS_DE_SAUDE_DOR_MAL_ESTAR'] = [471113, 471115, 471117]
        comorb_rb['AVAL_DE_GANHOS_DE_SAUDE_ANSIEDADE_DEPRESSAO'] = [471119, 471125, 471127]

    if any(doc in docs for doc in [895,900,904,1138,1142,1144,1163]):
        comorb_rb['PAROU_DURANTE_O_TESTE'] = [471030, 471032, 398335, 398336]

    if any(doc in docs for doc in [1136,1137,1138,1140,1141]):
        comorb_rb['OXIGENOTERAPIA'] = [468335, 468337]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_PCR'] = [468696]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_HIPERTENSAO'] = [468700]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_TAQUICARDIA'] = [468704]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_BRADICARDIA'] = [468706]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_CIANOSE'] = [468711]
        comorb_cb['INTERROMPEU_EXERCICIO_DEVIDO_PRECORDIALGIA'] = [468715]

    if any(doc in docs for doc in [1043]):
        comorb_cb['ATESTADO_OBITO'] = [434575] #SIM/NAO
        comorb_cb['OBITO_ANTES_DE_48_HORAS'] = [434565] #SIM/NAO
        comorb_cb['OBITO_APOS_48_HORAS'] = [434567] #SIM/NAO
        comorb_cb['IML'] = [434571] #SIM/NAO
        comorb_cb['SVO'] = [434573] #SIM/NAO
        comorb_cb['NECRO'] = [434569] #SIM/NAO

    if any(doc in docs for doc in [926]):
        comorb_rb['SITUACAO_PREVIDENCIARIA'] = [405258, 405256, 405260]
        comorb_rb['SITUACAO_HABITACIONAL'] = [404765, 404770, 404772, 404774]
        comorb_rb['BENEFICIO_PREVID'] = [405934, 405936]
        comorb_cb['BENEFICIO_PREVID_TIPO'] = [405524, 405267, 405269, 405265, 405262 ]
        comorb_cb['BENEFICIO_ASSIST_TIPO'] = [405273, 405271, 405275]
        comorb_rb['ENDERECO_TEMPORARIO'] = [404792, 404788]
        comorb_rb['ENDERECO_TEMP_TIPO'] = [404797, 404801, 405755]
        comorb_rb['RESIDE'] = [405743, 405745, 405747, 405749]
        comorb_rb['RENDA_FAMILIAR'] = [405288, 405291, 405297, 405301, 405305, 405309, 405313]

    if any(doc in docs for doc in [938, 942, 943, 1031, 1072, 1073, 1115, 1173]):
        comorb_rb['ISOLAMENTO'] = [321911, 321912]
        comorb_cb['ISOLAMENTO'] = [453217]
        comorb_rb['SITUACAO_HABITACIONAL'] = [404765, 404770, 404772, 404774]
        comorb_rb['BENEFICIO_PREVID'] = [405934, 405936]
        comorb_cb['BENEFICIO_PREVID_TIPO'] = [405524, 405267, 405269, 405265, 405262 ]
        comorb_cb['BENEFICIO_ASSIST_TIPO'] = [405273, 405271, 405275]
        comorb_rb['ENDERECO_TEMPORARIO'] = [404792, 404788]
        comorb_rb['ENDERECO_TEMP_TIPO'] = [404797, 404801, 405755]
        comorb_rb['RESIDE'] = [405743, 405745, 405747, 405749]
        comorb_rb['RENDA_FAMILIAR'] = [405288, 405291, 405297, 405301, 405305, 405309, 405313]

    missing_values = None
    missing_values = [
        metadado 
        for metadado in df_comorbidades['CD_METADADO'].unique()
        if metadado not in (
            set(item for sublist in comorb_rb.values() for item in sublist)
                .union(
                    set(item2 for sublist2 in comorb_cb.values() for item2 in sublist2)
                )
        )
    ]
    if missing_values:
        raise RuntimeError(f"These rb/cb metadata codes are missing: {sorted(missing_values)}")

    df_comorbidades = df_comorbidades.groupby(['CD_ATENDIMENTO', 'CD_PACIENTE', 'DH_DOCUMENTO', 'resposta'])['CD_METADADO'].apply(list).reset_index()

    for dct in [comorb_rb, comorb_cb]:
        for category, values in dct.items():
            df_comorbidades[category] = df_comorbidades['CD_METADADO'].apply(
                lambda x: [meta for meta in x if meta in values]
            )

    df_comorbidades.drop(columns=['CD_METADADO'], inplace=True)

    comorb_rn = {
        #AVC
            '410895':'SIM', '410897':'NAO',
        #HAS
            '410904':'SIM', '410906':'NAO',
        #DLP
            '445374':'SIM', '445373':'NAO',
        #DRC
            '445382':'SIM', '445380':'NAO', '446326':'DIALITICO',
        #DPOC
            '445385': 'SIM', '445384': 'NAO',
        #DAOP
            '446429':'SIM', '446430':'NAO',
        #DAC
            '445355':'SIM', '445353':'NAO',
        #DAC TIPOS
            '390901':'SCA PREVIA', '387449':'ANGINA ESTAVEL', '390902':'ICP PREVIA', '390951':'RM PREVIA',
        #IC
            '445363':'SIM', '445361':'NAO',
        #IC TIPOS
            '445366':'ISQUEMICA', '387661':'CHAGASICA', '390904':'VALVULAR', '390905':'FEP', '390906':'FER', '453834':'ICC OUTROS',
        #ARRITMIA
            '414259':'SIM', '414262':'NAO',
        #ARRITMIA TIPOS
            '390907':'TPSV', '390909':'TV', '390973':'WPW', '390978':'FA/FLUTTER', '391798':'MP', '445408':'EV', '453835':'ARRITMIA OUTROS', '464681':'SINCOPE',
        #ANGINA
            '445492':'SIM', '445490':'NAO',
        #ANGINA TIPOS
            '391032':'ANGINA CCS1', '391026':'ANGINA CCS3', '391033':'ANGINA CCS2', '391034':'ANGINA CCS4',
        #EDEMA
            '402270':'SIM', '402272':'NAO',
        #DT DOR TORACICA
            '440031':'SIM', '440029':'NAO',
        #DT TIPOS 
            '408125':'TIPO A', '408126':'TIPO B', '408127':'TIPO C', '408128':'TIPO D',
        #SINCOPE    
            '430175':'SIM', '430177':'NAO', 
        #DISPNEIA
            '440017':'SIM', '440015':'NAO',
        #DISPNEIA TIPOS
            '440021':'DISPNEIA I', '440023':'DISPNEIA II', '440025':'DISPNEIA III', '440027':'DISPNEIA IV',
            '387476':'DISPNEIA I', '387478':'DISPNEIA II', '387480':'DISPNEIA III', '387482':'DISPNEIA IV',
        #PALPITACAO
            '440035':'SIM', '440033':'NAO',
        #VALVOPATIA
            '445416':'SIM', '445413':'NAO',
        #VALVOPATIA TIPOS
            '391799':'NATIVA', '391792':'PROTESE',
        #VALVOPATIA PROTESE
            '446330':'MECANICA', '446332':'BIOLOGICA', '445475':'TAVI',
        #IAO
            '445449':'SIM', '445447':'NAO',
        #EAO
            '445453':'SIM', '445451':'NAO',
        #IMI
            '445457':'SIM', '445455':'NAO',
        #EMI
            '445461':'SIM', '445459':'NAO',
        #ITRI
            '445466':'SIM', '445463':'NAO',
        #ETRI
            '445470':'SIM', '445468':'NAO',
        #TONTURA
            '445500':'SIM', '445498':'NAO',
        #CLASSIFICACAO_PS
            '452354':'SCA', '452356':'IC', '452358':'ARRITMIA', '452360':'VALVULA', '452362':'CONGENITO', '452365':'C.CARDIACA', '452367':'CLINICO', '452369':'NEUROLOGIA', '444238':'C.VASCULAR',
        #ALCOOL_SEMANA
            '460318':'NADA', '460319':'<7', '460320':'7-21', '460321':'>21',
        #HAS_SECUNDARIA
            #'460245':'AFASTADA', '460246':'PESQUISA NAO REALIZADA', '460244':'EM RASTREAMENTO', #se comporta como checkbox 
        #HAS_SECUNDARIA_TIPOS
            '460248':'RENOVASCULAR', '460253':'TAKAYASU', '460250':'HIPERALDO', '460251':'FEO', '465266':'COARTACAO', '465267':'SAOS', '465268':'OUTROS HAS',
        #ESCOLARIDADE
            '465261':'A', '460268':'FI', '460269':'FC', '460270':'EMI', '460271':'EMC', '460272':'SI', '460273':'SC',
        #HIPERTROFIA_DE_VE
            '460346':'SIM', '460347':'NAO', '460348':'NAO AVALIADA',
        #ALBUMINURIA>30
            '460356':'SIM', '460357': 'NAO', '460358':'NAO AVALIADA',
        #FUNDO_OLHO
            '460362': 'G0', '460363': 'G1', '460364': 'G2', '460365': 'G3', '460366': 'NAO AVALIADA',
        #PA_MEDIDA
            #'': 'BD', '': 'BE', #MEDIDA_1 (está em ft_texto)
            '460384': 'BD', '460385': 'BE', #MEDIDA_2
            '460394': 'BD', '460395': 'BE', #MEDIDA_3
            '466709': 'NENHUMA', #FORA DO CONSULTÓRIO
        #META_PRESSORICA
            '460432': 'PA < 140/90 (BR RM)', '460434': 'PA <  130/80 (AR)', '460435': 'PA < 140/80 (Idoso)', '460436': 'PA < 150/80 (Idoso Fragil)',
        #DENERVACAO_RENAL
            '460437': 'SIM', '460439': 'NAO',
        #ANGIOPLASTIA_DE_ART_RENAL
            '460438': 'SIM', '460440': 'NAO',
        #DIURETICO
            '460454': 'CLORTALIDONA', '460455': 'HIDROCLOROTIAZIDA', '460456': 'INDAPAMIDA',
        #IECA_BRA
            '460472': 'CAPTOPRIL', '460473': 'ENALAPRIL', '460474': 'OUTRO ICEA', '460475': 'LOSARTANA', 
            '460476': 'OLMESARTANA', '460477': 'OUTRO BRA', '460478': 'SACUBITRIL VALSARTANA',
        #BCC
            '460494': 'ANLODIPINO', '460495': 'NIFEDIPINO', '460496': 'DILTIAZEM', 
            '460497': 'VERAPAMIL', '460498': 'OUTRO BCC',
        #BB
            '460512': 'ATENOLOL', '460513': 'PROPRANOLOL', '460514': 'METOPROLOL', '460515': 'BISOPROLOL',
            '460516': 'CARVEDILOL', '460517': 'OUTRO BB', 
        #ALFA_AGONISTAS_CENTRAIS
            '460466': 'ALFAMETILDOPA', '460467': 'CLONIDINA',
        #POUPADOR_DE_K
            '460480': 'ESPIRONOLACTONA', '460481': 'AMILORIDA',
        #VASODILATADORES_DIRETOS
            '460500': 'HIDRALAZINA', '460501': 'MINOXIDIL',
        #ALFA_BLOQUEADORES
            '460519': 'DOXAZOSINA', '460520': 'PRAZOSINA',
        #MORISKY_1
            '460532': 'SIM', '460533': 'NAO',
        #MORISKY_2
            '460534': 'SIM', '460535': 'NAO',
        #MORISKY_3
            '460536': 'SIM', '460537': 'NAO',
        #MORISKY_4
            '460538': 'SIM', '460539': 'NAO',
        #DM
            '410908':'SIM', '410910':'NAO', '410899': 'DMID', '410900': 'DMNID', '396925': 'DM TIPO 1', '396927': 'DM TIPO 2', '464663': 'SIM', '415610': 'SIM', '407453': 'SIM', '407454': 'NAO',
            '432674': 'DMID', '432676': 'DMNID',
        #ALERGIA
            '411336': 'NAO', '411337':'SIM',
        #PELE
            '411628':'PELE INTEGRA', '411630': 'PELE COM ESCORIACOES',
        #JEJUM
            '411661':'SIM', '411663':'NAO',
        #ACESSO_VENOSO_PERIFERICO
            '411665':'SIM', '411667':'NAO',
        #LOCAL_ACESSO_VENOSO_PERIFERICO
            '411675':'MSD', '411678':'MSE', '411680':'MID', '411682':'MIE',
        #'PROCEDIMENTO_CIRURGICO
            '413700':'RM COMPLETA', '413701':'RM INCOMPLETA',
        #PROCEDIMENTO_ASSOCIADO
            '413704': 'SIM', '413705':'NAO',
        #INSULINA
            '460297': 'SIM', '460298': 'NAO', '396971': 'INSULINA NPH', '396973': 'INSULINA IRREGULAR',
        #TABAGISMO
            '387394': 'SIM', '439924': 'SIM', '439920': 'NAO', '387396': 'NAO', '439922': 'EX-TBG',
        #CONDICOES_DE_ALTA
            '433482': 'CURADO', '433484': 'MELHORADO', '433486': 'INALTERADO',
            '456426': 'CURADO', '456427': 'MELHORADO', '456428': 'INALTERADO',
        #ALTA
            '433468': 'ALTA SEM ENCAMINHAMENTO', '433470': 'ALTA EVASAO', '433472': 'ALTA A PEDIDO', '433474': 'ALTA COM ENCAMINHAMENTO PARA SETOR', 
            '433476': 'SETOR ENCAMINHAMENTO ALTA', '433478': 'ALTA TRANSFERIDO', '433480': 'ALTA LOCAL TRANSFERENCIA', '456419': 'COM RETORNO AMBULATORIAL NO SETOR',
            '456416': 'COM ENCAMINHAMENTO PARA REDE BASICA DE SAUDE', '456414': 'SEM EMCAMINHAMENTO', '456423': 'TRANSFERIDO PARA',
        #Antecedentes Pessoais (odonto) + Preop Coron
            '453000': 'EX_ETILISTA', '387408': 'ETILISMO',
        #Secao de origem (odonto)
            '452946': 'ANTICOAGULACAO',   
        #Readmissão em UTI
            '492520': 'SIM', '492518': 'NAO',
        #Complicações Durante a Recuperação:
            #AVC
            '492530': 'SIM', '492528': 'NAO',
            #AVC Tipos
            '492534': 'ISQUEMICO', '492536': 'HEMORRAGICO',
            #AVC Déficit Sequela
            '492538': 'SIM', '492540': 'NAO',
            #Sangramento associado a queda de HB
            '492544': 'SIM', '492542': 'NAO',
            #Lesão renal aguda
            '492553': 'SIM', '492551': 'NAO',
            #Estágio Akin
            '492555': '1', '492557': '2', '492559': '3',
            #Necessidade de TSR
            '492563': 'SIM', '492561': 'NAO',
            #TSR Temporaria ou Definitiva
            '492567': 'TEMPORARIA', '492570': 'DEFINITIVA',
            #Novo Marca-Passo Permanente
            '492846': 'SIM', '492572': 'NAO',
            #Infarto pós-procedimento
            '492579': 'SIM', '492577': 'NAO',
            #Disfunção Valvar Precoce
            '492585': 'SIM', '492583': 'NAO',
            #Disfunção Valvar Precoce Tipos
            '492587': 'ESTRUTURAL', '492589': 'NAO ESTRUTURAL', '492591': 'ENDOCARDITE', '492593': 'TROMBOSE PROTESE',
            #Re-internação ou cirurgia não planejada
            '492599': 'SIM', '492597': 'NAO',
            #Infecção do sítio cirúrgico
            '492622': 'SIM', '492620': 'NAO',
            #Outra Infecção
            '492632': 'SIM', '492630': 'NAO',
            #Delirium
            '492650': 'SIM', '492648': 'NAO',
            #Reintubação não planejada
            '492654': 'SIM', '492652': 'NAO',
            #Traqueostomia
            '492658': 'SIM', '492656': 'NAO',
            #Choque Cardiogênico
            '492670': 'SIM', '492668': 'NAO',
            #Edema Pulmonar
            '492662': 'SIM', '492660': 'NAO',
            #Complicações relacionadas a acessos ou cateteres
            '492674': 'SIM', '492672': 'NAO',
            #Nova Arritmia
            '492666': 'SIM', '492664': 'NAO',
            #Alta resposta ventricular
            '492686': 'SIM', '492684': 'NAO',
            #Eventos Tromboembolicos:
            '492692': 'SIM', '492690': 'NAO',
        #Paralisia nervo frênico
            '492696': 'SIM', '492694': 'NAO',
        #Derrame pleural pericardio significativo
            '492700': 'SIM', '492698': 'NAO',
        #Necessidade de drenagem
            '492704': 'SIM', '492702': 'NAO',
        #Obito causa cardiovascular
            '492729': 'SIM', '492727': 'NAO',
        #Local do óbito
            '493083': 'DOMICILIO', '493085': 'HOSPITAL', '493089': 'OUTRO',
        #Método de contato
            '493063': 'CONSULTA PRESENCIAL', '493065': 'PACIENTE', '493067': 'FAMILIAR',
        #Status vital
            '493077': 'VIVO', '493079': 'OBITO',
        #Paciente foi ao pronto socorro desde a alta
            '493094': 'SIM', '493092': 'NAO',
        #Paciente foi reinternado desde a alta
            '493100': 'SIM', '493098': 'NAO',
        #Disfunção valvar pós alta hospital
            '493112': 'SIM', '493110': 'NAO',
        #Re-adimissão não planejada em UTI
            '493120': 'SIM', '493118': 'NAO',
        #Exacerbação da insufiência cardíaca
            '493130': 'SIM', '493128': 'NAO',
        #Complicações relacionadas a medicações
            '493126': 'SIM', '493124': 'NAO',
        #Classificação funcional NYHA no seguimento
            '493132': 'I', '493134': 'II', '493136': 'III', '493138': 'IV',
        #Parou Durante o teste
            '398335': 'SIM', '398336': 'NAO',
        #Interrupção do teste
            '398551': 'SIM', '398553': 'NAO',
        #Treinamento
            '398889': 'SIM', '398891': 'NAO',
        #Retornou ao teste
            '398352': 'SIM', '398353': 'NAO',
        #Risco Queda (está como 'RISCO DOR' no DD)
            '468968': 'SIM', '468970': 'NAO', '411513':'SIM', '411518':'NAO',
        #Dor
        #Glicemia
            '468954': 'SIM', '468956': 'NAO',
        #Nova Dor 
            '468962': 'SIM', '468964': 'NAO',
        #Apto para realizar exercícios
            '468978': 'SIM', '468980': 'NAO',
        #Treinamento muscular respiratório
            '468984': 'SIM', '468986': 'NAO',
        #Atingiu FC de treino
            '468994': 'SIM', '468996': 'NAO',
        #Teste de RM
            #Membro Superior
                '470969': 'DIREITO', '470971': 'ESQUERDO',
            #Membro Inferior
                '470979': 'DIREITO', '470983': 'ESQUERDO',
        #Avaliação de ganhas de saúde
            #Mobilidade
                '471095': '1', '471097': '2', '471099': '3',
            #Cuidados Pessoais
                '471101': '1', '471103': '2', '471105': '3',
            #Atividades Habituais
                '471107': '1', '471109': '2', '471111': '3',
            #Dor ou Mal Estar
                '471113': '1', '471115': '2', '471117': '3',
            #Ansiedade e depressão
                '471119': '1', '471125': '2', '471127': '3',
        #Estratificação do Risco Clínico dos Pacientes
            #Intervenção por evento cardiovascular, Intervenção cardiovascular ou descompensação clínica
                '471133': 'ALTO', '471135': 'INTERMEDIARIO', '471137': 'BAIXO',
                '471139': 'ALTO', '471141': 'INTERMEDIARIO', '471143': 'BAIXO',
                '471145': 'ALTO', '471147': 'INTERMEDIARIO', '471149': 'BAIXO',
                '471151': 'ALTO', '471153': 'INTERMEDIARIO', '471155': 'BAIXO',
                '471157': 'ALTO', '471159': 'INTERMEDIARIO', '471161': 'BAIXO',
        #Parou durante o teste
            '471030': 'SIM', '471032': 'NAO',
        #Oxigenoterapia
            '468335': 'SIM', '468337': 'NAO',
        #Demograficos Serviço Social
        #'SITUACAO_PREVIDENCIARIA
            '405256':'SEGURADO', '405258':'NAO SEGURADO', '405260':'SEGURADO C/ CARENCIA',
        #SITUACAO_HABITACIONAL
            '404765':'IMOVEL PROPRIO', '404770':'IMOVEL ALUGADO', '404772':'IMOVEL CEDIDO', '404774':'IMOVEL OUTROS',
        #BENEFICIO_PREVID
            '405934': 'SIM', '405936': 'NAO',
        #BENEFICIO_PREVID_TIPO   
            '405524':'APOSENTADO', '405267':'PENSIONISTA', '405262':'AUXILIO MATERNIDADE', '405265':'AUXILIO DOENÇA', 
            '405269':'AUXILIO RECLUSAO',
        #BENEFICIO_ASSIST_TIPO
            '405271':'BOLSA FAMILIA', '405273':'BPC', '405275':'BENEFICIO ASSISTENCIAL OUTROS',
        #ENDERECO_TEMPORARIO
            '404788': 'SIM', '404792': 'NAO', 
        #ENDERECO_TEMP_TIPO
            '404797':'CASA DE APOIO', '404801':'ENDEREÇO TEMPORARIO OUTROS', '405755':'ENDEREÇO TEMPORARIO OUTROS',
        #RESIDE
            '405743':'RESIDE SOZINHO', '405745':'RESIDE C/ FAMILIA', '405747':'SERVIÇO ACOLHIMENTO INSTITUCIONAL', 
            '405749':'PESSOA EM SITUAÇAO DE RUA',
        #RENDA_FAMILIAR 
            '405288':'1/2 SALARIO', '405291':'3 SALARIOS', '405297':'1 SALARIO', '405301':'>3 SALARIOS',
            '405305':'1 1/2 SALARIO', '405309':'2 SALARIOS', '405313':'2 1/2 SALARIOS', 
        #FICHAS CIRURGICAS PRE/POS OPERATORIA ENF/MED
            '321911': 'SIM', '321913': 'NAO',

    }

    df_comorbidades[df_comorbidades.columns[4:]] = (
        df_comorbidades[df_comorbidades.columns[4:]]
        .astype(str) 
        .replace([r'[^0-9\s]', r'\[\]'], '', regex=True)
    )

    for category in comorb_cb.keys():
        df_comorbidades.loc[
            df_comorbidades[category].apply(lambda x: len(x) > 0), 
            category
        ] = df_comorbidades['resposta']

    df_comorbidades.replace('', pd.NA, inplace=True)

    df_comorbidades.sort_values(by=['CD_ATENDIMENTO', 'CD_PACIENTE', 'DH_DOCUMENTO', 'resposta'], inplace=True, ignore_index=True)

    comorb_cb_cols = list(comorb_cb.keys())

    df_comorbidades[comorb_cb_cols] = df_comorbidades.groupby(
        ['CD_ATENDIMENTO','CD_PACIENTE','DH_DOCUMENTO']
    )[comorb_cb_cols].ffill()

    df_comorbidades = df_comorbidades.loc[df_comorbidades['resposta'] == 'SIM']

    df_comorbidades[df_comorbidades.columns[4:]] = (df_comorbidades[df_comorbidades.columns[4:]].replace(comorb_rn))

    df_comorbidades.drop(columns=['resposta'], inplace=True)

    query_comorbidades = f"""
        SET DATEFORMAT ymd;
        SELECT
            fdet.CD_PACIENTE,
            fdet.CD_ATENDIMENTO,
            fdet.DH_DOCUMENTO,
            fdet.NK_CD_CAMPO,
            dcd.CD_METADADO,
            fdet.DS_RESPOSTA
        FROM ft_doc_eletronico_texto fdet 
        LEFT JOIN dim_campo_documento dcd ON fdet.NK_CD_CAMPO = dcd.NK_CD_CAMPO
        WHERE
            fdet.CD_DOCUMENTO IN :docs
            {f"AND fdet.CD_PACIENTE IN :ids" if ids else ""}
    """

    print("Extraindo dados de ft_doc_eletronico_texto...", end="\r", flush=True)
    if not ids:
        df_comorbidades_txt = query_sql_to_dataframe(engine, query_comorbidades, params={'docs': docs})
    else:
        chunk_comorbidades = []
        for i, chunk in enumerate(ids, start=1):
            chunk_comorbidades.append(query_sql_to_dataframe(engine, query_comorbidades, params={'docs': docs, 'ids': chunk}))
            print(f"Extraindo dados de ft_doc_eletronico_texto... Progresso: {i}/{len(ids)} ({i/len(ids):.1%})", end="\r", flush=True)
        df_comorbidades_txt = pd.concat(chunk_comorbidades)

    print(f"Tratando dados de ft_doc_eletronico_texto... {' '*50}", end="\r", flush=True)

    df_comorbidades_txt['CD_METADADO'] = df_comorbidades_txt['CD_METADADO'].astype('Int64')
    df_comorbidades_txt.drop_duplicates(ignore_index=True, inplace=True)
    df_comorbidades_txt = df_comorbidades_txt[~df_comorbidades_txt['CD_METADADO'].isna()]

    comorb_txt = {}

    if any(doc in docs for doc in [1070]):
        comorb_txt['ORIGEM'] = [453682]

    if any(doc in docs for doc in [1130,1070]):
        comorb_txt['OUTROS_IC'] = [453834] #true/false Está na fdet e fde
        comorb_txt['OUTROS_ARRITIMIA'] = [453835] #true/false Está na fdet e fde
        comorb_txt['SINCOPE'] = [464681] #true/false Está na fdet e fde

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,891,907,915,916,929,934,941,942,943,954,984,1029,1035,1039,1044,1047,1048,1049,1061,1064,1066,1068,1070,1136,1137,1130,1138,1140,1141,1142,1163]):
        comorb_txt['ANTECEDENTES_PESSOAIS'] = [391040]

    if any(doc in docs for doc in [971,980,998,1003,1005,1006,1007,1008,1031,1034,1037,1041,1051,1061,1064,1066,1068,1070,1107]):
        comorb_txt['DESCRICAO_MEDICAMENTOS'] = [387423]

    if any(doc in docs for doc in [1061,1064,1066,1068,1070,1107,1130]):
        comorb_txt['DESCRICAO_DOENCAS_CARDIOVASCULARES'] = [445439]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,881,884,885,887,888,929,934,941,942,943,950,952,954,980,989,1029,1031,1039,1044,1047,1048,1049,1061,1064,1066,1068,1070]):
        comorb_txt['OBSERVACAO_EXAME_FISICO'] = [390930]

    if any(doc in docs for doc in [971,1061,1064,1066,1068,1070,1135]):
        comorb_txt['EVOLUCAO_DIARIA'] = [422853]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,880,881,884,885,887,888,897,907,910,911,912,913,915,916,918,929,934,941,942,943,950,952,954,958,959,968,969,980,984,998,1003,1005,1029,1035,1039,1044,1045,1047,1048,1049,1061,1064,1066,1068,1070,1125]):
        comorb_txt['CONDUTA'] = [390962]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,891,897,910,911,912,913,929,934,941,942,943,954,984,998,1003,1005,1006,1007,1008,1014,1029,1031,1037,1039,1041,1043,1044,1046,1047,1048,1049,1061,1064,1066,1068,1070]):
        comorb_txt['EXAMES_COMPLEMENTARES'] = [390915]

    if any(doc in docs for doc in [1068,1110,942,943,1072]):
        #comorb_txt['ALTURA'] = [460265,387402]
        #comorb_txt['PESO'] = [460266, 387400]
        #comorb_txt['IMC'] = [465248]
        comorb_txt['CIRCUNFERENCIA_BRAQUIAL'] = [460267]
        comorb_txt['CELULAR'] = [460275]
        comorb_txt['IDADE_DIAGNOESTICO_HA'] = [460264]
        comorb_txt['NUCLEO_FAMILIAR'] = [460276]
        comorb_txt['RENDA_DO_NUCLEO'] = [460277]
        comorb_txt['FEVE_MAIS_RECENTE'] = [460352]
        comorb_txt['IMVE_MAIS_RECENTE'] = [460311]
        comorb_txt['CREATININA_MAIS_RECENTE'] = [460326]
        comorb_txt['CICR'] = [460328]
        comorb_txt['TFGe'] = [466730]
        comorb_txt['PAS_MEDIDA_1'] = [460372]
        comorb_txt['PAD_MEDIDA_1'] = [460373]
        comorb_txt['PAS_MEDIDA_2'] = [460382]
        comorb_txt['PAD_MEDIDA_2'] = [460383]
        comorb_txt['PAS_MEDIDA_3'] = [460392]
        comorb_txt['PAD_MEDIDA_3'] = [460393]
        comorb_txt['BD_MEDIDA_1'] = [460374] ##true/false (radiobutton)
        comorb_txt['BE_MEDIDA_1'] = [460375] ##true/false (radiobutton)
        comorb_txt['HIPO_1M'] = [460402]
        comorb_txt['HIPO_3M'] = [460403]
        comorb_txt['ORTO_1M'] = [466720]
        comorb_txt['ORTO_3M'] = [466721]
        comorb_txt['FC_MEDIDA_1'] = [460376]
        comorb_txt['FC_MEDIDA_2'] = [460386]
        comorb_txt['FC_MEDIDA_3'] = [460396]
        comorb_txt['DATA_MES'] = [466711]
        comorb_txt['DATA_ANO'] = [466713]
        comorb_txt['TOTAL_PAS'] = [460406]
        comorb_txt['VIGILIA_PAS'] = [460410]
        comorb_txt['SONO_PAS'] = [460414]
        comorb_txt['TOTAL_PAD'] = [460407]
        comorb_txt['VIGILIA_PAD'] = [460411]
        comorb_txt['SONO_PAD'] = [460415]
        comorb_txt['MRPA_PAS'] = [460419]
        comorb_txt['MRPA_PAD'] = [460420]
        comorb_txt['AMAP_PAS'] = [460423]
        comorb_txt['AMAP_PAD'] = [460426]
        comorb_txt['DOSE_TOTAL_DIA_DIURETICO'] = [460457]
        comorb_txt['FUROSEMIDA'] = [460458] #true/false (radiobutton)
        comorb_txt['DOSE_TOTAL_DIA_FUROSEMIDA'] = [460459]
        comorb_txt['DOSE_TOTAL_DIA_IECA_BRA'] = [460479]
        comorb_txt['OUTRO_BRA'] = [466707]
        comorb_txt['DOSE_TOTAL_DIA_ALFA_AGONISTAS_CENTRAIS'] = [460468]
        comorb_txt['DOSE_TOTAL_DIA_POUPADOR_DE_K'] = [460482]
        comorb_txt['DOSE_TOTAL_DIA_BCC'] = [460499]
        comorb_txt['DOSE_TOTAL_DIA_VASODILATADORES_DIRETOS'] = [460502]
        comorb_txt['DOSE_TOTAL_DIA_BB'] = [460518]
        comorb_txt['DOSE_TOTAL_DIA_ALFA_BLOQUEADORES'] = [460521]
        comorb_txt['EMI_ESCOLARIDADE'] = [460270] #valor de radiobutton em 'ESCOLARIDADE'
        comorb_txt['FC_ESCOLARIDADE'] = [460269] #valor de radiobutton em 'ESCOLARIDADE'

    if any(doc in docs for doc in [1068, 880,1041,1045,1066,1072]):
        comorb_txt['DIAGNOSTICO'] = [467198, 390637]

    if any(doc in docs for doc in [951,955,979,988,989,990,1066,1068,1134]):
        comorb_txt['PLANO_TERAPEUTICO'] = [414578]

    if any(doc in docs for doc in [1061,1066]):
        comorb_txt['HPMA'] = [452587]

    if any(doc in docs for doc in [1066,942,943]):
        comorb_txt['FC'] = [445637]
        #comorb_txt['PAS'] = [451234,387696]
        #comorb_txt['PAD'] = [451236,387700]
        #comorb_txt['TEMPERATURA'] = [451238,387694]
        comorb_txt['GLICEMIA'] = [451240]
        comorb_txt['DIURESE'] = [451242]
        #comorb_txt['BH'] = [451244]
        comorb_txt['UNIDADE'] = [453830]

    if any(doc in docs for doc in [1066, 1134]):
        comorb_txt['PREVISAO_DE_ALTA'] = [464678]

    if any(doc in docs for doc in [998]):
        comorb_txt['DESCRICAO_CIRURGIAS'] = [428596]
        comorb_txt['CID'] = [428592]
        comorb_txt['DESCRICAO_OUTROS_RELATORIO_MEDICO'] = [428587]
        comorb_txt['ÚLTIMA CONSULTA MÉDICA'] = [379240]

    if any(doc in docs for doc in [998, 1009]):
        comorb_txt['LISTA_DIAGNOSTICO'] = [387369]

    if any(doc in docs for doc in [897,910,911,912,913,950,951,952,955,957,958,959,960,968,969,971,979,980,988,989,990,998]):
        comorb_txt['DOSE'] = [398638]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,881,884,885,887,888,897,910,911,912,913,929,934,938,941,942,943,954,984,998,1029,1039,1044,1045,1047,1048,1049]):
        comorb_txt['DESCRICAO_IMPRESSAO'] = [390979]
        comorb_txt['LOCAL_ACESSO'] = [411673]

    if any(doc in docs for doc in [951,955,957,958,959,960,968,969,971,979,980,988,990,998,1003,1005,1006,1007,1008,1031,1034,1037,1041,1051]):
        comorb_txt['ADICIONA_MEDICAMENTOS'] = [387425]

    if any(doc in docs for doc in [998,1011,1046]):
        comorb_txt['DESCRICAO_CID'] = [387419]

    if any(doc in docs for doc in [897,910,911,912,913,950,952,989,998]):
        comorb_txt['MEDICAMENTO_EM_USO'] = [398718]

    if any(doc in docs for doc in [957,960,1037,1043]):
        comorb_txt['DATA_DA_ALTA'] = [416886]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,881,887,888,891,914,917,922,924,928,929,933,934,935,941,942,943,954,980,984,1003,1005,1006,1007,1008,1029,1031,1034,1037,1039,1041,1044,1047,1048,1049,1051]):
        comorb_txt['LISTA_MEDICAMENTO'] = [390987]

    if any(doc in docs for doc in [1037]):
        comorb_txt['SETOR_ENCAMINHAMENTO_ALTA'] = [433476]
        comorb_txt['ALTA_LOCAL_TRANSFERENCIA'] = [433480]
        comorb_txt['OBSERVACAO_PARA_ACOMPANHAMENTO_AMBULATORIAL'] = [433488]
        comorb_txt['MEDICACOES_DE_USO_DOMICILIAR'] = [456406]
        #comorb_txt['TRATAMENTO_REALIZADO'] = [456412]
        comorb_txt['LOCAL'] = [456421]
        comorb_txt['OBSERVACOES_PARA_ACOMPANHAMENTO_AMBULATORIAL'] = [456432]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,929,934,941,942,943,951,954,955,971,984,988,990,1029,1039,1044,1046,1047,1048,1049,1066]):
        comorb_txt['TEMPERATURA'] = [451238, 387694]

    if any(doc in docs for doc in [971,974,978,981,1006,1007,1008,1013,1018,1019,1020,1021,1033,1034,1051,943]):
        comorb_txt['DESCREVER_CONDUTA'] = [422471]
        comorb_txt['DESCREVER_MAMARIA'] = [413716]
        comorb_txt['DESCREVER_SAFENA'] = [413717]
        comorb_txt['DESCREVER_RADIAL'] = [413718]
        comorb_txt['DESCREVER_OUTROS_PROCEDIMENTOS'] = [413730]
        comorb_txt['DESCREVER_OUTROS_DISPOSITIVOS'] = [420201]

    if any(doc in docs for doc in [971,980,984,1066]):
        comorb_txt['BH'] = [451244, 419884]

    if any(doc in docs for doc in [951,955,957,958,959,960,968,969,971,979,988,990]):
        comorb_txt['LISTA_MEDICAMENTOS'] = [387377] 

    if any(doc in docs for doc in [971]):
        comorb_txt['CULTURAS'] = [423722]
        comorb_txt['DESCRICAO_TROCA_DE_CATETERES'] = [423728]
        comorb_txt['TIPO_HEMODIALISE'] = [422869]
        comorb_txt['DESCRICAO_DE_TRATAMENTO_ADICIONAIS'] = [423011]

    if any(doc in docs for doc in [971,980]):
        comorb_txt['PAM'] = [423730]

    if any(doc in docs for doc in [971,1045]):
        comorb_txt['DRENOS'] = [423731]
        comorb_txt['DATA_HEMODIALISE'] = [422868]

    if any(doc in docs for doc in [971,980,984]):
        comorb_txt['DEXTRO'] = [419881]

    if any(doc in docs for doc in [969,971,980]):
        comorb_txt['VOLUME_DIURESE'] = [421572]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,897,907,910,911,912,913,915,916,918,929,934,941,942,943,951,954,955,971,982,988,990,1029,1035,1039,1041,1044,1047,1048,1049]):
        comorb_txt['FR'] = [387692]

    if any(doc in docs for doc in [951,955,958,959,968,969,971,988,990]):
        comorb_txt['EXAMES_DE_IMAGEM_DADOS_RELEVANTES'] = [414575]

    if any(doc in docs for doc in [1051]):
        comorb_txt['DESCRICAO_DROGAS_ILICITAS'] = [440003]
        comorb_txt['DESCRICAO_OUTROS_ANTECEDENTES_PATOLOGICOS'] = [440009]
        comorb_txt['DESCRICAO_PREENCHE_CRITERIOS_DE_INCLUSAO_PARA_AMCP_PROTOCOLO'] = [440071]
        comorb_txt['PA_SISTOLICA'] = [440307] # juntar com PAS?
        comorb_txt['PA_DIASTOLICA'] = [440308] # juntar com PAD?

    if any(doc in docs for doc in [33,44,57,535,536,537,566,568,673,729,1034,1051]):
        comorb_txt['DESCRICAO_DA_HISTORIA_CLINICA_DA_DOENCA_ATUAL'] = [58136]

    if any(doc in docs for doc in [913,913,913,913,913,913,913,984,1045,1045,1045,1045,1045,1045,1045,1045,1045,1045,1051,1051]):
        comorb_txt['DESCRICAO_FC'] = [387698] #juntar com FC 1066? 

    if any(doc in docs for doc in [880,897,910,911,912,913,1051,1125]):
        comorb_txt['DS_EXAME_FISICO'] = [393702] 

    if any(doc in docs for doc in [1034,1051]):
        comorb_txt['HIPOSTESES_DIGNOSTICAS_E_DIAGNOSTICOS_DIFERENCIAIS'] = [432963]

    if any(doc in docs for doc in [957,960,1034,1051]):
        comorb_txt['DATA_RETORNO_AMBULATORIAL'] = [417721]

    if any(doc in docs for doc in [998]):
        comorb_txt['PRIMEIRA_CONSULTA'] = [455871]

    if any(doc in docs for doc in [1037,1043]):
        comorb_txt['DIAGNOSTICO_INTERNADO'] = [456395, 456396]
        comorb_txt['DIAGNOSTICO_PREVIO'] = [456402]
        comorb_txt['RESUMO_DA_HISTORIA_CLINICA_E_EVOLUCAO'] = [456404, 433458]
        comorb_txt['EXAMES_COMPLEMENTARES_RELEVANTES'] = [456408]
        comorb_txt['PROCEDIMENTOS_REALIZADOS'] = [456410]
        comorb_txt['DATA_DE_ADMISSAO'] = [433448]
        comorb_txt['TEMPO_DE_PERMANENCIA'] = [433451]
        comorb_txt['DESCRICAO_DO_DIAGNOSTICO_INICIAL'] = [60048]
        comorb_txt['DIAGNOSTICO_DEFINITIVO'] = [433454]
        comorb_txt['OUTROS_DIAGNOSTICOS_E_COMPLICACOES'] = [433456]
        comorb_txt['DESCRICAO_SUMARIA_DA_CIRURGIA'] = [433461]
        comorb_txt['TRATAMENTO_REALIZADO'] = [433463, 456412]

    if any(doc in docs for doc in [1031]):
        comorb_txt['OUTRAS_CIRURGIAS'] = [432704]
        comorb_txt['OUTRAS_MOLESTIAS'] = [432706]
        comorb_txt['RAIO_X'] = [432714]
        comorb_txt['DIAGNOSTICO_CLINICO'] = [432716]
        comorb_txt['PROPOSTA_CIRURGICA'] = [432734]
        comorb_txt['DIAGNOSTICO_HEMODINAMICO'] = [432731]

    if any(doc in docs for doc in [980,1031]):
        comorb_txt['RESUMO_DO_QUADRO_CLINICO'] = [423922]

    if any(doc in docs for doc in [876,980,1031]):
        comorb_txt['DESCRICAO_ECG'] = [394448]

    if any(doc in docs for doc in [1107]):
        comorb_txt['EXAMES_BIOQUIMICOS'] = [459369]
        comorb_txt['EXAMES_COMPLEMENTARES_CIRURGIA'] = [459361]
        comorb_txt['EXAME_FISICO_CIRURGIA'] = [459353]
        comorb_txt['CONDUTA_CIRURGIA'] = [459373]
        comorb_txt['OUTRAS_ORIENTACOES'] = [459402]
        comorb_txt['ANEXO_I'] = [459408]
        comorb_txt['ANEXO_II'] = [459578]
        comorb_txt['ANEXO_II_II'] = [459584]
        comorb_txt['ANEXO_III'] = [459586]
        comorb_txt['ANEXO_IV'] = [459591]

    if any(doc in docs for doc in [1094,1107]):
        comorb_txt['AVALIACAO_CLINICA'] = [457203]
        comorb_txt['PROCEDIMENTO'] = [457196]

    if any(doc in docs for doc in [1072,1107]):
        comorb_txt['ANTECEDENTES_CIRURGICOS'] = [453171]

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,897,910,911,912,913,929,934,941,942,943,954,982,1029,1039,1044,1047,1048,1049,1066]):
        comorb_txt['PAS'] = [387696, 451234] # 451234 -> 1066

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,897,910,911,912,913,929,934,941,942,943,954,982,1029,1039,1044,1045,1047,1048,1049,1066]):
        comorb_txt['PAD'] = [387700, 451236]    #451236 -> 1066  

    if any(doc in docs for doc in [887,888,929,934,941,942,943,954,1029,1039,1044,1047,1048,1049]):
        comorb_txt['CIRCUNFERENCIA_ABDOMINAL'] = [387689] 

    if any(doc in docs for doc in [881,887,888,891,914,917,922,924,928,929,933,934,935,941,942,943,954,984,1029,1039,1044,1047,1048,1049]):
        comorb_txt['DESCRICAO_MEDICACAO'] = [391031]    

    if any(doc in docs for doc in [929,933,934,935,941,942,943,950,952,954,984,989,1029,1039,1044,1047,1048,1049]):
        comorb_txt['ADICIONAR_MEDICACAO'] = [391030]    

    if any(doc in docs for doc in [929,933,934,935,941,942,943,954,1011,1012,1016,1029,1039,1041,1044,1047,1048,1049,1068,1110]):
        comorb_txt['IMC'] = [387404, 465248]    #465248 -> 1068,1110   

    if any(doc in docs for doc in [934,935,941,942,943,950,951,952,954,955,957,958,959,960,968,969,979,980,988,989,990,1012,1016,1029,1039,1041,1042,1044,1047,1048,1049,1068,1110]):
        comorb_txt['PESO'] = [387400, 460266]   #460266 -> 1068,1110

    if any(doc in docs for doc in [856,857,858,859,876,877,879,880,881,885,887,888,897,910,911,912,913,929,934,941,942,943,954,1003,1005,1006,1007,1008,1029,1039,1044,1047,1048,1049]):
        comorb_txt['OBSERVACAO_QUEIXAS'] = [390189]   

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,929,934,941,942,943,954,984,1029,1039,1044,1047,1048,1049]):
        comorb_txt['DESCRICAO_PATOLOGIA'] = [390914]   

    if any(doc in docs for doc in [856,857,858,859,876,877,879,887,888,897,910,911,912,913,914,917,922,924,928,929,933,934,935,941,942,943,950,951,952,954,955,957,958,959,960,968,969,979,980,988,989,990,1011,1012,1016,1029,1039,1041,1044,1047,1048,1049,1068,1072,1110]):
        comorb_txt['ALTURA'] = [387402, 460265] #460265 -> 1068,1110

    if any(doc in docs for doc in [931,1043]):
        comorb_txt['DATA_DO_OBITO'] = [406546]

    if any(doc in docs for doc in [1043]):
        comorb_txt['DESCRICAO_CAUSA_DA_MORTE'] = [434577]

    if any(doc in docs for doc in [943]):
        comorb_txt['DESCRICAO_SAFENA'] = [413717]
        comorb_txt['DESCRICAO_RADIAL'] = [413718]
        comorb_txt['DESCRICAO_MAMARIA'] = [413716]

    if any(doc in docs for doc in [938,943,954,1073]):
        comorb_txt['DT_CIRURGIA'] = [390627]

    if any(doc in docs for doc in [943,950,952,957,960,1072]):
        comorb_txt['OUTROS_PROCEDIMENTOS'] = [413730]
        comorb_txt['CIRURGIA'] = [390638]

    if any(doc in docs for doc in [1157,1158]):
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_LOCAL_SANGRAMENTO'] = [492547]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_PROCEDIMENTO_1_RE_INTERVENCAO_OU_CIRURGIA_NAO_PLANEJADA'] = [492608]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_DATA_1_RE_INTERVENCAO_OU_CIRURGIA_NAO_PLANEJADA'] = [492601]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_MOTIVO_1_RE_INTERVENCAO_OU_CIRURGIA_NAO_PLANEJADA'] = [492614]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_DATA_AVC'] = [492532]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_DATA_SAGRAMENTO_ASSOCIADO_A_QUEDA_DE_HB'] = [492549]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_DATA_IMPLANTE_DE_NOVO_MARCA_PASSO_PERMANENTE'] = [492575]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_MECANISMO'] = [492595]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_DATA_CHOQUE_SEPTICO'] = [492646]
        comorb_txt['OUTRAS_COMPLICACOES_RELEVANTES'] = [492706]

    if any(doc in docs for doc in [1157]):
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_DATA_2_RE_INTERVENCAO_CIRURGIA_NAO_PLANEJADA'] = [492604]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_MOTIVO_2_RE_INTERVENCAO_CIRURGIA_NAO_PLANEJADA'] = [492616]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_PROCEDIMENTO_2_RE_INTERVENCAO_CIRURGIA_NAO_PLANEJADA'] = [492610]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_DATA_3_RE_INTERVENCAO_CIRURGIA_NAO_PLANEJADA'] = [492606]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_MOTIVO_3_RE_INTERVENCAO_CIRURGIA_NAO_PLANEJADA'] = [492618]
        comorb_txt['COMPLICACOES_DURANTE_RECUPERACAO_PROCEDIMENTO_3_RE_INTERVENCAO_CIRURGIA_NAO_PLANEJADA'] = [492612]
        comorb_txt['DATA_DA_CIRURGIA'] = [492514]
        comorb_txt['DATA_ADMISSAO_HOSPITAL'] = [492512]
        comorb_txt['DATA_ALTA_UTI'] = [492516]
        comorb_txt['READMISSAO_UTI_MOTIVO'] = [492522]
        comorb_txt['DATA_ALTA_HOSPITALAR'] = [492524]
        comorb_txt['TEMPO_TOTAL_PERMANENCIA_ENFERMARIA_DIAS'] = [492526]
        comorb_txt['STATUS_VITAL_MOTIVO_DA_TRANSFERENCIA'] = [492720]
        comorb_txt['STATUS_VITAL_OBITO_DATA'] = [492724]
        comorb_txt['STATUS_VITAL_OBITO_MOTIVO'] = [492731]
        comorb_txt['STATUS_VITAL_RESIDENTE'] = [492735]
        comorb_txt['ALTA_DISCUTIDA_COM_STAFF'] = [492733]

    if any(doc in docs for doc in [1158]):
        comorb_txt['METODO_DE_CONTATO_PARENTESCO'] = [493069]
        comorb_txt['METODO_DE_CONTATO_NOME_LOCAL'] = [493071]
        comorb_txt['METODO_DE_CONTATO_OUTRO'] = [493075]
        comorb_txt['CAUSA_PRESUMIDA_DO_OBITO'] = [493081]
        comorb_txt['LOCAL_DO_OBITO_HOSPITAL_NOME'] = [493087]
        comorb_txt['NUMERO_DE_IDAS_AO_PS_POS_ALTA'] = [493096]
        comorb_txt['NUMERO_DE_INTERNACOES_POS_ALTA'] = [493102]
        comorb_txt['MOTIVO_DA_REINTERNACAO_POS_ALTA'] = [493104]
        comorb_txt['DATA_LESAO_RENAL_AGUDA'] = [493106]
        comorb_txt['DATA_INFARTO_DO_MIOCARDICO_POS_PROCEDIMENTO'] = [493108]
        comorb_txt['DATA_DISFUNCAO_VALVAR_POS_ALTA'] = [493114]

    if any(doc in docs for doc in [914,917,922,924,928,933,935,1041]):
        comorb_txt['QUANTIDADE_CIGARRO'] = [402667]

    if any(doc in docs for doc in [1041]):
        comorb_txt['DESCRICAO_CF'] = [434303]
        comorb_txt['POSTURA'] = [434335]
        comorb_txt['ALTERACOES_OSTEOARTICULARES'] = [434338]
        comorb_txt['EXTABAGISTA_HA_QUANTOS_ANOS'] = [434293]
        comorb_txt['TEMPO_DE_ATIVIDADE'] = [434300]
        comorb_txt['INTERVENCAO'] = [434316]
        comorb_txt['INTERNACAO_E_COMPLICACOES'] = [434318]
        comorb_txt['QUEIXA_PRINCIPAL_EXAME_FISICO'] = [434332]
        comorb_txt['PRESCRICAO_DE_EXERCICIO'] = [434344]
        comorb_txt['FISIOTERAPEUTA_RESPONSAVEL'] = [434346]
        comorb_txt['DESCRICAO_IAM'] = [434310]
        comorb_txt['AUSCULTA_PULMONAR'] = [419912]
        comorb_txt['OUTRAS_ALTERACOES_EXAME_FISICO'] = [434340]
        comorb_txt['QQV'] = [434342]

    if any(doc in docs for doc in [895,900,909,984,1041,1046,1051]):
        comorb_txt['PA'] = [398291]

    if any(doc in docs for doc in [895,900,906]):
        comorb_txt['LISTA_ESCALA_BORG'] = [398295]

    if any(doc in docs for doc in [895,897,900,909,910,911,912,913,984,1045,1051]):
        comorb_txt['DESCRICAO_FC'] = [387698]

    if any(doc in docs for doc in [895,900]):
        comorb_txt['RETORNOU_AO_TESTE'] = [398399]

    if any(doc in docs for doc in [902,903,906]):
        comorb_txt['OBSERVACAO_INTERRUPCAO_DO_TESTE'] = [398818]

    if any(doc in docs for doc in [950,951,952,955,957,958,959,960,968,969,979,988,989,990,1009,1041,1046,1151]):
        comorb_txt['ADICIONAR'] = [390657]

    if any(doc in docs for doc in [895]):
        comorb_txt['LISTA_GRAU_DE_DESEMPENHO'] = [398283]
        comorb_txt['DESCRICAO_PAROU_POR_QUANTO_TEMPO'] = [398340]
        comorb_txt['DESCRICAO_DISTANCIA_PERCORRIDA'] = [398281]

    if any(doc in docs for doc in [895,900,1041]):
        comorb_txt['DESCRICAO_SPO2'] = [398289]

    if any(doc in docs for doc in [529,1041,1072]):
        comorb_txt['SE_O_PACIENTE_TEM_OUTROS_ANTECEDENTES_PESSOAIS'] = [313568]

    if any(doc in docs for doc in [1013,1041]):
        comorb_txt['TB_DIAGNOSTICO'] = [390659]

    if any(doc in docs for doc in [900]):
        comorb_txt['N_DE_DEGRAUS'] = [398554]
        comorb_txt['TOTAL_DE_DEGRAUS'] = [398546]

    if any(doc in docs for doc in [901]):
        comorb_txt['TESTE_HAND_GRIP'] = [398548]
        comorb_txt['OBSERVACAO_TESTE_HAND_GRIP'] = [398559]

    if any(doc in docs for doc in [902]):
        comorb_txt['CARGA_MAXIMA_SUPERIOR'] = [398590]
        comorb_txt['CARGA_MAXIMA_INFERIOR'] = [398605]
        comorb_txt['CARGA_TREINAMENTO_SUPERIOR'] = [398591]
        comorb_txt['CARGA_TREINAMENTO_INFERIOR'] = [398700]

    if any(doc in docs for doc in [903]):
        comorb_txt['CVF_PREDITO'] = [398767]
        comorb_txt['VEF_ABSOLUTO'] = [398720]
        comorb_txt['VEF_PREDITO'] = [398723]
        comorb_txt['PFE_PREDITO'] = [398773]
        comorb_txt['FEF_PREDITO'] = [398775]
        comorb_txt['CVF_ABSOLUTO'] = [398724]
        comorb_txt['FEF_ABSOLUTO'] = [398774]
        comorb_txt['VEF_CVF_ABSOLUTO'] = [398769]
        comorb_txt['VEF_CVF_PREDITO'] = [398770]
        comorb_txt['PFE_ABSOLUTO'] = [398772]

    if any(doc in docs for doc in [904]):
        comorb_txt['VALOR_OBTIDO'] = [398874]
        comorb_txt['POR_CENTO_DO_PREDITO'] = [398882]
        comorb_txt['DESCRICAO_PRESCRICAO'] = [398916]
        comorb_txt['OBSERVACAO_DESCRICAO_MOTIVO'] = [398946]

    if any(doc in docs for doc in [905]):
        comorb_txt['LISTA_HIPERTONIA_TONUS_MUSCULAR'] = [398973]

    if any(doc in docs for doc in [906]):
        comorb_txt['PA_FINAL'] = [399284]
        comorb_txt['FC_FINAL'] = [399277]
        comorb_txt['SPO2_FINAL'] = [399279]
        comorb_txt['NUMERO_DE_REPETICOES'] = [442608]

    if any(doc in docs for doc in [906,1138]):
        comorb_txt['FC_RECUPERACAO'] = [399294, 468998]     # 399294 -> 906
        comorb_txt['PA_RECUPERACAO'] = [399307, 469000]     # 399307 -> 906
        comorb_txt['SPO2_RECUPERACAO'] = [399299, 469002]   # 399299 -> 906

    if any(doc in docs for doc in [908]):
        comorb_txt['APRESENTA_SIMETRIA_DA_CABECA_EM_RELACAO_AO_TRONCO'] = [399373]
        comorb_txt['EM_DD_TRAZ_A_CABECA_EM_FLEXAO_QUANDO_PUXADA_PELOS_MMSS'] = [399507]
        comorb_txt['CONTROLE_DE_CERVICAL'] = [399523]
        comorb_txt['CONTROLE_TRONCO'] = [399526]
        comorb_txt['CAMINHA'] = [399556]
        comorb_txt['MANTEM_MMSS_E_MMII_EM_EXTENSAO_DE_FORMA_SIMETRICA'] = [399496]
        comorb_txt['APRESENTA_EXTENSAO_DE_TRONCO_E_APOIA_OS_MMSS'] = [399520]
        comorb_txt['MANTEM_A_POSTURA_COM_APOIO_OU_AUXILIO'] = [399547]
        comorb_txt['MANUSEIA_OBJETOS_EM_PE'] = [399552]
        comorb_txt['DESCRICAO_OBJETIVO_MOTOR'] = [399561]
        comorb_txt['MANTEM_OS_MMSS_EM_FLEXAO_DE_FORMA_SIMETRICA'] = [399499]
        comorb_txt['PASSA_PARA_SENTADO_SEM_AUXILIO'] = [399514]
        comorb_txt['APRESENTA_EXTENSAO_CERVICAL_E_MOVIMENTOS_LATRAIS_DA_CABECA'] = [399518]
        comorb_txt['MANUSEIA_OBJETOS'] = [399532]
        comorb_txt['APRESENTA_REACOES_DE_EQUILIBRIO'] = [399535]
        comorb_txt['FICA_EM_PE_SOZINHO'] = [399549]
        comorb_txt['ROLA_PARA_DECUBITO_VENTRAL'] = [399512]
        comorb_txt['CONTROLA_CABECA_E_TRONCO_APOIANDO_OS_MMSS'] = [399529]
        comorb_txt['APRESENTA_REACOES_QUAL'] = [399537]
        comorb_txt['PASSA_PARA_GATO'] = [399539]
        comorb_txt['PASSA_PARA_EM_PE'] = [399542]

    if any(doc in docs for doc in [906,1136,1137,1138,1140,1141,1142,1144,1163]):
        comorb_txt['FC_REPOUSO'] = [399256, 468280]     # 399256 -> 906
        comorb_txt['PA_REPOUSO'] = [399262, 468282]     # 399262 -> 906
        comorb_txt['SPO2_REPOUSO'] = [399260, 468321]   # 399260 -> 906

    if any(doc in docs for doc in [1136,1137,1138,1140,1141,1142,1144,1163]):
        comorb_txt['DISTANCIA_PERCORRIDA_TESTE_CAMINHADA'] = [468628]

    if any(doc in docs for doc in [1136,1137,1138,1140,1141]):
        comorb_txt['PESO_TESTE_CAMINHADA'] = [468936]

    if any(doc in docs for doc in [1136,1137,1138,1140,1141,1142,1163]):
        comorb_txt['ANTECEDENTES_PESSOAIS_EVOLUCAO'] = [468247]
    
    if any(doc in docs for doc in [1072]):
        comorb_txt['ALERGIA'] = [390634]

    if any(doc in docs for doc in [1138]):
        comorb_txt['SESSAO'] = [468944]
        comorb_txt['GRUPO'] = [468946]
        comorb_txt['FC_TREINAMENTO'] = [468949]
        comorb_txt['REPOUSO_GLICEMIA_INICIAL'] = [468958]
        comorb_txt['REPOUSO_GLICEMIA_FINAL'] = [468960]
        comorb_txt['DESCRICAO_NOVA_DOR'] = [468966]
        comorb_txt['DESCRICAO_RISCO_DE_QUEDA'] = [468972]
        comorb_txt['DESCRICAO_OXIGENOTERAPIA'] = [468976]
        comorb_txt['DESCRICAO_TREINAMENTO_MUSCULAR_RESPIRATORIO_SIM'] = [468984]
        comorb_txt['DESCRICAO_TREINAMENTO_MUSCULAR_RESPIRATORIO_NAO'] = [468986]
        comorb_txt['MMSS'] = [468988]
        comorb_txt['MMII'] = [468990]
        comorb_txt['CARGA_CICLOERGONOMETRO'] = [468992]
        comorb_txt['INTERROMPEU_EXERCICIO_DEVIDO_OUTROS'] = [469029]
        comorb_txt['OUTRAS_INFORMACOES'] = [469031]

    if any(doc in docs for doc in [1138,1142,1144,1163]):
        comorb_txt['DATA_DA_AVALIACAO'] = [469033]
        comorb_txt['IDADE_NA_AVALIACAO'] = [469035]
        comorb_txt['PESO_EXAME_FISICO'] = [469048]
        comorb_txt['ALTURA_EXAME_FISICO'] = [469050]
        comorb_txt['IMC_EXAME_FISICO'] = [469052]
        comorb_txt['FC_REPOUSO_BPM'] = [469071]
        comorb_txt['SPO2_REPOUSO_EVOLUCAO'] = [469073]
        comorb_txt['PA_REPOUSO_EVOLUCAO'] = [469075]
        comorb_txt['FADIGA_MMII_BORG'] = [469077]
        comorb_txt['TESTE_SENTAR_LEVANTAR_DISPNEIA_BORG'] = [469081]
        comorb_txt['TESTE_CAMINHADA_SEIS_MINUTOS_FC'] = [471011]
        comorb_txt['TESTE_CAMINHADA_SEIS_MINUTOS_SPO2'] = [471013]
        comorb_txt['TESTE_CAMINHADA_SEIS_MINUTOS_PA'] = [471015]
        comorb_txt['TESTE_CAMINHADA_SEIS_MINUTOS_FADIGA_MMII'] = [471017]
        comorb_txt['TESTE_CAMINHADA_RECUPERACAO_2_MINUTOS_FC'] = [471020]
        comorb_txt['TESTE_CAMINHADA_RECUPERACAO_2_MINUTOS_SPO2'] = [471022]
        comorb_txt['TESTE_CAMINHADA_RECUPERACAO_2_MINUTOS_PA'] = [471024]
        comorb_txt['TESTE_CAMINHADA_RECUPERACAO_2_MINUTOS_FADIGA_MMII'] = [471026]
        comorb_txt['TESTE_CAMINHADA_RECUPERACAO_2_MINUTOS_DISPNEIA'] = [471028]
        comorb_txt['TESTE_CAMINHADA_PAROU_DURANTE_O_TESTE'] = [471030] #Botão. Retorna true/false.
        comorb_txt['TESTE_CAMINHADA_NAO_PAROU_DURANTE_O_TESTE'] = [471032] #Botão. Retorna true/false.
        comorb_txt['TESTE_CAMINHADA_POR_QUANTO_TEMPO_PAROU'] = [471034]
        comorb_txt['TESTE_CAMINHADA_SINTOMAS_DURANTE_OU_POS_TESTE'] = [471036]
        comorb_txt['TESTE_CAMINHADA_NIVEL_DE_CAMINHADA'] = [471039]
        comorb_txt['TESTE_CAMINHADA_PREDITO_METROS'] = [471042]
        comorb_txt['TESTE_CAMINHADA_PREDITO_PORCENTAGEM'] = [471044]
        comorb_txt['TESTE_CAMINHADA_PICO_VO2_MAX'] = [471046]

    if any(doc in docs for doc in [1138,1142,1163]):
        comorb_txt['DIAGNOSTICO_MEDICO'] = [469039]
        comorb_txt['MEDICAMENTOS_EVOLUCAO'] = [469044]
        comorb_txt['HAND_GRIP_PRIMEIRA_MEDIDA'] = [469063]
        comorb_txt['HAND_GRIP_SEGUNDA_MEDIDA'] = [469065]
        comorb_txt['HAND_GRIP_TERCEIRA_MEDIDA'] = [469067]
        comorb_txt['HAND_GRIP_MAIOR_MEDIDA'] = [469069]
        comorb_txt['TESTE_RM_CARGA_MAXIMA_MEMBRO_SUPERIOR'] = [470973]
        comorb_txt['TESTE_RM_CARGA_TREINAMENTO_40_MEMBRO_SUPERIOR'] = [470975]
        comorb_txt['TESTE_RM_CARGA_TREINAMENTO_60_MEMBRO_SUPERIOR'] = [470977]
        comorb_txt['TESTE_RM_CARGA_MAXIMA_MEMBRO_INFERIOR'] = [470985]
        comorb_txt['TESTE_RM_CARGA_TREINAMENTO_40_MEMBRO_INFERIOR'] = [470987]
        comorb_txt['TESTE_RM_CARGA_TREINAMENTO_60_MEMBRO_INFERIOR'] = [471649]
        comorb_txt['PES_UNIDOS_EM_PARALELO'] = [471048]
        comorb_txt['PES_UNIDOS_EM_PARALELO_SCORE'] = [471050]
        comorb_txt['PE_PARCIALMENTE_A_FRENTE'] = [471054]
        comorb_txt['PE_PARCIALMENTE_A_FRENTE_SCORE'] = [471056]
        comorb_txt['PE_PARCILAMENTE_A_FRENTE_NAO_REALIZOU'] = [471058] #Botão. Retorna true/false.
        comorb_txt['PE_A_FRENTE'] = [471060]
        comorb_txt['PE_A_FRENTE_SCORE'] = [471062]
        comorb_txt['PE_A_FRENTE_NAO_REALIZOU'] = [471064] #Botão. Retorna true/false.
        comorb_txt['TESTE_DE_VELOCIDADE_DE_MARCHA_PRIMEIRA_TENTATIVA'] = [471066]
        comorb_txt['TESTE_DE_VELOCIDADE_DE_MARCHA_SEGUNDA_TENTATIVA'] = [471068]
        comorb_txt['TESTE_DE_VELOCIDADE_DE_MARCHA_PRIMEIRA_TENTATIVA_SCORE'] = [471070]
        comorb_txt['TESTE_DE_VELOCIDADE_DE_MARCHA_SEGUNDA_TENTATIVA_SCORE'] = [471072]
        comorb_txt['TESTE_DE_VELOCIDADE_DE_MARCHA_SEGUNDA_TENTATIVA_NAO_REALIZOU'] = [471076] #Botão. Retorna true/false.
        comorb_txt['TESTE_DE_VELOCIDADE_DE_MARCHA_PONTUACAO'] = [471078]
        comorb_txt['TESTE_DE_CADEIRA_TEMPO_SEGUNDOS'] = [471080]
        comorb_txt['TESTE_DE_CADEIRA_SCORE'] = [471082]
        comorb_txt['TESTE_DE_CADEIRA_PONTUACAO_TOTAL'] = [471084]
        comorb_txt['TUG_PRIMEIRO'] = [471086]
        comorb_txt['TUG_SEGUNDO'] = [471088]
        comorb_txt['TUG_TERCEIRO'] = [471090]
        comorb_txt['TUG_PONTUACAO'] = [471092]
        comorb_txt['SUA_SAUDE_HOJE_ESTA'] = [471129]

    if any(doc in docs for doc in [1138,1142]):
        comorb_txt['PROFISSAO'] = [469037]
        comorb_txt['QUEIXA_PRINCIPAL'] = [469041]
        comorb_txt['EXAMES_COMPLEMENTARES_EVOLUCAO'] = [469046]
        comorb_txt['TESTE_SENTAR_LEVANTAR_FC_PRIMEIRO_MINUTO'] = [469079]
        comorb_txt['TESTE_SENTAR_LEVANTAR_SPO2_PRIMEIRO_MINUTO'] = [469083]
        comorb_txt['TESTE_SENTAR_LEVANTAR_PA_PRIMEIRO_MINUTO'] = [469085]
        comorb_txt['TESTE_SENTAR_LEVANTAR_FADIGA_MMII_PRIMEIRO_MINUTO'] = [469087]
        comorb_txt['TESTE_SENTAR_LEVANTAR_DISPNEIA_PRIMEIRO_MINUTO'] = [469089]
        comorb_txt['TESTE_SENTAR_LEVANTAR_SPO2_RECUPERACAO_DOIS_MINUTOS'] = [469093]
        comorb_txt['TESTE_SENTAR_LEVANTAR_PA_RECUPERACAO_DOIS_MINUTOS'] = [469095]
        comorb_txt['TESTE_SENTAR_LEVANTAR_FADIGA_MMII_RECUPERACAO_DOIS_MINUTOS'] = [469097]
        comorb_txt['TESTE_SENTAR_LEVANTAR_DISPNEIA_RECUPERACAO_DOIS_MINUTOS'] = [469099]
        comorb_txt['TESTE_SENTAR_LEVANTAR_N_TOTAL_REPETICOES_DOIS_MINUTOS'] = [469101]
        comorb_txt['PIMAX_MELHOR'] = [470989]
        comorb_txt['PIMAX_PREVISTA'] = [470991]
        comorb_txt['PIMAX_30'] = [470993]
        comorb_txt['PIMAX_40'] = [470995]
        comorb_txt['PIMAX_50'] = [470997]
        comorb_txt['MRC_SELECIONAR_LISTA_ABAIXO'] = [471131]
        comorb_txt['CLASSIFICACAO_FUNCIONAL_NYHA'] = [471163]
        comorb_txt['CLASSIFICACAO_ANGINA'] = [471165]

    if any(doc in docs for doc in [1138,1142,1144]):
        comorb_txt['FR_EXAME_FISICO'] = [469056]
        comorb_txt['TESTE_CAMINHADA_FC'] = [469091]
        comorb_txt['TESTE_CAMINHADA_DOIS_MINUTOS_SPO2'] = [471005]
        comorb_txt['TESTE_CAMINHADA_QUATRO_MINUTOS_SPO2'] = [471007]
        comorb_txt['TESTE_CAMINHADA_QUATRO_MINUTOS_FC'] = [471009]
    
    if any(doc in docs for doc in [1043]):
        comorb_txt['CAUSA_DA_MORTE'] = [434577]

    if any(doc in docs for doc in [931,1043]):
        comorb_txt['DATA_OBITO'] = [406546]

    missing_values = None
    missing_values = [
        metadado 
        for metadado in df_comorbidades_txt['CD_METADADO'].unique()
        if metadado not in (
            set(item for sublist in comorb_txt.values() for item in sublist)
        )
    ]
    if missing_values:
        raise RuntimeError(f"These txt metadata codes are missing: {sorted(missing_values)}")

    df_comorbidades_txt = df_comorbidades_txt[~df_comorbidades_txt['CD_METADADO'].isin(['464681', '453835', '453834'])]

    df_comorbidades_txt['CD_METADADO'] = df_comorbidades_txt['CD_METADADO'].map({v: k for k, vals in comorb_txt.items() for v in vals})

    #Para casos em que há metadados duplicados para campos diferentes, diferenciar por NK_CD_CAMPO
    if df_comorbidades_txt[['CD_PACIENTE', 'CD_ATENDIMENTO', 'DH_DOCUMENTO', 'CD_METADADO']].duplicated().sum() > 0:
        comorb_cd_campo = {}
        comorb_cd_campo['DESCRICAO_FC_1'] = [444312, 398464]
        comorb_cd_campo['DESCRICAO_FC_2'] = [444317, 398469]
        comorb_cd_campo['DESCRICAO_FC_3'] = [444318, 398470]
        comorb_cd_campo['DESCRICAO_FC_4'] = [444319, 398471]
        comorb_cd_campo['DESCRICAO_FC_5'] = [444328, 398480]
        comorb_cd_campo['DESCRICAO_FC_6'] = [398625]
        comorb_cd_campo['DESCRICAO_SPO2_1'] = [444313, 398465]
        comorb_cd_campo['DESCRICAO_SPO2_2'] = [444320, 398472]
        comorb_cd_campo['DESCRICAO_SPO2_3'] = [444321, 398473]
        comorb_cd_campo['DESCRICAO_SPO2_4'] = [444322, 398474]
        comorb_cd_campo['DESCRICAO_SPO2_5'] = [444329, 398481]
        comorb_cd_campo['DESCRICAO_SPO2_6'] = [398631]
        comorb_cd_campo['LISTA_ESCALA_BORG_1'] = [444315, 442551, 398467]
        comorb_cd_campo['LISTA_ESCALA_BORG_2'] = [444316, 442552, 398468]
        comorb_cd_campo['LISTA_ESCALA_BORG_3'] = [444326, 442557, 398478]
        comorb_cd_campo['LISTA_ESCALA_BORG_4'] = [444327, 442558, 398479]
        comorb_cd_campo['LISTA_ESCALA_BORG_5'] = [444330, 442559, 398482]
        comorb_cd_campo['LISTA_ESCALA_BORG_6'] = [444333, 442560, 398485]
        comorb_cd_campo['PA_2'] = [444314, 398466]
        comorb_cd_campo['PA_3'] = [444323, 398475]
        comorb_cd_campo['PA_4'] = [444324, 398476]
        comorb_cd_campo['PA_5'] = [444325, 398477]
        comorb_cd_campo['PA_6'] = [444331, 398483]
        comorb_cd_campo['PA_7'] = [444332, 398484]
        comorb_cd_campo['TESTE_HAND_GRIP_1'] = [398633]
        comorb_cd_campo['TESTE_HAND_GRIP_2'] = [398634]
        comorb_cd_campo['TESTE_HAND_GRIP_3'] = [398635]
        comorb_cd_campo['POR_CENTO_DO_PREDITO_2'] = [442678]
        comorb_cd_campo['VALOR_OBTIDO_1'] = [442675]
        comorb_cd_campo['VALOR_OBTIDO_2'] = [442677]
        comorb_cd_campo['PA_FINAL_1'] = [442555]
        comorb_cd_campo['PA_FINAL_2'] = [442556]
        comorb_cd_campo['PA_REPOUSO'] = [442549]
        comorb_cd_campo['PA_REPOUSO_1'] = [442550]
        comorb_cd_campo['PA_RECUPERACAO_1'] = [442563]
        comorb_cd_campo['PA_RECUPERACAO_2'] = [442564]
        comorb_cd_campo['N_DE_DEGRAUS_1'] = [398556]
        comorb_cd_campo['N_DE_DEGRAUS_2'] = [398581]
        comorb_cd_campo['N_DE_DEGRAUS_3'] = [398613]
        comorb_cd_campo['N_DE_DEGRAUS_4'] = [398637]    
        comorb_cd_campo['TESTE_SENTAR_LEVANTAR_REPOUSO_FC'] = [469072,473198,477244,479896,486274,490057]
        comorb_cd_campo['TESTE_CAMINHADA_REPOUSO_FC'] = [470999,473228,477274,479926,486304,490087,476584,476753,479710,482136,496654,498426,498690,511522]
        comorb_cd_campo['TESTE_SENTAR_LEVANTAR_REPOUSO_SPO2'] = [469074,473199,477245,479897,486275,490058]
        comorb_cd_campo['TESTE_CAMINHADA_REPOUSO_SPO2'] = [4471000,473229,476585,476754,477275,479711,479927,482137,486305,490088,496655,498427,498691,511523]
        comorb_cd_campo['TESTE_SENTAR_LEVANTAR_REPOUSO_PA'] = [469076,473200,477246,479898,486276,490059]
        comorb_cd_campo['TESTE_CAMINHADA_REPOUSO_PA'] = [471001,473230,477276,479928,486306,490089,476586,476755,479712,482138,496656,498428,498692,511524]
        comorb_cd_campo['TESTE_SENTAR_LEVANTAR_REPOUSO_FADIGA_MMII_BORG'] = [469078,473201,477247,479899,486277,490060]
        comorb_cd_campo['TESTE_CAMINHADA_REPOUSO_FADIGA_MMII_BORG'] = [471002,473231,477277,479929,486307,490090,476587,476756,479713,482139,496657,498429,498693,511525]
        comorb_cd_campo['TESTE_SENTAR_LEVANTAR_REPOUSO_DISPNEIA_BORG'] = [469082,473203,477249,479901,486279,490062]
        comorb_cd_campo['TESTE_CAMINHADA_REPOUSO_DISPNEIA_BORG'] = [471003,473232,476588,476757,477278,479714,479930,482140,486308,490091,496658,498430,498694,511526]
        comorb_cd_campo['TESTE_CAMINHADA_SEIS_MINUTOS_DISPNEIA_BORG'] = [471019,473241,476597,476766,477287,479723,479939,482149,486317,490100,496667,498435,498699,511531]
        comorb_cd_campo['TESTE_CAMINHADA_DOIS_MINUTOS_FC'] = [471004,473233,476589,476758,477279,479715,479931,482141,486309,490092]
        comorb_cd_campo['TESTE_SENTAR_LEVANTAR_RECUPERACAO_DOIS_MINUTOS_FC'] = [469092,473208,477254,479906,486284,490067]

        nk_to_nome = {}
        for nome, codigos in comorb_cd_campo.items():
            for c in codigos:
                if c in nk_to_nome and nk_to_nome[c] != nome:
                    raise ValueError(f"NK_CD_CAMPO {c} aparece em mais de uma key: {nk_to_nome[c]} e {nome}")
                nk_to_nome[c] = nome

        # Atualiza CD_METADADO baseado em NK_CD_CAMPO (mantém o original quando não houver mapeamento)
        df_comorbidades_txt["CD_METADADO"] = (
            df_comorbidades_txt["NK_CD_CAMPO"].map(nk_to_nome)
            .fillna(df_comorbidades_txt["CD_METADADO"])
        )
        
    df_comorbidades_txt = df_comorbidades_txt.pivot(
        index=['CD_PACIENTE', 'CD_ATENDIMENTO', 'DH_DOCUMENTO'],
        columns='CD_METADADO',
        values='DS_RESPOSTA',  
    ).reset_index()

    df_comorbs = pd.merge(df_comorbidades, df_comorbidades_txt, on=['CD_PACIENTE', 'CD_ATENDIMENTO', 'DH_DOCUMENTO'], how='outer')

    if ('EMI_ESCOLARIDADE' in df_comorbs.columns) and ('ESCOLARIDADE' in df_comorbs.columns):
        df_comorbs['ESCOLARIDADE'].loc[df_comorbs['EMI_ESCOLARIDADE'] == 'true'] = 'EMI'
        df_comorbs.drop(columns=['EMI_ESCOLARIDADE'], inplace=True)

    if ('FC_ESCOLARIDADE' in df_comorbs.columns) and ('ESCOLARIDADE' in df_comorbs.columns):
        df_comorbs['ESCOLARIDADE'].loc[df_comorbs['FC_ESCOLARIDADE'] == 'true'] = 'FC'
        df_comorbs.drop(columns=['FC_ESCOLARIDADE'], inplace=True)

    if 'FUROSEMIDA' in df_comorbs.columns:
        df_comorbs['FUROSEMIDA'].loc[df_comorbs['FUROSEMIDA'] == 'false'] = 'NAO'
        df_comorbs['FUROSEMIDA'].loc[df_comorbs['FUROSEMIDA'] == 'true'] = 'SIM'

    if ('BD_MEDIDA_1' in df_comorbs.columns) or ('BE_MEDIDA_1' in df_comorbs.columns):
        df_comorbs['PA_CONSULTORIO_MEDIDA_1'] = None
        df_comorbs['PA_CONSULTORIO_MEDIDA_1'].loc[df_comorbs['BD_MEDIDA_1'] == 'true'] = 'BD'
        df_comorbs['PA_CONSULTORIO_MEDIDA_1'].loc[df_comorbs['BE_MEDIDA_1'] == 'true'] = 'BE'
        if 'BD_MEDIDA_1' in df_comorbs.columns:
            df_comorbs.drop(columns=['BD_MEDIDA_1'], inplace=True)
        if 'BE_MEDIDA_1' in df_comorbs.columns:
            df_comorbs.drop(columns=['BE_MEDIDA_1'], inplace=True)
    
    print(f"Extração concluída! {' '*50}", end="\r")
    return df_comorbs 
