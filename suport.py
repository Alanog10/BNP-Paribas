import pandas as pd

#----------------------------------------------------

def ID_mapping(df):
   
    # só pares únicos para evitar duplicação mensal
    UNIQUE = df[['CONTRIB', 'DOSSIER']].drop_duplicates().copy()

    # renumerar clientes
    UNIQUE['CLIENT_NEW'] = pd.factorize(UNIQUE['CONTRIB'])[0] + 1

    # renumerar dossiers dentro do cliente
    UNIQUE['DOSSIER_NEW'] = (
        UNIQUE.groupby('CLIENT_NEW')['DOSSIER']
           .transform(lambda x: pd.factorize(x)[0] + 1)
    )

    # id final
    NEW = UNIQUE.copy()
    NEW['ID_NEW'] = (
        NEW['CLIENT_NEW'].astype(str) + '.' +
        NEW['DOSSIER_NEW'].astype(str)
    )

    # devolver mapping “limpo”
    mapping = NEW[['CONTRIB', 'DOSSIER', 'CLIENT_NEW', 'DOSSIER_NEW', 'ID_NEW']].copy()
    return mapping


def apply_mapping(df, mapping):
    
    # validação mínima
    cols_needed = {'CONTRIB', 'DOSSIER'}
    if not cols_needed.issubset(df.columns):
        raise ValueError(f"df must contain columns {cols_needed}")

    map_needed = {'CONTRIB', 'DOSSIER', 'ID_NEW'}
    if not map_needed.issubset(mapping.columns):
        raise ValueError(f"mapping must contain columns {map_needed}")

    df_out = df.merge(mapping[['CONTRIB', 'DOSSIER', 'ID_NEW']],
                      on=['CONTRIB', 'DOSSIER'],
                      how='left')

    # se houver pares sem correspondência, é melhor falhar cedo
    if df_out['ID_NEW'].isna().any():
        n_miss = df_out['ID_NEW'].isna().sum()
        raise ValueError(f"It's missing {n_miss} on the mapping. "
                         f"Verify if the mapping was created from the same df.")

    # remover colunas originais
    df_out = df_out.drop(columns=['CONTRIB', 'DOSSIER'])

    # opcional: pôr ID_NEW como primeira coluna
    cols = ['ID_NEW'] + [c for c in df_out.columns if c != 'ID_NEW']
    df_out = df_out[cols]

    return df_out


def map_srec_status(df):
    """
    Classifica cada ID_NEW com base no comportamento do SREC:

    - paid_ok   -> SREC sempre 0
    - paid_less -> existe SREC > 0
    - paid_more -> existe SREC < 0
    - mixed     -> existe SREC > 0 e < 0
    """

    # 1️⃣ Criar flags por ID
    status = (
        df
        .groupby('ID_NEW')['SREC']
        .agg(
            has_positive = lambda x: (x > 0).any(),
            has_negative = lambda x: (x < 0).any()
        )
    )

    # 2️⃣ Criar classificação
    def classify(row):
        if row['has_positive'] and row['has_negative']:
            return 'mixed'
        elif row['has_positive']:
            return 'paid_less'
        elif row['has_negative']:
            return 'paid_more'
        else:
            return 'paid_ok'

    status['SREC_STATUS'] = status.apply(classify, axis=1)

    # 3️⃣ Juntar ao dataframe original
    df_out = df.merge(
        status[['SREC_STATUS']],
        left_on='ID_NEW',
        right_index=True,
        how='left'
    )

    return df_out





