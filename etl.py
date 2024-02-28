import pandas as pd
import os

from glob import glob


def extract_data() -> pd.DataFrame:

    arquivos_json = glob(os.path.join('data/*.json'))

    df = list()
    for arquivo in arquivos_json:
        # Add files in a list
        df.append(pd.read_json(f'{arquivo}'))

        # Concatenat all files in one
        df_total = pd.concat(df, ignore_index=True)


    return df_total


def transform_data(df_extracted: pd.DataFrame) -> pd.DataFrame:
    # Add column income
    df_extracted['Receita'] = df_extracted['Quantidade'] * df_extracted['Venda']

    # Add columns 10% discount
    df_extracted['Receita_mais_iva'] = df_extracted['Receita'] + (df_extracted['Receita'] * 0.23)

    return df_extracted


def load_data(df_transformed: pd.DataFrame, desired_format: str):

    if desired_format == 'csv':
        df_transformed.to_csv('df_transformed.csv', index=False)

    elif desired_format == 'parquet':
        df_transformed.to_parquet('df_transformed.parquet', index=False)

    else:
        print('Format not suported!!!')


# df = extract_data()
# df = transform_data(df)

# load_data(df, 'csv')

def pipeline(desired_format: str):
    data = extract_data()
    data_transformed = transform_data(data)

    print('done')

    load_data(data_transformed, desired_format)


# pipeline(desired_format='csv')

