from etl import pipeline
from pathlib import Path
import os


if __name__ == "__main__":

    formato_saida = ["csv"]  # Ou 'parquet', conforme a decisão de saída

    pipeline(formato_saida[0])

    