import io
import logging.config
import os
from datetime import datetime
from typing import Tuple, List, Dict, Any, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobServiceClient, ContainerClient
from pandas import DataFrame

from dotenv import load_dotenv
load_dotenv()

storage_account_name = os.getenv('STORAGE_ACCOUNT_NAME')
storage_account_key = os.getenv('STORAGE_ACCOUNT_KEY')
container = "landing"


# Falta mejorar obtención de credenciales usando key_vault
# Quitar la respuesta de los headers al hacer el upload al servicio de blob storage
# Agregar hilos para la lectura y almacenamiento de los df

def save_df_to_blob_storage_parquet(df: pd.DataFrame, azure_client: ContainerClient, file_name: str) -> None:
    """
    Guarda un DataFrame de pandas en Azure Blob Storage en formato Parquet.
        :param df_course_category: Nombre del archivo.
        :param df: DataFrame de pandas a guardar.
        :param azure_client: Cliente de contenedor de Azure Blob Storage.
        :return None:
    """
    logger.info(f"UPLOAD_FILE_INTO_{container.upper()}")
    try:
        table = pa.Table.from_pandas(df)

        with io.BytesIO() as buf:
            pq.write_table(table, buf)
            blob_client = azure_client.get_blob_client(blob=f"{file_name}.parquet")
            blob_client.upload_blob(buf.getvalue(), overwrite=True)
            logger.info(f"DATA_FRAME_UPLOAD_SUCCESSFULLY_:_{file_name.upper()}")
    except Exception as e:
        logger.info(f"ERROR_SAVING_DF_{file_name.upper()}: {e}")


def create_client_azure() -> Optional[ContainerClient]:
    """
    Crea un cliente de Azure Blob Storage.
        :return ContainerClient: Un cliente de contenedor de Azure Blob Storage si la conexión es exitosa,
        None en caso contrario.
    """
    logger.info("CREATE_CLIENT_AZURE")
    try:
        container_name = container
        blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net",
                                                credential=storage_account_key)
        container_client = blob_service_client.get_container_client(container_name)
        return container_client
    except AzureError as e:
        logger.info(f"Error connecting to the Blob service: {e}")
    except Exception as e:
        logger.info(f"An unexpected error occurred: {e}")


def create_df(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Crea un DataFrame de pandas a partir de una lista de diccionarios.
        :param data: Una lista de diccionarios donde cada diccionario representa una fila en el DataFrame.
        :return pd.DataFrame: Un DataFrame de pandas creado a partir de la lista de diccionarios.
    """
    return pd.DataFrame(data)


def create_data() -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Crea tres DataFrames: course_category_data, course_level_data y course_data.
    Los DataFrames contienen datos simulados para categorías de cursos, niveles de cursos y cursos respectivamente.
        :return Tuple[DataFrame, DataFrame, DataFrame]: Un tuple que contiene los tres DataFrames.
    """
    logger.info("CREATE_DATA")
    course_category_data = [
        {'category_id': 1, 'category_name': "Technology"},
        {'category_id': 2, 'category_name': "Business"},
        {'category_id': 3, 'category_name': "Art"},
        {'category_id': 4, 'category_name': "Science"},
        {'category_id': 5, 'category_name': "Health"}
    ]

    course_level_data = [
        {'level_id': 1, 'level_name': "Beginner"},
        {'level_id': 2, 'level_name': "Intermediate"},
        {'level_id': 3, 'level_name': "Advance"}
    ]

    np.random.seed(0)
    category_ids = np.random.choice([1, 2, 3, 4, 5], 100)
    level_ids = np.random.choice([101, 102, 103], 100)
    start_dates = [datetime(2021, np.random.randint(1, 13), np.random.randint(1, 29))
                   for _ in range(100)]

    course_data = {
        "course_id": np.arange(1, 101),
        "course_name": [f'Course{i}' for i in range(1, 101)],
        "category_id": category_ids,
        "level_id": level_ids,
        "start_date": start_dates
    }
    return create_df(course_category_data), create_df(course_level_data), create_df(course_data)


def main():
    df_course_category,  df_course_level, df_course = create_data()
    azure_client = create_client_azure()
    if azure_client:
        dataframes = [(df_course_category, "df_course_category"),
                      (df_course_level, "df_course_level"),
                      (df_course, "df_course")]
        for df, name in dataframes:
            save_df_to_blob_storage_parquet(df, azure_client, name)
    else:
        logger.info("CLIENT_AZURE_NOT_CREATED")


if __name__ == "__main__":

    with open('logging.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    logger = logging.getLogger(__name__)
    logger.info("START_PROCESS")
    main()
    logger.info("END_PROCESS")
