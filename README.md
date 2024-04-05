# Documentación del Proyecto
Este proyecto es una aplicación de Python que crea tres DataFrames de pandas con datos simulados y los guarda en Azure Blob Storage en formato Parquet.

# Dependencias
El proyecto depende de las siguientes bibliotecas de Python:

- `io`
- `logging.config`
- `os`
- `datetime`
- `typing`
- `numpy`
- `pandas`
- `pyarrow`
- `yaml`
- `azure.core.exceptions`
- `azure.storage.blob`
- `dotenv`

## Configuración del entorno
Para configurar el entorno, necesitas un archivo `.env` en el directorio raíz del proyecto con las siguientes variables de entorno:

- `STORAGE_ACCOUNT_NAME`: El nombre de tu cuenta de almacenamiento de Azure.
- `STORAGE_ACCOUNT_KEY`: La clave de tu cuenta de almacenamiento de Azure.

### Funciones
El proyecto consta de las siguientes funciones:

- `save_df_to_blob_storage_parquet(df: pd.DataFrame, azure_client: ContainerClient, file_name: str) -> None:` Esta función guarda un DataFrame de pandas en Azure Blob Storage en formato Parquet.
- `create_client_azure() -> Optional[ContainerClient]`: Esta función crea un cliente de Azure Blob Storage.
- `create_df(data: List[Dict[str, Any]]) -> pd.DataFrame`: Esta función crea un DataFrame de pandas a partir de una lista de diccionarios.
- `create_data() -> Tuple[DataFrame, DataFrame, DataFrame]`: Esta función crea tres DataFrames con datos simulados.
- `main()`: Esta es la función principal que crea los DataFrames, crea el cliente de Azure y guarda los DataFrames en Azure Blob Storage.

## Ejecución

Para ejecutar el proyecto, simplemente ejecuta el archivo `main.py`:
```
python main.py
```

## Registro
El proyecto utiliza `logging` para registrar información sobre la ejecución del programa. La configuración de registro se encuentra en el archivo `logging.yaml`.