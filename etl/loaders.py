"""
Data Loaders
=============
Write transformed data to Azure targets (ADLS, SQL, console).
"""


import json
import logging


from etl.pipeline import BaseLoader

logger = logging.getLogger(__name__)


class ADLSLoader(BaseLoader):
    """
    Load data as JSON to an ADLS Gen2 container.

    Args:
        account_name:  Azure storage account name
        container:     Target container (e.g. 'silver', 'gold')
        output_path:   Path within container (e.g. 'sales/2024/output.json')
        credential:    Azure credential object

    Example:
        from azure.identity import DefaultAzureCredential
        loader = ADLSLoader(
            account_name="mystorageacct",
            container="silver",
            output_path="sales/2024/sales_clean.json",
            credential=DefaultAzureCredential(),
        )
        count = loader.load(records)
    """

    def __init__(self, account_name: str, container: str, output_path: str, credential):
        super().__init__(target_name=f"adls://{account_name}/{container}/{output_path}")
        self.account_name = account_name
        self.container = container
        self.output_path = output_path
        self.credential = credential

    def load(self, data: list[dict]) -> int:
        from azure.storage.filedatalake import DataLakeServiceClient

        self.logger.info(f"Writing {len(data)} records to {self.target_name}")
        url = f"https://{self.account_name}.dfs.core.windows.net"
        client = DataLakeServiceClient(account_url=url, credential=self.credential)

        fs = client.get_file_system_client(self.container)
        file_client = fs.get_file_client(self.output_path)

        content = json.dumps(data, indent=2, default=str).encode("utf-8")
        file_client.upload_data(content, overwrite=True)

        self._records_loaded = len(data)
        self.logger.info(f"Successfully loaded {self._records_loaded} records")
        return self._records_loaded


class ConsoleLoader(BaseLoader):
    """
    Development loader — prints records to console.
    Useful for local testing without Azure credentials.

    Example:
        loader = ConsoleLoader(max_preview=5)
        loader.load(records)
    """

    def __init__(self, max_preview: int = 10):
        super().__init__(target_name="console")
        self.max_preview = max_preview

    def load(self, data: list[dict]) -> int:
        print(f"\n{'='*60}")
        print(f"ConsoleLoader — {len(data)} records")
        print(f"{'='*60}")
        for i, record in enumerate(data[:self.max_preview]):
            print(json.dumps(record, indent=2, default=str))
        if len(data) > self.max_preview:
            print(f"... and {len(data) - self.max_preview} more records")
        print(f"{'='*60}\n")
        self._records_loaded = len(data)
        return self._records_loaded


class SQLLoader(BaseLoader):
    """
    Upsert records into Azure SQL / SQL Server using pyodbc.

    Args:
        connection_string: ODBC connection string
        table:             Target table name (schema.table)
        upsert_keys:       Columns to match on for upsert logic

    Example:
        loader = SQLLoader(
            connection_string="Driver={ODBC Driver 18};...",
            table="dbo.SalesClean",
            upsert_keys=["order_id"],
        )
    """

    def __init__(self, connection_string: str, table: str, upsert_keys: list[str]):
        super().__init__(target_name=f"sql://{table}")
        self.connection_string = connection_string
        self.table = table
        self.upsert_keys = upsert_keys

    def load(self, data: list[dict]) -> int:
        import pyodbc

        if not data:
            self.logger.warning("No records to load")
            return 0

        columns = list(data[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        col_names = ", ".join(columns)
        insert_sql = f"INSERT INTO {self.table} ({col_names}) VALUES ({placeholders})"

        self.logger.info(f"Inserting {len(data)} records into {self.table}")
        conn = pyodbc.connect(self.connection_string)
        cursor = conn.cursor()
        cursor.fast_executemany = True

        rows = [tuple(r.get(col) for col in columns) for r in data]
        cursor.executemany(insert_sql, rows)
        conn.commit()
        conn.close()

        self._records_loaded = len(data)
        self.logger.info(f"Committed {self._records_loaded} records")
        return self._records_loaded
