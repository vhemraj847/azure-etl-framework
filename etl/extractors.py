"""
Azure Data Lake Storage (ADLS) Extractor
=========================================
Reads JSON / CSV files from ADLS Gen2 containers.
Requires: azure-storage-file-datalake
"""

import csv
import io
import json
import logging
from typing import Literal

from etl.pipeline import BaseExtractor

logger = logging.getLogger(__name__)


class ADLSExtractor(BaseExtractor):
    """
    Extract data from an ADLS Gen2 container path.

    Args:
        account_name:   Azure storage account name
        container:      Container (filesystem) name
        file_path:      Path to file within container
        file_format:    'json' or 'csv'
        credential:     Azure credential (DefaultAzureCredential recommended)

    Example:
        from azure.identity import DefaultAzureCredential
        extractor = ADLSExtractor(
            account_name="mystorageacct",
            container="bronze",
            file_path="sales/2024/data.json",
            credential=DefaultAzureCredential(),
        )
        records = extractor.extract()
    """

    def __init__(
        self,
        account_name: str,
        container: str,
        file_path: str,
        credential,
        file_format: Literal["json", "csv"] = "json",
    ):
        super().__init__(source_name=f"adls://{account_name}/{container}/{file_path}")
        self.account_name = account_name
        self.container = container
        self.file_path = file_path
        self.credential = credential
        self.file_format = file_format

    def _get_client(self):
        from azure.storage.filedatalake import DataLakeServiceClient
        url = f"https://{self.account_name}.dfs.core.windows.net"
        return DataLakeServiceClient(account_url=url, credential=self.credential)

    def extract(self) -> list[dict]:
        self.logger.info(f"Extracting from {self.source_name}")
        client = self._get_client()
        fs = client.get_file_system_client(self.container)
        file_client = fs.get_file_client(self.file_path)

        download = file_client.download_file()
        content = download.readall().decode("utf-8")

        if self.file_format == "json":
            data = json.loads(content)
            return data if isinstance(data, list) else [data]

        elif self.file_format == "csv":
            reader = csv.DictReader(io.StringIO(content))
            return list(reader)

        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")


class SQLExtractor(BaseExtractor):
    """
    Extract data from Azure SQL / SQL Server using pyodbc.

    Args:
        connection_string: ODBC connection string
        query:             SQL SELECT query to run
        params:            Optional query parameters (tuple)

    Example:
        extractor = SQLExtractor(
            connection_string="Driver={ODBC Driver 18};Server=...;Database=...;",
            query="SELECT * FROM dbo.Sales WHERE year = ?",
            params=(2024,),
        )
    """

    def __init__(self, connection_string: str, query: str, params: tuple = ()):
        super().__init__(source_name="azure_sql")
        self.connection_string = connection_string
        self.query = query
        self.params = params

    def extract(self) -> list[dict]:
        import pyodbc

        self.logger.info(f"Running query: {self.query[:80]}...")
        conn = pyodbc.connect(self.connection_string)
        cursor = conn.cursor()
        cursor.execute(self.query, self.params)

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        conn.close()

        records = [dict(zip(columns, row)) for row in rows]
        self.logger.info(f"Extracted {len(records)} rows from SQL")
        return records
