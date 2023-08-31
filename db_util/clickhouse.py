import clickhouse_connect 
import pandas as pd

class ClickHouseConnector:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.client = None

    def connect(self):
        self.client = clickhouse_connect.get_client(host=self.host, port=self.port, user=self.user, password=self.password)

    
    def read_query_as_dataframe(self, query, cols, chunk_size=10000):
        if not self.client:
            self.connect()

        offset = 0
        dfs = []

        while True:
            query_with_offset = f'{query} LIMIT {chunk_size} OFFSET {offset}'
            result = self.client.query(query_with_offset)

            if len(result.result_rows) == 0:
                break

            # columns = [col[0] for col in result.cursor.description]
   
            data = [list(row) for row in result.result_rows]

            df = pd.DataFrame(data, columns=cols)
            dfs.append(df)

            offset += chunk_size

        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return None
        
    def close(self):
        if self.client:
            self.client.disconnect()