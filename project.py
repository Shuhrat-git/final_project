#!/usr/bin/env python
"""
project.py
----------
Main file for the cryptocurrency trading project. Contains:
  - main() function
  - Three additional functions:
      1. fetch_data
      2. store_data_in_sql
      3. compute_signals
  - A class (CryptoAnalyzer) with a user-defined method (analyze_data)
"""

import os
import sqlite3
import pandas as pd
import ccxt
import pandas_ta as ta

# ------------------------------------------------------------------------
# CONFIGURATION / CONFIGURAÇÃO
# ------------------------------------------------------------------------
DATABASE_NAME = "crypto_btc.db"          # Nome do banco de dados
PRICE_TABLE = "btc_prices"               # Nome da tabela de preços
SYMBOL = "BTC/USDT"                      # Par de negociação
TIMEFRAME = "1d"                         # Intervalo de tempo das velas (1 dia)
CANDLE_LIMIT = 200                       # Limite de velas a serem buscadas

# ------------------------------------------------------------------------
# FUNCTIONS / FUNÇÕES
# ------------------------------------------------------------------------

def fetch_data(symbol=SYMBOL, timeframe=TIMEFRAME, limit=CANDLE_LIMIT):
    """
    Fetch OHLCV data from Binance using ccxt and return as a Pandas DataFrame.
    
    Busca dados OHLCV da Binance usando ccxt e retorna como um DataFrame do Pandas.
    """
    try:
        exchange = ccxt.binance()
        print(f"Fetching data for {symbol} with timeframe {timeframe}...")
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        
        # Convert to DataFrame / Converter para DataFrame
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")  # Converter timestamp de milissegundos
        df.sort_values("timestamp", inplace=True)                     # Ordenar por timestamp
        return df
    except Exception as e:
        print(f"Error fetching data from Binance: {e}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro

def store_data_in_sql(df, db_name=DATABASE_NAME, table_name=PRICE_TABLE):
    """
    Store the given DataFrame into an SQLite database table, avoiding duplicates.
    Creates the table if it does not exist.
    
    Armazena o DataFrame fornecido em uma tabela SQLite, evitando duplicatas.
    Cria a tabela se não existir.
    """
    if df.empty:
        print("DataFrame is empty. Nothing to store.")
        return 0  # Indica que 0 linhas foram inseridas

    # Convert numeric columns to float for consistency / Converter colunas numéricas para float para consistência
    numeric_columns = ["open", "high", "low", "close", "volume"]
    for column in numeric_columns:
        df[column] = df[column].astype(float)

    # Connect to (or create) the SQLite database / Conectar ou criar o banco de dados SQLite
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create table if not exists / Criar tabela se não existir
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TEXT PRIMARY KEY,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
    """)

    # Create an index on timestamp if it doesn't exist / Criar um índice em timestamp se não existir
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_timestamp ON {table_name}(timestamp)")

    # Check latest timestamp in DB / Verificar o último timestamp no banco de dados
    cursor.execute(f"SELECT MAX(timestamp) FROM {table_name}")
    last_timestamp_in_db = cursor.fetchone()[0]

    # Prepare data for insertion / Preparar dados para inserção
    df["timestamp_str"] = df["timestamp"].astype(str)
    if last_timestamp_in_db:
        # Filtrar apenas novos dados / Filter only new data
        df_to_insert = df[df["timestamp_str"] > last_timestamp_in_db].copy()
    else:
        df_to_insert = df.copy()

    if df_to_insert.empty:
        print("No new data to insert into the database.")
        conn.close()
        return 0

    # Renomear coluna para timestamp / Rename column to timestamp
    df_to_insert.rename(columns={"timestamp_str": "timestamp"}, inplace=True)
    df_to_insert = df_to_insert[["timestamp", "open", "high", "low", "close", "volume"]]

    # Insert new records / Inserir novos registros
    df_to_insert.to_sql(table_name, conn, if_exists="append", index=False)
    conn.commit()
    conn.close()

    print(f"Inserted {len(df_to_insert)} new rows into {table_name}")
    return len(df_to_insert)

def compute_signals(db_name=DATABASE_NAME, table_name=PRICE_TABLE):
    """
    Read price data from the SQLite database, compute indicators,
    and generate a 'signal' column using a multi-indicator confluence approach.
    Returns a DataFrame containing the signals.
    
    Lê os dados de preços do banco de dados SQLite, calcula indicadores,
    e gera uma coluna 'signal' usando uma abordagem de confluência de múltiplos indicadores.  
    Retorna um DataFrame contendo os sinais.
    """
    # Connect to DB and read table / Conectar ao banco de dados e ler a tabela
    conn = sqlite3.connect(db_name)
    df = pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY timestamp", conn)
    conn.close()

    if df.empty:
        print("No data available to compute signals.")
        return pd.DataFrame()

    # Convert timestamp back to datetime / Converter timestamp de volta para datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)

    # Compute indicators / Calcular indicadores
    df["EMA_20"] = ta.ema(df["close"], length=20)      # Média Móvel Exponencial de 20 períodos
    df["RSI"] = ta.rsi(df["close"], length=14)        # Índice de Força Relativa de 14 períodos
    df["VWAP"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])  # VWAP
    bollinger_bands = ta.bbands(df["close"], length=20)
    df["BB_upper"] = bollinger_bands["BBU_20_2.0"]     # Banda Superior de Bollinger
    df["BB_lower"] = bollinger_bands["BBL_20_2.0"]     # Banda Inferior de Bollinger
    df.dropna(inplace=True)                             # Remover linhas com NaN

    # Multi-indicator confluence logic / Lógica de confluência de múltiplos indicadores
    df["signal"] = 0  # Inicializar coluna de sinal como 0 (nenhuma ação)

    # Condição de confluência para compra / Bullish confluence condition
    bullish_conditions = (
        (df["close"] > df["EMA_20"]) &         # Preço de fechamento acima da EMA_20
        (df["close"] > df["VWAP"]) &           # Preço de fechamento acima do VWAP
        (df["RSI"] > 50) & (df["RSI"] < 70) &  # RSI entre 50 e 70
        (df["close"] < df["BB_upper"])         # Preço de fechamento abaixo da banda superior de Bollinger
    )

    # Condição de confluência para venda / Bearish confluence condition
    bearish_conditions = (
        (df["close"] < df["EMA_20"]) &         # Preço de fechamento abaixo da EMA_20
        (df["close"] < df["VWAP"]) &           # Preço de fechamento abaixo do VWAP
        (df["RSI"] < 50) &                      # RSI abaixo de 50
        (df["close"] > df["BB_lower"])         # Preço de fechamento acima da banda inferior de Bollinger
    )

    # Atribuir sinais / Assign signals
    df.loc[bullish_conditions, "signal"] = 1    # Sinal de compra
    df.loc[bearish_conditions, "signal"] = -1   # Sinal de venda

    return df

# ------------------------------------------------------------------------
# CLASS
# ------------------------------------------------------------------------

class CryptoAnalyzer:
    """
    Classe para realizar análises adicionais em um DataFrame contendo sinais.
    
    Class to perform additional analysis on a DataFrame containing signals.
    """

    def __init__(self, df):
        """
        Inicializa a classe com um DataFrame esperado ter uma coluna 'signal'.
        
        Initializes the class with a DataFrame expected to have a 'signal' column.
        """
        self.df = df

    def analyze_data(self):
        """
        Conta o número de sinais de compra (1), venda (-1) e manter (0).
        
        Counts the number of buy (1), sell (-1), and hold (0) signals.
        Returns a dictionary with the counts.
        Retorna um dicionário com as contagens.
        """
        if self.df.empty or "signal" not in self.df.columns:
            return {"buy": 0, "sell": 0, "hold": 0}

        buy_count = (self.df["signal"] == 1).sum()
        sell_count = (self.df["signal"] == -1).sum()
        hold_count = (self.df["signal"] == 0).sum()
        return {"buy": buy_count, "sell": sell_count, "hold": hold_count}

# ------------------------------------------------------------------------
# MAIN FUNCTION
# ------------------------------------------------------------------------

def main():
    """
    Main function: orchestrates fetching data, storing it, computing signals,
    and finally analyzing them with CryptoAnalyzer.
    
    Função principal: orquestra a busca de dados, armazenamento, computação de sinais,
    e finalmente a análise com CryptoAnalyzer.
    """
    print("1) Fetch data from Binance...")
    price_data = fetch_data()

    print("2) Store data into SQLite database...")
    rows_inserted = store_data_in_sql(price_data)

    print("3) Compute signals from SQLite data...")
    signals_data = compute_signals()

    # Instanciar a classe CryptoAnalyzer para analisar os sinais
    analyzer = CryptoAnalyzer(signals_data)
    analysis_results = analyzer.analyze_data()

    # Exibir resultados da análise
    print("\n--- ANALYSIS RESULTS ---")
    print(f"Buy signals  : {analysis_results['buy']}")
    print(f"Sell signals : {analysis_results['sell']}")
    print(f"Hold signals : {analysis_results['hold']}")

    # Exibir as últimas 5 linhas dos dados de sinais
    print("\nLast 5 signals data:")
    print(signals_data.tail(5))

# ------------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------------

if __name__ == "__main__":
    main()
