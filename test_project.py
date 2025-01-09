#!/usr/bin/env python
"""
test_project.py
---------------
Pytest-based tests for the three required functions in project.py:
    1) test_fetch_data()
    2) test_store_data_in_sql()
    3) test_compute_signals()
"""

import pytest
import os
import sqlite3
import pandas as pd
from project import fetch_data, store_data_in_sql, compute_signals

# ------------------------------------------------------------------------
# CONFIGURATION / CONFIGURAÇÃO
# ------------------------------------------------------------------------
TEST_DATABASE_NAME = "test_crypto.db"         # Nome do banco de dados de teste
TEST_PRICE_TABLE = "test_btc_prices"          # Nome da tabela de preços de teste

# ------------------------------------------------------------------------
# FIXTURES
# ------------------------------------------------------------------------

@pytest.fixture
def cleanup_test_database():
    """
    A pytest fixture to remove the test database file after each test run
    to ensure a clean state.
    
    Uma fixture do pytest para remover o arquivo do banco de dados de teste após cada execução de teste
    para garantir um estado limpo.
    """
    yield
    if os.path.exists(TEST_DATABASE_NAME):
        os.remove(TEST_DATABASE_NAME)

# ------------------------------------------------------------------------
# TEST FUNCTIONS / FUNÇÕES DE TESTE
# ------------------------------------------------------------------------

def test_fetch_data():
    """
    Tests that fetch_data returns a DataFrame with the expected columns
    and is not empty (assuming Binance returns data).
    
    Testa se fetch_data retorna um DataFrame com as colunas esperadas
    e não está vazio (assumindo que a Binance retorna dados).
    """
    # Chama a função fetch_data para obter os dados
    fetched_data = fetch_data()
    
    # Define as colunas esperadas no DataFrame
    expected_columns = ["timestamp", "open", "high", "low", "close", "volume"]
    
    # Verifica se todas as colunas esperadas estão presentes
    for column in expected_columns:
        assert column in fetched_data.columns, f"Missing column: {column}"
    
    # Verifica se o DataFrame não está vazio (assumindo que a chamada à API foi bem-sucedida)
    assert not fetched_data.empty, "DataFrame is empty, expected some rows."

def test_store_data_in_sql(cleanup_test_database):
    """
    Tests that store_data_in_sql creates the table and inserts data.
    
    Testa se store_data_in_sql cria a tabela e insere os dados.
    """
    # Cria um pequeno DataFrame de teste com dados fictícios
    test_data = {
        "timestamp": pd.date_range("2023-01-01", periods=5, freq="D"),
        "open": [17000, 17100, 17200, 17300, 17400],
        "high": [17100, 17200, 17300, 17400, 17500],
        "low": [16900, 17000, 17100, 17200, 17300],
        "close": [17050, 17150, 17250, 17350, 17450],
        "volume": [100, 200, 300, 400, 500],
    }
    test_df = pd.DataFrame(test_data)

    # Chama a função store_data_in_sql para armazenar os dados de teste no banco de dados de teste
    linhas_inseridas = store_data_in_sql(
        test_df,
        db_name=TEST_DATABASE_NAME,
        table_name=TEST_PRICE_TABLE
    )
    
    # Verifica se o número de linhas inseridas é igual ao esperado (5)
    assert linhas_inseridas == 5, "Expected to insert 5 rows."

    # Conecta ao banco de dados de teste para verificar se os dados foram realmente inseridos
    conexao = sqlite3.connect(TEST_DATABASE_NAME)
    dados_db = pd.read_sql_query(f"SELECT * FROM {TEST_PRICE_TABLE}", conexao)
    conexao.close()

    # Verifica se o número de linhas na tabela de teste é igual ao esperado (5)
    assert len(dados_db) == 5, "Expected 5 rows in the test table."

    # Verifica se todas as colunas esperadas estão presentes na tabela de teste
    for coluna in ["timestamp", "open", "high", "low", "close", "volume"]:
        assert coluna in dados_db.columns, f"Column {coluna} not found in DB"

def test_compute_signals(cleanup_test_database):
    """
    Tests that compute_signals successfully reads data from SQL,
    computes indicators, and returns a DataFrame with a 'signal' column.
    
    Testa se compute_signals lê dados do SQL com sucesso,
    calcula indicadores e retorna um DataFrame com uma coluna 'signal'.
    """
    # Cria um pequeno DataFrame de teste com dados fictícios para sinais
    dados_para_sinal = {
        "timestamp": pd.date_range("2023-01-01", periods=10, freq="D"),
        "open": [17000, 17100, 17150, 17200, 17300, 17250, 17280, 17310, 17400, 17500],
        "high": [17050, 17150, 17200, 17250, 17350, 17300, 17330, 17360, 17450, 17550],
        "low": [16950, 17050, 17100, 17150, 17250, 17200, 17230, 17260, 17350, 17450],
        "close": [17020, 17120, 17180, 17220, 17310, 17270, 17290, 17320, 17410, 17510],
        "volume": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
    }
    df_para_sinal = pd.DataFrame(dados_para_sinal)
    
    # Armazena os dados de teste no banco de dados de teste
    store_data_in_sql(
        df_para_sinal,
        db_name=TEST_DATABASE_NAME,
        table_name=TEST_PRICE_TABLE
    )

    # Chama a função compute_signals para calcular os sinais com base nos dados armazenados
    df_sinais = compute_signals(
        db_name=TEST_DATABASE_NAME,
        table_name=TEST_PRICE_TABLE
    )

    # Define as colunas de indicadores esperadas no DataFrame de sinais
    colunas_de_indicadores = ["EMA_20", "RSI", "VWAP", "BB_upper", "BB_lower", "signal"]
    
    # Verifica se todas as colunas de indicadores estão presentes no DataFrame de sinais
    for coluna in colunas_de_indicadores:
        assert coluna in df_sinais.columns, f"Missing indicator column: {coluna}"

    # Verifica se a coluna 'signal' está presente e não está completamente vazia (NaN)
    assert "signal" in df_sinais.columns, "No 'signal' column found"
    assert not df_sinais["signal"].isna().all(), "Signal column is entirely NaN"

"""
Usage:
------
1) Install required libraries:
    pip install pytest ccxt pandas pandas_ta

2) From the project root folder, run:
    pytest
"""
