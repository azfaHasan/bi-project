import clickhouse_connect
from dotenv import load_dotenv
import os

def run_ddl():
    load_dotenv()  # Memuat variabel lingkungan dari file .env
    print("Membangun Arsitektur Medallion di ClickHouse...")
    try:
        client = clickhouse_connect.get_client(
            host=os.getenv('CH_HOST'),
            port=os.getenv('CH_PORT'),
            username=os.getenv('CH_USER'),
            password=os.getenv('CH_PASSWORD')
    )        
        # 1. Membuat 3 Database
        for db in ['db_bronze', 'db_silver', 'db_gold']:
            client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
        
        # 2. BRONZE LEVEL DATABASE
        # Semua hasil extract -> String, menghindari error
        client.command("""
        CREATE TABLE IF NOT EXISTS db_bronze.cars_raw (
            model String,
            year String,
            price String,
            transmission String,
            mileage String,
            fuelType String,
            engineSize String
        ) ENGINE = MergeTree() ORDER BY tuple()
        """)

        # 3. SILVER LEVEL DATABASE
        # Menyesuaikan Tipe Data Setelah Data Preprocessing
        client.command("""
        CREATE TABLE IF NOT EXISTS db_silver.cars_cleaned (
            model String,
            year Int32,
            price Float64,
            transmission String,
            mileage Float64,
            fuelType String,
            engineSize Float64,
            clean_date DateTime DEFAULT now()
        ) ENGINE = MergeTree() ORDER BY (model, year)
        """)

        # 4. GOLD LEVEL DATABASE
        client.command("""
        CREATE TABLE IF NOT EXISTS db_gold.fact_price_trends (
            model String,
            year Int32,
            avg_price Float64,
            avg_mileage Float64,
            total_units UInt32
        ) ENGINE = MergeTree() ORDER BY (model, year)
        """)

        # 5. GOLD LEVEL DATABASE - Tabel Hasil Prediksi Harga
        client.command("""
        CREATE TABLE IF NOT EXISTS db_gold.fact_price_predictions (
            model String,
            mileage Float64,
            actual_price Float64,
            predicted_price Float64
        ) ENGINE = MergeTree() ORDER BY (model, mileage)
        """)

        print("Setup DDL selesai! Database siap digunakan.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    run_ddl()