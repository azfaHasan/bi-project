import pandas as pd
import clickhouse_connect
import time
import glob

def run_pipeline():
    start_time = time.time()
    # Pastikan password sesuai dengan konfigurasi ClickHouse Anda
    client = clickhouse_connect.get_client(
        host='localhost', 
        port=8123, 
        username='default', 
        password='admin123' 
    )
    
    # ==========================================
    # TAHAP 1: EXTRACT & LOAD
    # ==========================================
    print("1. Mengekstrak raw data...")
    all_files = glob.glob('data/raw/*.csv') 

    kolom = ['model', 'year', 'price', 'transmission', 'mileage', 'fuelType', 'engineSize']
    client.command("TRUNCATE TABLE db_bronze.cars_raw")
    
    total_baris = 0
    for file in all_files:
        print(f"   Memproses file: {file}")
        df = pd.read_csv(file, usecols=kolom, dtype=str)
        df.fillna('', inplace=True)
        client.insert_df('db_bronze.cars_raw', df)
        total_baris += len(df)
        
    print(f"Selesai memasukkan {total_baris} baris ke Layer Bronze.")

    # ==========================================
    # TAHAP 2: TRANSFORM (Bronze ke Silver)
    # ==========================================
    print("2. Membersihkan data...")
    client.command("TRUNCATE TABLE db_silver.cars_cleaned")
    
    # KITA KEMBALIKAN KE LOGIKA ASLI ANDA YANG BENAR
    transform_to_silver_query = """
    INSERT INTO db_silver.cars_cleaned (model, year, price, transmission, mileage, fuelType, engineSize)
    SELECT 
        trim(model) AS model,
        toInt32OrZero(year) AS year,
        toFloat64OrZero(price) AS price,
        trim(transmission) AS transmission,
        toFloat64OrZero(mileage) AS mileage,
        trim(fuelType) AS fuelType,
        toFloat64OrZero(engineSize) AS engineSize
    FROM db_bronze.cars_raw
    WHERE 
        price > 0 AND
        year > 1900 AND
        model != ''
    """
    client.command(transform_to_silver_query)
    print("Selesai memproses Layer Silver.")

    # ==========================================
    # TAHAP 3: AGGREGATE (Silver ke Gold)
    # ==========================================
    print("3. Menyiapkan Tabel KPI...")
    client.command("TRUNCATE TABLE db_gold.fact_price_trends")
    
    transform_to_gold_query = """
    INSERT INTO db_gold.fact_price_trends (model, year, avg_price, avg_mileage, total_units)
    SELECT 
        model,
        year,
        round(avg(price), 2) AS avg_price, 
        round(avg(mileage), 2) AS avg_mileage,
        toUInt32(count()) AS total_units
    FROM db_silver.cars_cleaned
    GROUP BY model, year
    HAVING count() > 5
    """
    client.command(transform_to_gold_query)
    print("Selesai memproses Layer Gold.")
    
    end_time = time.time()
    print(f"Pipeline ELT Selesai dalam {round(end_time - start_time, 2)} detik!")
    print("Data siap ditarik oleh Power BI dari database 'db_gold'.")

if __name__ == "__main__":
    run_pipeline()