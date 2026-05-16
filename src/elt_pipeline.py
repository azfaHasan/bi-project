import pandas as pd
import clickhouse_connect
import time
import glob
from dotenv import load_dotenv
from sklearn.linear_model import LinearRegression
import os


# ============================================================
# Fungsi ini harus di LUAR run_pipeline(), bukan di dalamnya
# ============================================================
def generate_regression_data(client):
    print("4. Menghitung Regresi Linear...")

    # 1. Extract data dari Silver — tambahkan year dan engineSize
    query = "SELECT model, year, mileage, engineSize, price FROM db_silver.cars_cleaned"
    df = client.query_df(query)

    # Rename kolom price -> actual_price
    df = df.rename(columns={'price': 'actual_price'})

    # Bersihkan data null dan engineSize = 0
    df = df.dropna(subset=['mileage', 'year', 'engineSize', 'actual_price'])
    df = df[df['engineSize'] > 0]

    # 2. Setup & Fit Model Regresi Linear per model mobil
    results = []
    for model_name, group in df.groupby('model'):
        if len(group) < 2:
            continue

        X = group[['mileage', 'year', 'engineSize']]
        y = group['actual_price']

        model = LinearRegression()
        model.fit(X, y)

        # 3. Buat prediksi
        group = group.copy()
        group['predicted_price'] = model.predict(X)

        # Pastikan predicted_price tidak negatif
        group['predicted_price'] = group['predicted_price'].clip(lower=0)

        results.append(group)

    if not results:
        print("⚠️ Tidak ada data yang cukup untuk regresi.")
        return

    df_final = pd.concat(results, ignore_index=True)
    df_final = df_final[['model', 'year', 'mileage', 'engineSize', 'actual_price', 'predicted_price']]

    # 4. Load ke tabel Gold
    client.command("TRUNCATE TABLE db_gold.fact_price_predictions")
    client.insert_df('db_gold.fact_price_predictions', df_final)

    print(f"✅ Regresi Linear berhasil dihitung dan diload ke Gold! ({len(df_final)} baris)")


def run_pipeline():
    load_dotenv()
    start_time = time.time()
    client = clickhouse_connect.get_client(
        host=os.getenv('CH_HOST'),
        port=os.getenv('CH_PORT'),
        username=os.getenv('CH_USER'),
        password=os.getenv('CH_PASSWORD')
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

    # ==========================================
    # TAHAP 3: AGGREGATE
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
        count() AS total_units
    FROM db_silver.cars_cleaned
    GROUP BY model, year
    HAVING total_units > 5
    """
    client.command(transform_to_gold_query)

    # ==========================================
    # TAHAP 4: REGRESI LINEAR
    # ==========================================
    generate_regression_data(client)  # <- dipanggil di sini

    end_time = time.time()
    print(f"Pipeline ELT Selesai dalam {round(end_time - start_time, 2)} detik!")
    print("Data siap ditarik oleh Power BI dari database 'db_gold'.")


if __name__ == "__main__":
    run_pipeline()