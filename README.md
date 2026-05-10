## Repository Guide

Folder `data` akan diisi dengan data yang akan dianalisis

Folder `src` berisi script Python untuk proses `DDL` dan `ETL`

Folder `dashboard` berisi file visualisasi Power BI

## Preparation

Install [docker](https://www.docker.com/products/docker-desktop/) untuk mempercepat setup melalui sistem image dan container.

Cek apakah docker sudah terinstall menggunakan _command_ ini di terminal/cmd:

```bash
docker --version
```

Setelah selesai, lanjut ke tahap setup

## Setup - Repo Cloning

Clone repository proyek ini pada sebuah folder menggunakan GitHub Desktop atau melalui terminal.

```bash
cd alamat folder kalian
# misalnya cd C:\kuliah\project-bi
```

Jalankan ini di Git Bash dalam folder terkait untuk cloning.
```bash
git clone [URL_REPOSITORY_ANDA_DI_SINI]
```

Selanjutnya buat branch baru untuk area kerja.

## Setup - File .env

Buat file baru dengan nama `.env` di root berisi kredensial database.

```bash
CH_USER=default
CH_PASSWORD=masukkan_password_rahasia_disini
CH_HOST=localhost
CH_PORT=8123
```

## Setup - Virtual Environment

Instalasi dependensi proyek tidak bertabrakan dengan dependensi proyek lain jika menggunakan instalasi global.

Buka terminal vscode, jalankan ini untuk setup venv:
```bash
# buat virtual env dengan nama 'venv'
python -m venv venv
```

Pastikan sudah ada folder baru dengan nama `.venv`.

Sebelum install dependensi proyek, pastikan terminal sudah berada pada venv yang dibuat. Tandanya baris pertama terminal diawali (venv).

Jika belum ada jalankan ini untuk aktivasi venv:
```bash
venv\Scripts\active
```

dan ini untuk keluar dari venv:
```bash
deactive
```

Jika sudah berada di venv, install semua dependensi yang diperlukan:
```bash
pip install -r requirements.txt
```

## Cara Menjalankan Proyek

Jalankan docker, atau mudahnya menyalakan docker desktop yang sudah terinstall.

Pada terminal vscode, jalankan ini untuk menjalankan container:
```bash
docker compose up -d
```
Mungkin akan memakan waktu beberapa menit jika belum ada image Clickhouse

Pastikan container sudah berjalan (bisa dilihat pada docker desktop), kemudian jalankan skrip pertama untuk proses DDL:

```bash
python src/ddl_setup.py
```

Setelah proses DDL selesai, jalankan skrip untuk proses ELT:
```bash
python src/elt_pipeline.py
```

Cek hasil ELT, akses Clickhouse pada:
`http://localhost:8123/play`

## Cara Menghentikan Proyek

Jalankan ini untuk menghapus container:
```bash
docker compose down
```

Atau jalankan ini untuk pause sementara container:
```bash
docker compose stop
```

Jalankan lagi dengan:
```bash
docker compose start
```