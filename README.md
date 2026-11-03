# Reservasi Tiket Bioskop berbasis gRPC (Studi Kasus 1)

Mata kuliah Komputasi Paralel dan Terdistribusi - Kelompok: **Omar, Rosy, Rozan**

## Struktur berkas

| Berkas | Fungsi |
|---|---|
| `bioskop.proto` | Kontrak layanan (IDL): 4 RPC, 1 di antaranya server streaming |
| `server_bioskop.py` | Server gRPC (ThreadPoolExecutor, Lock, deduplikasi request_id, broadcast streaming) |
| `client_bioskop.py` | Client CLI: lihat, pesan, batal, pantau (timeout + retry exponential backoff + UUID) |
| `bioskop_pb2.py`, `bioskop_pb2_grpc.py` | Stub Python yang digenerate dari `bioskop.proto` |
| `test_beban.py` | Uji race condition dan uji kinerja 1 / 5 / 20 client (menghasilkan tabel laporan) |
| `requirements.txt` | Dependensi |

## 1. Instalasi

```bash
python3 -m venv venv
# Windows PowerShell:
.\venv\Scripts\Activate.ps1
# Windows Command Prompt: venv\Scripts\activate.bat
# macOS/Linux:
source venv/bin/activate
pip install -r requirements.txt
```

## 2. Generate stub

```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. bioskop.proto
```

Menghasilkan `bioskop_pb2.py` (kelas message) dan `bioskop_pb2_grpc.py`
(`LayananBioskopStub` untuk client dan `LayananBioskopServicer` untuk server).
Ulangi perintah ini setiap kali `bioskop.proto` diubah.
Stub yang dihasilkan ikut disertakan agar proyek dapat langsung dijalankan; generate ulang jika kontrak berubah.

## 3. Menjalankan (terminal terpisah)

```bash
# Terminal 1 - server
python server_bioskop.py                  # opsi: --port 50051 --workers 30 --lambat

# Terminal 2 - pemantau real-time
python client_bioskop.py pantau

# Terminal 3 - client biasa
python client_bioskop.py lihat
python client_bioskop.py pesan A1 --nama Omar
python client_bioskop.py pesan A1 --nama Rosy     # ditolak: ALREADY_EXISTS
python client_bioskop.py batal A1 --nama Rozan    # ditolak: PERMISSION_DENIED
python client_bioskop.py batal A1 --nama Omar     # berhasil
python client_bioskop.py pesan Z9 --nama Omar     # ditolak: NOT_FOUND
```

Dua komputer: jalankan server di komputer A, lalu di komputer B pakai
`python client_bioskop.py --alamat <IP_A>:50051 lihat` (buka port 50051 di firewall).

### Demo retry + deduplikasi (at-most-once)

```bash
python server_bioskop.py --lambat       # 30% balasan terlambat 3 detik (operasi SUDAH tersimpan)
python client_bioskop.py pesan B1 --nama Omar
```

Client timeout (2 s) -> mencetak `[retry]` -> mengulang dengan **request_id yang sama** ->
server menjawab dari catatan (`dijawab dari catatan duplikat`) tanpa memesan dua kali.
Ulangi beberapa kali dengan kursi berbeda sampai pesan retry muncul.

### Uji beban dan race condition

```bash
python server_bioskop.py                # server baru (kursi C1-C5 dipakai uji race)
python test_beban.py                    # opsi: --alamat host:port --ops 30
```

Output: (1) 20 client berebut kursi yang sama, harus tepat 1 berhasil dan 19 `ALREADY_EXISTS`;
(2) tabel latensi dan throughput untuk 1, 5, 20 client (siap disalin ke laporan, buat grafiknya dari tabel ini).
Restart server sebelum menjalankan ulang uji race.

## 4. Ringkasan RPC

| RPC | Tipe | Keterangan | Error yang mungkin |
|---|---|---|---|
| `GetDaftarKursi` | Unary | Status seluruh kursi (5 baris x 8 kolom) | - |
| `PesanKursi` | Unary | Pesan kursi, dilindungi `request_id` | `ALREADY_EXISTS`, `NOT_FOUND`, `INVALID_ARGUMENT` |
| `BatalkanPesanan` | Unary | Hanya pemesan asli yang boleh batal, dilindungi `request_id` | `PERMISSION_DENIED`, `FAILED_PRECONDITION`, `NOT_FOUND` |
| `StreamStatusKursi` | Server streaming | Push perubahan kursi real-time (opsional snapshot awal) | - |

## 5. Penjelasan untuk laporan

### Strategi concurrency (Lock)
- Server memakai `ThreadPoolExecutor`, sehingga banyak RPC berjalan bersamaan di thread berbeda.
- Pemesanan adalah pola **check-then-act** (cek kursi kosong, lalu tulis pemesan). Tanpa pengaman, dua
  thread dapat sama-sama melihat kursi kosong lalu keduanya menulis sehingga terjadi pemesanan ganda (race condition).
- Seluruh state (kursi, versi, catatan request_id, daftar subscriber) dilindungi **satu `threading.Lock`**,
  dan blok cek + tulis + siaran event dilakukan atomik di dalam lock. Hasilnya, pada rebutan kursi yang sama
  hanya satu pemenang; yang lain mendapat `ALREADY_EXISTS`.
- Pilihan satu lock global dipilih demi kesederhanaan dan kebenaran; critical section sangat pendek (operasi dict),
  sehingga dampaknya ke throughput kecil. Alternatif yang lebih granular: lock per kursi.
- Streaming: tiap client punya `queue.Queue` sendiri. Snapshot dan pendaftaran subscriber dilakukan atomik
  di dalam lock agar tidak ada event yang terlewat. Setiap stream aktif menahan satu thread worker,
  jadi `--workers` harus lebih besar dari jumlah client `pantau`.

### Toleransi kegagalan
- **Timeout**: setiap pemanggilan punya deadline (default 2 s), sehingga client tidak menunggu selamanya.
- **Retry dengan exponential backoff**: jeda 0.5 s, 1 s, 2 s, ... (plus jitter acak 20% agar banyak client
  tidak mengulang serempak). Hanya `DEADLINE_EXCEEDED` dan `UNAVAILABLE` yang diulang; error logika
  seperti `ALREADY_EXISTS` tidak diulang.
- **request_id (UUID) -> at-most-once**: `PesanKursi` dan `BatalkanPesanan` tidak idempoten. Bila balasan hilang
  atau terlambat dan client mengulang, server mencari `request_id` di catatan dan mengembalikan hasil
  sebelumnya (`duplikat=true`) tanpa menjalankan operasi lagi. UUID dibuat sekali per operasi dan dipakai ulang di semua percobaan.
- Kunci deduplikasi memisahkan jenis operasi dan memeriksa payload. Memakai `request_id` yang sama untuk
  payload berbeda ditolak dengan `INVALID_ARGUMENT`, bukan dianggap sebagai retry yang sah.
- **Streaming**: client `pantau` menyambung ulang otomatis dengan backoff bila server sempat mati.
- **Keterbatasan** (bahan analisis): catatan `request_id` ada di memori, sehingga jika server restart
  jaminan at-most-once hilang. Perbaikan: simpan state dan `request_id` ke penyimpanan persisten (SQLite/Redis);
  tambahkan juga TTL agar catatan tidak membengkak.

### Pembagian tugas (saran)
| Anggota | Fokus |
|---|---|
| Omar | `bioskop.proto` + `server_bioskop.py` (lock, error handling) |
| Rosy | `client_bioskop.py` (timeout, retry, request_id, streaming) |
| Rozan | `test_beban.py`, pengukuran 2 mesin, grafik, laporan dan demo |

Pastikan tiap anggota melakukan commit sendiri di Git (syarat rubrik).
