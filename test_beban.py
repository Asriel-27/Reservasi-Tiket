"""
Uji konkurensi & kinerja (Ketentuan #5 modul): race condition + latensi/throughput untuk 1, 5, 20 client.
Jalankan setelah server hidup:  python test_beban.py --alamat localhost:50051
"""
import argparse
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

import bioskop_pb2 as pb
from client_bioskop import KlienBioskop


def uji_race(alamat, jumlah_client=20, kursi_uji=("C1", "C2", "C3", "C4", "C5")):
    print(f"== Uji race condition: {jumlah_client} client berebut tiap kursi ==")
    semua_ok = True
    for kursi in kursi_uji:
        barrier = threading.Barrier(jumlah_client)
        hasil = []

        def rebut(i):
            k = KlienBioskop(alamat)
            barrier.wait()                              # semua mulai bersamaan
            try:
                k.pesan(kursi, f"client{i}", log=False)
                hasil.append("OK")
            except grpc.RpcError as e:
                hasil.append(e.code().name)
            finally:
                k.tutup()

        with ThreadPoolExecutor(jumlah_client) as ex:
            list(ex.map(rebut, range(jumlah_client)))
        ok, ditolak = hasil.count("OK"), hasil.count("ALREADY_EXISTS")
        status = "LULUS" if ok == 1 and ditolak == jumlah_client - 1 else "GAGAL"
        semua_ok &= status == "LULUS"
        print(f"  kursi {kursi}: berhasil={ok}, ditolak={ditolak} -> {status}")
    print("  HASIL:", "tidak ada race condition" if semua_ok else "ADA RACE CONDITION!", "\n")


def beban(alamat, n_client, ops_per_client=30):
    kursi_id = [f"{b}{k}" for b in "ABCDE" for k in range(1, 9)]
    latensi, galat = [], []
    lock = threading.Lock()

    def kerja(i):
        k = KlienBioskop(alamat, timeout=5.0)
        nama = f"beban{i}"
        lokal = []
        for _ in range(ops_per_client):
            kursi = random.choice(kursi_id[8 * 2 + 5:])   # hindari kursi uji race (C1-C5)
            t = time.perf_counter()
            try:
                k.pesan(kursi, nama, log=False)
                lokal.append(time.perf_counter() - t)
                t = time.perf_counter()
                k.batal(kursi, nama, log=False)
                lokal.append(time.perf_counter() - t)
            except grpc.RpcError as e:
                lokal.append(time.perf_counter() - t)
                if e.code() != grpc.StatusCode.ALREADY_EXISTS:   # rebutan wajar, bukan galat
                    with lock:
                        galat.append(e.code().name)
        k.tutup()
        with lock:
            latensi.extend(lokal)

    mulai = time.perf_counter()
    with ThreadPoolExecutor(n_client) as ex:
        list(ex.map(kerja, range(n_client)))
    total = time.perf_counter() - mulai
    return total, latensi, galat


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--alamat", default="localhost:50051")
    ap.add_argument("--ops", type=int, default=30, help="siklus pesan+batal per client")
    a = ap.parse_args()

    uji_race(a.alamat)

    print("== Uji kinerja (siklus PesanKursi + BatalkanPesanan) ==")
    print("| Client | Total RPC | Waktu total (s) | Throughput (req/s) | Latensi rata-rata (ms) | Galat |")
    print("|---|---|---|---|---|---|")
    for n in (1, 5, 20):
        total, lat, galat = beban(a.alamat, n, a.ops)
        print(f"| {n} | {len(lat)} | {total:.2f} | {len(lat) / total:.1f} | "
              f"{statistics.mean(lat) * 1000:.2f} | {len(galat)} |")

    k = KlienBioskop(a.alamat)
    dipesan = [x for x in k.lihat().kursi if x.status == pb.DIPESAN and x.pemesan.startswith("beban")]
    print(f"\nKonsistensi akhir: kursi 'beban*' yang tertinggal = {len(dipesan)} (harus 0)")
    k.tutup()


if __name__ == "__main__":
    main()
