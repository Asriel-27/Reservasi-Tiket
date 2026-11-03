"""
Client gRPC Reservasi Tiket Bioskop.
Kelompok: Omar, Rosy, Rozan

Contoh:
  python client_bioskop.py lihat
  python client_bioskop.py pesan A1 --nama Omar
  python client_bioskop.py batal A1 --nama Omar
  python client_bioskop.py pantau
"""
import argparse
import logging
import random
import time
import uuid

import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc

logger = logging.getLogger("bioskop.client")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)
DEFAULT_TIMEOUT = 2.0
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_DELAY = 0.5


def generate_request_id():
    return str(uuid.uuid4())


def hitung_backoff(percobaan, jeda_awal=DEFAULT_RETRY_DELAY, maks_jeda=8.0, jitter=0.2):
    """Backoff eksponensial dengan jitter kecil agar retry tidak bersamaan."""
    dasar = min(jeda_awal * (2 ** (percobaan - 1)), maks_jeda)
    return dasar * random.uniform(1.0 - jitter, 1.0 + jitter)


def panggil_dengan_retry(rpc, request, timeout=DEFAULT_TIMEOUT, maks=DEFAULT_MAX_RETRIES, jeda_awal=DEFAULT_RETRY_DELAY, log=True):
    """Menjalankan RPC dengan timeout dan retry hanya untuk error sementara."""
    for percobaan in range(1, maks + 1):
        try:
            return rpc(request, timeout=timeout)
        except grpc.RpcError as e:
            if e.code() not in RETRYABLE or percobaan == maks:
                raise
            jeda = hitung_backoff(percobaan, jeda_awal=jeda_awal)
            if log:
                logger.info("[retry] percobaan %s gagal (%s), ulang dalam %.2f s", percobaan, e.code().name, jeda)
            time.sleep(jeda)


class KlienBioskop:
    def __init__(self, alamat="localhost:50051", timeout=DEFAULT_TIMEOUT):
        self.alamat = alamat
        self.timeout = timeout
        self.channel = grpc.insecure_channel(alamat)
        self.stub = pb_grpc.LayananBioskopStub(self.channel)

    def tutup(self):
        self.channel.close()

    def lihat(self, log=True):
        return panggil_dengan_retry(
            self.stub.GetDaftarKursi,
            pb.DaftarKursiRequest(),
            timeout=self.timeout,
            log=log,
        )

    def pesan(self, kursi_id, pemesan, request_id=None, log=True):
        """request_id dibuat sekali lalu dipakai di semua retry agar tidak ada duplikasi operasi."""
        rid = request_id or generate_request_id()
        req = pb.PesanKursiRequest(request_id=rid, kursi_id=kursi_id, pemesan=pemesan)
        return panggil_dengan_retry(self.stub.PesanKursi, req, timeout=self.timeout, log=log)

    def batal(self, kursi_id, pemesan, request_id=None, log=True):
        rid = request_id or generate_request_id()
        req = pb.BatalkanRequest(request_id=rid, kursi_id=kursi_id, pemesan=pemesan)
        return panggil_dengan_retry(self.stub.BatalkanPesanan, req, timeout=self.timeout, log=log)

    def pantau(self, snapshot=True, reconnect_delay=0.5):
        """Stream status kursi dengan reconnect otomatis bila server tidak tersedia."""
        percobaan = 0
        while True:
            try:
                for event in self.stub.StreamStatusKursi(pb.StreamRequest(kirim_snapshot=snapshot)):
                    percobaan = 0
                    yield event
                return
            except grpc.RpcError as e:
                if e.code() not in RETRYABLE:
                    raise
                percobaan += 1
                jeda = min(reconnect_delay * (2 ** (percobaan - 1)), 8.0) * random.uniform(0.85, 1.15)
                logger.info("[stream terputus: %s] sambung ulang dalam %.1f s", e.code().name, jeda)
                time.sleep(jeda)


def cetak_denah(resp):
    print("\n          [ LAYAR ]\n")
    baris = {}
    for kursi in resp.kursi:
        baris.setdefault(kursi.kursi_id[0], []).append(kursi)

    for huruf in sorted(baris):
        kolom = " ".join(
            f"{k.kursi_id:>3}{'X' if k.status == pb.DIPESAN else '.'}"
            for k in baris[huruf]
        )
        print(f"  {huruf}  {kolom}")

    print("\n  . = tersedia, X = dipesan   (versi state: %d)\n" % resp.versi)


def format_status_response(response, *, field="pesan"):
    status = "(dijawab dari catatan duplikat)" if getattr(response, "duplikat", False) else ""
    return f"{getattr(response, field)} {status}".strip()


def parse_args():
    ap = argparse.ArgumentParser(description="Client gRPC Reservasi Tiket Bioskop")
    ap.add_argument("--alamat", default="localhost:50051", help="host:port server")
    ap.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="timeout per RPC dalam detik")
    ap.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="maksimal retry untuk koneksi sementara")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("lihat")

    for nama in ("pesan", "batal"):
        p = sub.add_parser(nama, help=f"{nama} kursi")
        p.add_argument("kursi", help="kode kursi, contoh A1")
        p.add_argument("--nama", required=True, help="NIM / nama pemesan")
        p.add_argument("--request-id", default=None, help="UUID request_id opsional")

    sub.add_parser("pantau")
    return ap.parse_args()


def main():
    a = parse_args()
    client = KlienBioskop(a.alamat, a.timeout)
    try:
        if a.cmd == "lihat":
            cetak_denah(client.lihat())
        elif a.cmd == "pesan":
            result = client.pesan(a.kursi, a.nama, a.request_id)
            print(format_status_response(result))
        elif a.cmd == "batal":
            result = client.batal(a.kursi, a.nama, a.request_id)
            print(format_status_response(result))
        elif a.cmd == "pantau":
            print("Mendengarkan perubahan kursi... (Ctrl+C untuk berhenti)")
            for event in client.pantau():
                print(f"[{event.waktu}] v{event.versi} kursi {event.kursi_id} -> {pb.StatusKursi.Name(event.status)} {event.pemesan}")
    except grpc.RpcError as e:
        print(f"Error dari server: {e.code().name} - {e.details()}")
    except KeyboardInterrupt:
        pass
    finally:
        client.tutup()


if __name__ == "__main__":
    main()
