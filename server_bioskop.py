"""
Server gRPC Reservasi Tiket Bioskop.
Kelompok: Omar, Rosy, Rozan - Komputasi Paralel dan Terdistribusi

Strategi utama:
  * threading.Lock  -> mencegah race condition saat banyak client memesan kursi yang sama.
  * request_id      -> deduplikasi (at-most-once) agar retry tidak mengeksekusi operasi dua kali.
  * queue per client streaming -> push perubahan kursi secara real-time.
"""
import argparse
import queue
import random
import threading
import time
from concurrent import futures
from datetime import datetime

import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc

BARIS = "ABCDE"
KOLOM = 8


class StateBioskop:
    """Seluruh state dilindungi SATU lock agar kursi, versi, riwayat, dan subscriber konsisten."""

    def __init__(self):
        self.lock = threading.Lock()
        self.kursi = {f"{b}{k}": "" for b in BARIS for k in range(1, KOLOM + 1)}  # id -> pemesan
        self.versi = 0
        self.riwayat = {}          # (operasi, request_id) -> payload dan response
        self.subscriber = set()    # queue milik tiap client streaming

    def _siarkan(self, kursi_id):
        """Dipanggil saat lock sudah dipegang."""
        self.versi += 1
        pemesan = self.kursi[kursi_id]
        event = pb.PerubahanKursi(
            kursi_id=kursi_id,
            status=pb.DIPESAN if pemesan else pb.TERSEDIA,
            pemesan=pemesan,
            waktu=datetime.now().strftime("%H:%M:%S"),
            versi=self.versi,
        )
        for q in self.subscriber:
            q.put_nowait(event)


class LayananBioskop(pb_grpc.LayananBioskopServicer):
    def __init__(self, state, lambat=False):
        self.s = state
        self.lambat = lambat

    def _simulasi_balasan_lambat(self):
        # Operasi SUDAH tersimpan, tetapi balasan terlambat -> client timeout lalu retry.
        if self.lambat and random.random() < 0.3:
            time.sleep(3)

    # ---------- Unary: lihat semua kursi ----------
    def GetDaftarKursi(self, request, context):
        with self.s.lock:
            daftar = [
                pb.Kursi(kursi_id=k, status=pb.DIPESAN if p else pb.TERSEDIA, pemesan=p)
                for k, p in self.s.kursi.items()
            ]
            versi = self.s.versi
        return pb.DaftarKursiResponse(kursi=daftar, versi=versi)

    # ---------- Unary: pesan kursi (tidak idempoten -> pakai request_id) ----------
    def PesanKursi(self, request, context):
        request_id = request.request_id.strip()
        if not request_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request_id wajib diisi")
        kursi_id = request.kursi_id.strip().upper()
        pemesan = request.pemesan.strip()
        if not pemesan:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "pemesan wajib diisi")

        with self.s.lock:
            kunci_riwayat = ("pesan", request_id)
            lama = self.s.riwayat.get(kunci_riwayat)
            if lama is not None:                       # permintaan duplikat (retry)
                payload_lama, response_lama = lama
                if payload_lama != (kursi_id, pemesan):
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "request_id sudah digunakan untuk permintaan berbeda",
                    )
                hasil = pb.PesanKursiResponse()
                hasil.CopyFrom(response_lama)
                hasil.duplikat = True
                return hasil

            if kursi_id not in self.s.kursi:
                context.abort(grpc.StatusCode.NOT_FOUND, f"Kursi {kursi_id} tidak ditemukan")
            if self.s.kursi[kursi_id]:
                context.abort(
                    grpc.StatusCode.ALREADY_EXISTS,
                    f"Kursi {kursi_id} sudah dipesan oleh {self.s.kursi[kursi_id]}",
                )

            self.s.kursi[kursi_id] = pemesan           # check-then-act berada dalam satu lock
            self.s._siarkan(kursi_id)
            hasil = pb.PesanKursiResponse(
                sukses=True, pesan=f"Kursi {kursi_id} berhasil dipesan {pemesan}", kursi_id=kursi_id
            )
            self.s.riwayat[kunci_riwayat] = ((kursi_id, pemesan), hasil)

        self._simulasi_balasan_lambat()
        return hasil

    # ---------- Unary: batalkan pesanan ----------
    def BatalkanPesanan(self, request, context):
        request_id = request.request_id.strip()
        if not request_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request_id wajib diisi")
        kursi_id = request.kursi_id.strip().upper()
        pemesan = request.pemesan.strip()
        if not pemesan:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "pemesan wajib diisi")

        with self.s.lock:
            kunci_riwayat = ("batal", request_id)
            lama = self.s.riwayat.get(kunci_riwayat)
            if lama is not None:
                payload_lama, response_lama = lama
                if payload_lama != (kursi_id, pemesan):
                    context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT,
                        "request_id sudah digunakan untuk permintaan berbeda",
                    )
                hasil = pb.BatalkanResponse()
                hasil.CopyFrom(response_lama)
                hasil.duplikat = True
                return hasil

            if kursi_id not in self.s.kursi:
                context.abort(grpc.StatusCode.NOT_FOUND, f"Kursi {kursi_id} tidak ditemukan")
            pemilik = self.s.kursi[kursi_id]
            if not pemilik:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, f"Kursi {kursi_id} belum dipesan")
            if pemilik != pemesan:
                context.abort(grpc.StatusCode.PERMISSION_DENIED,
                              f"Kursi {kursi_id} bukan milik {pemesan}")

            self.s.kursi[kursi_id] = ""
            self.s._siarkan(kursi_id)
            hasil = pb.BatalkanResponse(sukses=True, pesan=f"Pesanan kursi {kursi_id} dibatalkan")
            self.s.riwayat[kunci_riwayat] = ((kursi_id, pemesan), hasil)

        self._simulasi_balasan_lambat()
        return hasil

    # ---------- Server streaming: update real-time ----------
    def StreamStatusKursi(self, request, context):
        q = queue.Queue()
        with self.s.lock:                              # snapshot + subscribe atomik: tidak ada event terlewat
            self.s.subscriber.add(q)
            awal = []
            if request.kirim_snapshot:
                awal = [
                    pb.PerubahanKursi(kursi_id=k, status=pb.DIPESAN, pemesan=p,
                                      waktu="snapshot", versi=self.s.versi)
                    for k, p in self.s.kursi.items() if p
                ]
        try:
            for e in awal:
                yield e
            while context.is_active():
                try:
                    yield q.get(timeout=1.0)
                except queue.Empty:
                    continue
        finally:
            with self.s.lock:
                self.s.subscriber.discard(q)


def serve(port, workers, lambat):
    # Catatan: setiap stream aktif menahan 1 thread, jadi workers harus > jumlah client streaming.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    pb_grpc.add_LayananBioskopServicer_to_server(LayananBioskop(StateBioskop(), lambat), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Server bioskop berjalan di port {port} (workers={workers}, balasan_lambat={lambat})")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--workers", type=int, default=30)
    ap.add_argument("--lambat", action="store_true",
                    help="simulasi balasan terlambat 30%% (untuk demo retry + deduplikasi)")
    a = ap.parse_args()
    serve(a.port, a.workers, a.lambat)
