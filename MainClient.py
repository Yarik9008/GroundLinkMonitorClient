#!/usr/bin/env python3
import asyncio
import sys
import time
from pathlib import Path

import asyncssh

# ====== Static config ======
SERVER_IP = "130.49.146.15"
SERVER_PORT = 1234
USERNAME = "sftpuser"
PASSWORD = "sftppass123"
# ===========================

REMOTE_DIR = "/"  # внутри chroot это uploads/

# Тюнинг скорости SFTP
BLOCK_SIZE = 256 * 1024   # 256KB блоки (часто быстрее дефолта)
MAX_REQUESTS = 128        # параллельные запросы (увеличивает throughput)

# Тюнинг UI прогресса (частый print/flush может резать скорость)
PROGRESS_MIN_INTERVAL_S = 2.0  # не чаще, чем раз в N секунд
PROGRESS_MIN_BYTES = 512 * 1024  # или при приросте >= N байт (512KB)


def format_bytes(n: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    return f"{n:.2f}{units[i]}"


def make_progress_printer():
    """
    Простой прогресс-бар + текущая скорость + ETA.
    Используется через progress_handler в asyncssh sftp.put()
    """
    now0 = time.monotonic()
    state = {"t0": now0, "last_t": now0, "last_n": 0}

    def progress(srcpath, dstpath, transferred, total):
        now = time.monotonic()
        is_final = bool(total) and transferred >= total

        # Throttle progress updates to reduce overhead on fast links.
        dt_emit = now - state["last_t"]
        dn_emit = transferred - state["last_n"]
        if not is_final and dt_emit < PROGRESS_MIN_INTERVAL_S and dn_emit < PROGRESS_MIN_BYTES:
            return

        dt = max(now - state["last_t"], 1e-6)
        dn = transferred - state["last_n"]
        speed = dn / dt  # bytes/s

        elapsed = now - state["t0"]
        eta = (total - transferred) / speed if speed > 0 and total else 0.0

        pct = (transferred / total * 100) if total else 0.0
        bar_len = 30
        filled = int(bar_len * pct / 100) if total else 0
        bar = "#" * filled + "-" * (bar_len - filled)

        line = (
            f"\r[{bar}] {pct:6.2f}%  "
            f"{format_bytes(transferred)}/{format_bytes(total)}  "
            f"{format_bytes(speed)}/s  "
            f"elapsed {elapsed:,.1f}s  eta {eta:,.1f}s"
        )
        print(line, end="", flush=True)

        state["last_t"] = now
        state["last_n"] = transferred

    return progress


async def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py <path_to_file>")
        raise SystemExit(2)

    local_path = Path(sys.argv[1])
    if not local_path.is_file():
        raise SystemExit(f"File not found: {local_path}")

    progress = make_progress_printer()

    async with asyncssh.connect(
        SERVER_IP,
        port=SERVER_PORT,
        username=USERNAME,
        password=PASSWORD,
        known_hosts=None,  # для простоты (в проде лучше проверять host key)

        # ======= SPEED / CPU tuning =======
        # Компрессия полностью OFF
        compression_algs=["none"],

        # Быстрые шифры (порядок = приоритет)
        encryption_algs=[
            "chacha20-poly1305@openssh.com",
            "aes128-gcm@openssh.com",
            "aes256-gcm@openssh.com",
            "aes128-ctr",
        ],
        # ==================================
    ) as conn:
        async with conn.start_sftp_client() as sftp:
            remote_path = f"{REMOTE_DIR}{local_path.name}"

            await sftp.put(
                str(local_path),
                remote_path,
                block_size=BLOCK_SIZE,
                max_requests=MAX_REQUESTS,
                progress_handler=progress,
            )

    print(f"\n[client] Uploaded: {local_path} -> {remote_path}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (OSError, asyncssh.Error) as e:
        print(f"[client] ERROR: {e}")
        raise
