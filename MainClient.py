#!/usr/bin/env python3
import asyncio
import sys
from pathlib import Path

import asyncssh
from tqdm import tqdm

# ====== Static config (as requested) ======
SERVER_IP = "130.49.146.15"
SERVER_PORT = 1234
USERNAME = "sftpuser"
PASSWORD = "sftppass123"
# =========================================

# Performance tuning (large files / high throughput)
# Avoid frequent SSH rekey during multi-GB transfers.
REKEY_BYTES = 16 * 1024 * 1024 * 1024  # 16 GiB
# Disable compression for max throughput (and CPU savings)
COMPRESSION_ALGS = ["none"]
# Prefer fast ciphers (hardware AES if available, otherwise ChaCha20)
ENCRYPTION_ALGS = [
    "aes128-gcm@openssh.com",
    "aes256-gcm@openssh.com",
    "chacha20-poly1305@openssh.com",
    "aes128-ctr",
    "aes256-ctr",
]


def format_bytes(size):
    """Format bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

class LorettSFTPClient:
    """Single client class: connect + upload with fast SFTP and progress bar."""

    def __init__(self, host: str, port: int, username: str, password: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    async def upload(self, local_path: str) -> None:
        local_file = Path(local_path)
        if not local_file.is_file():
            raise FileNotFoundError(local_path)

        filename = local_file.name
        remote_path = f"/{filename}"
        file_size = local_file.stat().st_size

        def _is_winerror_121(exc: BaseException) -> bool:
            if not isinstance(exc, OSError):
                return False
            if getattr(exc, "winerror", None) == 121:
                return True
            if getattr(exc, "errno", None) == 121:
                return True
            return "WinError 121" in str(exc)

        max_attempts = 5
        backoff = 2.0

        for attempt in range(1, max_attempts + 1):
            print(f"[client] Connecting to {self.host}:{self.port}... (attempt {attempt}/{max_attempts})")
            try:
                async with asyncssh.connect(
                    self.host,
                    port=self.port,
                    username=self.username,
                    password=self.password,
                    known_hosts=None,  # demo / no host key verification
                    rekey_bytes=REKEY_BYTES,
                    compression_algs=COMPRESSION_ALGS,
                    encryption_algs=ENCRYPTION_ALGS,
                    # Keepalives reduce chance of long-transfer timeouts
                    keepalive_interval=15,
                    keepalive_count_max=3,
                ) as conn:
                    print("[client] Connected! Starting SFTP session...")

                    async with conn.start_sftp_client() as sftp:
                        # best-effort cleanup of partial file from previous attempts
                        if attempt > 1:
                            try:
                                await sftp.remove(remote_path)
                            except Exception:
                                pass

                        print(f"[client] File: {filename} ({format_bytes(file_size)})")

                        with tqdm(
                            total=file_size,
                            unit="B",
                            unit_scale=True,
                            unit_divisor=1024,
                            desc=f"Uploading {filename}",
                            ncols=80,
                            # Make UI updates cheap (Windows console is slow).
                            mininterval=1.0,
                            smoothing=0,
                            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
                            disable=not sys.stderr.isatty(),
                        ) as pbar:
                            block_size = 4 * 1024 * 1024  # 4 MiB
                            max_requests = 128

                            progress_step = 16 * 1024 * 1024  # update every 16 MiB
                            last_reported = 0

                            def progress_handler(_src: bytes, _dst: bytes, bytes_copied: int, total_bytes: int) -> None:
                                nonlocal last_reported
                                if bytes_copied < last_reported:
                                    return
                                delta = bytes_copied - last_reported
                                if delta >= progress_step or bytes_copied >= total_bytes:
                                    pbar.update(delta)
                                    last_reported = bytes_copied

                            await sftp.put(
                                str(local_file),
                                remote_path,
                                block_size=block_size,
                                max_requests=max_requests,
                                progress_handler=progress_handler,
                            )
                            if last_reported < file_size:
                                pbar.update(file_size - last_reported)

                        print("[client] ✓ Upload complete!")
                        attrs = await sftp.stat(remote_path)
                        print(f"[client] Remote file size: {format_bytes(attrs.size)}")
                        return
            except asyncssh.PermissionDenied:
                print("[client] Error: Permission denied (check username/password)")
                raise SystemExit(1)
            except asyncssh.Error as e:
                cause = getattr(e, "__cause__", None)
                if _is_winerror_121(e) or (cause is not None and _is_winerror_121(cause)):
                    print("[client] Warning: WinError 121 (semaphore timeout). Retrying...")
                else:
                    print(f"[client] SSH Error: {e}")
                    raise SystemExit(1)
            except OSError as e:
                if _is_winerror_121(e):
                    print("[client] Warning: WinError 121 (semaphore timeout). Retrying...")
                else:
                    print(f"[client] OS Error: {e}")
                    raise SystemExit(1)

            if attempt < max_attempts:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 30.0)
            else:
                raise SystemExit("[client] Failed after retries (WinError 121).")


async def main():
    if len(sys.argv) < 2:
        print("Usage: python MainClient.py <path_to_file>")
        raise SystemExit(2)

    client = LorettSFTPClient(SERVER_IP, SERVER_PORT, USERNAME, PASSWORD)
    await client.upload(sys.argv[1])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[client] Interrupted by user")
        sys.exit(130)
