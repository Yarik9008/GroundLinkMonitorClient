#!/usr/bin/env python3
import asyncio
import sys
from pathlib import Path
import asyncssh

# ====== Static config (как вы просили) ======
SERVER_IP = "130.49.146.15"
SERVER_PORT = 1234
USERNAME = "sftpuser"
PASSWORD = "sftppass123"
# ===========================================

REMOTE_DIR = "/"  # внутри chroot это uploads/


async def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py <path_to_file>")
        raise SystemExit(2)

    local_path = Path(sys.argv[1])
    if not local_path.is_file():
        raise SystemExit(f"File not found: {local_path}")

    async with asyncssh.connect(
        SERVER_IP,
        port=SERVER_PORT,
        username=USERNAME,
        password=PASSWORD,
        known_hosts=None,  # для простоты (в проде лучше проверять host key)
    ) as conn:
        async with conn.start_sftp_client() as sftp:
            remote_path = f"{REMOTE_DIR}{local_path.name}"
            await sftp.put(str(local_path), remote_path)

    print(f"[client] Uploaded: {local_path} -> {remote_path}")


if __name__ == "__main__":
    asyncio.run(main())
