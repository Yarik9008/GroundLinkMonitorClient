#!/usr/bin/env python3
"""
Минимальный пример Paramiko (SFTP client).

Источник: репозиторий Paramiko — https://github.com/paramiko/paramiko

Примеры:
    python3 MainClient.py ./test_upload.txt
"""

import argparse
from pathlib import Path
import paramiko

# ===== Статические настройки подключения =====
HOST = "130.49.146.15"
PORT = 2222
USERNAME = "demo"
PASSWORD = "secret"
# ============================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Простой SFTP клиент на Paramiko (загрузка одного файла)")
    parser.add_argument("local_path", help="Путь к локальному файлу, который нужно передать на сервер")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    local_path = Path(args.local_path)
    if not local_path.exists():
        raise SystemExit(f"Файл не найден: {local_path}")
    if not local_path.is_file():
        raise SystemExit(f"Это не файл: {local_path}")

    client = paramiko.SSHClient()
    # ВНИМАНИЕ: AutoAddPolicy удобен для демо, но не рекомендуется для production.
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(HOST, port=PORT, username=USERNAME, password=PASSWORD, timeout=10)

        sftp = client.open_sftp()
        try:
            remote_path = f"/{local_path.name}"
            sftp.put(str(local_path), remote_path)
            print(f"Uploaded: {local_path} -> {remote_path}")
        finally:
            sftp.close()
    finally:
        client.close()