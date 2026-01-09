#!/usr/bin/env python3
"""
Простой SFTP‑клиент на Paramiko.

Автоматически загружает локальный файл на сервер.
Удаленный путь формируется из имени файла в корне сервера.

Примеры:
    python MainClient.py ./data.txt
    python MainClient.py /path/to/file.log

SFTP хост/порт и учетные данные заданы статически: 130.49.146.15:2222 (demo/secret).
"""

import argparse
import sys
from pathlib import Path

import paramiko

# Статически заданные сетевые параметры и учетные данные
HOST = "130.49.146.15"
PORT = 2222
USER = "demo"
PASSWORD = "secret"


def connect_sftp():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connect_kwargs = {
        "hostname": HOST,
        "port": PORT,
        "username": USER,
        "timeout": 10,
        "password": PASSWORD,
    }

    ssh.connect(**connect_kwargs)
    return ssh, ssh.open_sftp()


def upload_file(sftp: paramiko.SFTPClient, local_path: str, remote_path: str):
    local_path = Path(local_path)
    if not local_path.exists():
        raise SystemExit(f"Ошибка: файл '{local_path}' не найден")
    if not local_path.is_file():
        raise SystemExit(f"Ошибка: '{local_path}' не является файлом")

    total_size = local_path.stat().st_size

    def progress(sent, _total):
        percent = (sent / total_size) * 100 if total_size else 100
        sys.stdout.write(f"\rОтправлено {sent}/{total_size} байт ({percent:.1f}%)")
        sys.stdout.flush()

    sftp.put(str(local_path), remote_path, callback=progress)
    print(f"\nФайл '{local_path.name}' успешно загружен на сервер как '{remote_path}'")


def main():
    parser = argparse.ArgumentParser(description="Простой SFTP клиент на Paramiko")
    parser.add_argument("local_path", help="Путь к локальному файлу для загрузки")
    args = parser.parse_args()

    local_path = Path(args.local_path)
    # Формируем удаленный путь из имени файла (в корне сервера)
    remote_path = f"/{local_path.name}"

    ssh, sftp = connect_sftp()
    try:
        upload_file(sftp, str(local_path), remote_path)
    finally:
        sftp.close()
        ssh.close()


if __name__ == "__main__":
    main()