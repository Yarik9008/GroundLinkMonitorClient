#!/usr/bin/env python3
import os
import subprocess
import sys
from pathlib import Path

# ====== Static config ======
SERVER_IP = "130.49.146.15"
SERVER_PORT = 1234
USERNAME = "sftpuser"
PASSWORD = "sftppass123"
# ===========================

REMOTE_DIR = "/uploads"  # внутри chroot это uploads/

SFTP_BUFFER_SIZE = 4 * 1024 * 1024  # sftp -B (буфер I/O)
SFTP_NUM_REQUESTS = 256             # sftp -R (кол-во параллельных запросов)

# OpenSSH key (по умолчанию). Если ключа нет — сгенерируем.
DEFAULT_KEY_PATH = Path.home() / ".ssh" / "lorett_sftp_ed25519"

# Для простоты (в проде лучше хранить known_hosts)
SFTP_STRICT_HOSTKEY = False


def _which(cmd: str) -> str | None:
    from shutil import which

    return which(cmd)


def ensure_keypair(private_key_path: Path) -> Path:
    private_key_path = private_key_path.expanduser()
    public_key_path = private_key_path.with_suffix(private_key_path.suffix + ".pub")

    if private_key_path.exists() and public_key_path.exists():
        return public_key_path

    private_key_path.parent.mkdir(parents=True, exist_ok=True)
    ssh_keygen = _which("ssh-keygen")
    if not ssh_keygen:
        raise SystemExit("ssh-keygen not found. Install OpenSSH client tools.")

    cmd = [
        ssh_keygen,
        "-t",
        "ed25519",
        "-f",
        str(private_key_path),
        "-N",
        "",
        "-C",
        "lorett-sftp",
    ]
    subprocess.run(cmd, check=True)
    if not public_key_path.exists():
        raise SystemExit(f"Failed to generate public key: {public_key_path}")
    return public_key_path


def run_sftp_put(local_path: Path, remote_path: str, key_path: Path) -> None:
    sftp = _which("sftp")
    if not sftp:
        raise SystemExit("sftp not found. Install OpenSSH client tools.")

    opts: list[str] = []
    if not SFTP_STRICT_HOSTKEY:
        opts += [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            f"UserKnownHostsFile={os.devnull}",
        ]

    # В batch-файле обязательно кавычки, иначе пути с пробелами ломаются.
    batch = f'put "{local_path}" "{remote_path}"\nquit\n'

    def build_cmd(batch_mode: bool) -> list[str]:
        # IMPORTANT: all -o options must appear BEFORE destination, otherwise sftp prints usage and exits.
        bm = "yes" if batch_mode else "no"
        return [
            sftp,
            "-P",
            str(SERVER_PORT),
            "-i",
            str(key_path),
            "-B",
            str(SFTP_BUFFER_SIZE),
            "-R",
            str(SFTP_NUM_REQUESTS),
            *opts,
            "-o",
            f"BatchMode={bm}",
            f"{USERNAME}@{SERVER_IP}",
        ]

    # 1) Быстрая попытка только по ключу (не зависнуть на вводе пароля).
    proc = subprocess.run(build_cmd(batch_mode=True), input=batch, text=True)
    if proc.returncode == 0:
        return

    # 2) Если ключ не установлен, но запуск интерактивный — дадим шанс ввести пароль.
    if sys.stdin.isatty():
        print("[client] Key auth failed; falling back to interactive password prompt...")
        proc2 = subprocess.run(build_cmd(batch_mode=False), input=batch, text=True)
        if proc2.returncode == 0:
            return
        raise SystemExit(proc2.returncode)

    raise SystemExit(proc.returncode)


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python3 MainClient.py <path_to_file>")
        raise SystemExit(2)

    local_path = Path(sys.argv[1])
    if not local_path.is_file():
        raise SystemExit(f"File not found: {local_path}")

    # Ключ по умолчанию можно переопределить через LORETT_SFTP_KEY
    key_path = Path(os.environ.get("LORETT_SFTP_KEY", str(DEFAULT_KEY_PATH))).expanduser()
    pub_path = ensure_keypair(key_path)

    remote_path = f"{REMOTE_DIR}/{local_path.name}"

    if not key_path.exists():
        raise SystemExit(f"Private key not found: {key_path}")

    print("[client] Using OpenSSH sftp (internal-sftp on server)")
    print(f"[client] Identity key: {key_path}")
    print(f"[client] Public key to install on server: {pub_path}")
    print(f"[client] Uploading: {local_path} -> {remote_path}")

    run_sftp_put(local_path, remote_path, key_path)
    print(f"[client] Done: {local_path} -> {remote_path}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, subprocess.SubprocessError) as e:
        print(f"[client] ERROR: {e}")
        raise
