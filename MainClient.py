#!/usr/bin/env python3
import os
import sys
import posixpath
import paramiko

# ====== Static config (as requested) ======
SERVER_IP = "130.49.146.15"
SERVER_PORT = 1234
USERNAME = "sftpuser"
PASSWORD = "sftppass123"
# =========================================


def main():
    if len(sys.argv) < 2:
        print("Usage: python sftp_client.py <path_to_file>")
        sys.exit(2)

    local_path = sys.argv[1]
    if not os.path.isfile(local_path):
        print(f"File not found: {local_path}")
        sys.exit(2)

    filename = os.path.basename(local_path)
    remote_path = posixpath.join("/", filename)  # upload into server root (uploads folder on server)

    transport = paramiko.Transport((SERVER_IP, SERVER_PORT))
    try:
        transport.connect(username=USERNAME, password=PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        print(f"[client] Uploading {local_path} -> {remote_path} on {SERVER_IP}:{SERVER_PORT}")
        sftp.put(local_path, remote_path)

        print("[client] Done.")
        sftp.close()
    finally:
        transport.close()


if __name__ == "__main__":
    main()
