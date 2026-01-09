#!/usr/bin/env python3
import asyncio
import os
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


def format_bytes(size):
    """Format bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


async def upload_file(local_path: str):
    """Upload a file to the SFTP server."""
    local_file = Path(local_path)
    if not local_file.is_file():
        print(f"[client] Error: File not found: {local_path}")
        sys.exit(2)
    
    filename = local_file.name
    remote_path = f"/{filename}"
    file_size = local_file.stat().st_size
    
    print(f"[client] Connecting to {SERVER_IP}:{SERVER_PORT}...")
    
    try:
        async with asyncssh.connect(
            SERVER_IP,
            port=SERVER_PORT,
            username=USERNAME,
            password=PASSWORD,
            known_hosts=None,  # Skip host key verification (for demo)
        ) as conn:
            print(f"[client] Connected! Starting SFTP session...")
            
            async with conn.start_sftp_client() as sftp:
                print(f"[client] File: {filename} ({format_bytes(file_size)})")
                
                # Create progress bar
                with tqdm(
                    total=file_size,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"Uploading {filename}",
                    ncols=80,
                    bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
                ) as pbar:
                    
                    # Upload file in chunks with progress tracking
                    chunk_size = 32768  # 32KB chunks
                    bytes_sent = 0
                    
                    async with await sftp.open(remote_path, 'wb') as remote_file:
                        with open(local_file, 'rb') as local_file_obj:
                            while True:
                                chunk = local_file_obj.read(chunk_size)
                                if not chunk:
                                    break
                                await remote_file.write(chunk)
                                bytes_sent += len(chunk)
                                pbar.n = bytes_sent
                                pbar.refresh()
                
                print(f"[client] ✓ Upload complete!")
                
                # Verify file was uploaded
                attrs = await sftp.stat(remote_path)
                print(f"[client] Remote file size: {format_bytes(attrs.size)}")
                
    except asyncssh.PermissionDenied:
        print("[client] Error: Permission denied (check username/password)")
        sys.exit(1)
    except asyncssh.Error as e:
        print(f"[client] SSH Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"[client] Unexpected error: {e}")
        sys.exit(1)


async def main():
    """Main client function."""
    if len(sys.argv) < 2:
        print("Usage: python MainClient.py <path_to_file>")
        sys.exit(2)
    
    local_path = sys.argv[1]
    await upload_file(local_path)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[client] Interrupted by user")
        sys.exit(130)
