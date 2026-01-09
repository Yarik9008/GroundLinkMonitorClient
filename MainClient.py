#!/usr/bin/env python3
"""
Клиент для отправки изображений/файлов на сервер через SFTP (SSH).

Поведение (совместимо с прежней логикой `.part/.done`):
- Загружаем в:   <client_name>/<upload_id>_<filename>.part
- Если `.part` уже есть — докачиваем (resume) с текущего размера
- После завершения:
  - переименовываем `.part` в `<timestamp>_<filename>`
  - пишем маркер `<upload_id>.done` с именем финального файла
"""
from dataclasses import dataclass
import sys
import os
import asyncio
import hashlib
from datetime import datetime
from typing import Optional
from Logger import Logger
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Внимание: библиотека tqdm не установлена. Прогресс-бар отключен.")
    print("Установите её командой: pip install tqdm")

# имя клиента по умолчанию
CLIENT_NAME = "R2.0S"


try:
    import paramiko
except ImportError as e:  # pragma: no cover
    raise SystemExit(
        "Не найдено 'paramiko'. Установите зависимости:\n"
        "  pip install -r GroundLinkMonitorClient/requirements.txt\n"
    ) from e


@dataclass(frozen=True)
class ClientConfig:
    server_ip: str = "130.49.146.15"
    server_port: int = 8888  # SSH/SFTP port
    username: str = "lorett"
    password: str = "lorett"
    client_name: str = "default_client"
    log_level: str = "info"

    # Производительность
    chunk_size: int = 4 * 1024 * 1024  # 4 MB
    file_buffering: int = 4 * 1024 * 1024
    connect_timeout: float = 10.0

    # Повторы при разрыве связи
    max_retries: int = 0  # 0 = бесконечно
    retry_delay_sec: float = 2.0


class SFTPUploader:
    def __init__(self, config: ClientConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self._transport: Optional["paramiko.Transport"] = None
        self._sftp: Optional["paramiko.SFTPClient"] = None

    @staticmethod
    def compute_upload_id(client_name: str, filename: str, file_size: int, path: str) -> str:
        st = os.stat(path)
        mtime_ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))
        seed = f"{client_name}|{filename}|{file_size}|{mtime_ns}".encode("utf-8")
        return hashlib.sha256(seed).hexdigest()

    @staticmethod
    def safe_filename(name: str) -> str:
        base = os.path.basename(name)
        return base.replace("/", "_").replace("\\", "_")

    def _connect_blocking(self) -> bool:
        try:
            self.logger.info(f"SFTP подключение к {self.config.server_ip}:{self.config.server_port}...")
            transport = paramiko.Transport((self.config.server_ip, self.config.server_port))
            transport.banner_timeout = self.config.connect_timeout
            transport.auth_timeout = self.config.connect_timeout
            transport.connect(username=self.config.username, password=self.config.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            self._transport = transport
            self._sftp = sftp
            self.logger.info("SFTP подключение установлено")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка SFTP подключения: {e}")
            return False

    async def connect(self) -> bool:
        return await asyncio.to_thread(self._connect_blocking)

    async def disconnect(self) -> None:
        sftp = self._sftp
        transport = self._transport
        self._sftp = None
        self._transport = None
        try:
            if sftp is not None:
                sftp.close()
        except Exception:
            pass
        try:
            if transport is not None:
                transport.close()
        except Exception:
            pass

    def _ensure_remote_dir(self, remote_dir: str) -> None:
        assert self._sftp is not None
        # Создаём цепочку директорий (POSIX-пути)
        parts = [p for p in remote_dir.strip("/").split("/") if p]
        cur = ""
        for p in parts:
            cur = f"{cur}/{p}" if cur else p
            try:
                self._sftp.stat(cur)
            except IOError:
                self._sftp.mkdir(cur)

    def _upload_blocking(self, path: str) -> bool:
        if not os.path.exists(path):
            self.logger.error(f"Файл {path} не найден")
            return False

        if self._sftp is None or self._transport is None:
            if not self._connect_blocking():
                return False
        assert self._sftp is not None

        filename = self.safe_filename(os.path.basename(path))
        file_size = os.path.getsize(path)
        upload_id = self.compute_upload_id(self.config.client_name, filename, file_size, path)

        remote_dir = self.config.client_name
        remote_part = f"{remote_dir}/{upload_id}_{filename}.part"

        self._ensure_remote_dir(remote_dir)

        # Resume: докачиваем в существующий .part
        try:
            offset = int(self._sftp.stat(remote_part).st_size)
        except IOError:
            offset = 0

        if offset > file_size:
            # странный случай: удаляем и начинаем заново
            try:
                self._sftp.remove(remote_part)
            except Exception:
                pass
            offset = 0

        remaining = file_size - offset
        self.logger.info(
            f"Загрузка SFTP: {filename} ({file_size} байт), upload_id={upload_id}, offset={offset}, осталось={remaining}"
        )

        mode = "ab" if offset > 0 else "wb"
        with open(path, "rb", buffering=self.config.file_buffering) as lf:
            lf.seek(offset)
            with self._sftp.open(remote_part, mode) as rf:
                sent = 0
                if remaining > 0 and TQDM_AVAILABLE:
                    bar_format = "{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
                    with tqdm(
                        total=remaining,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=f"Отправка {filename} (resume {offset})" if offset else f"Отправка {filename}",
                        ncols=100,
                        bar_format=bar_format,
                    ) as pbar:
                        while sent < remaining:
                            chunk = lf.read(min(self.config.chunk_size, remaining - sent))
                            if not chunk:
                                break
                            rf.write(chunk)
                            sent += len(chunk)
                            pbar.update(len(chunk))
                else:
                    while sent < remaining:
                        chunk = lf.read(min(self.config.chunk_size, remaining - sent))
                        if not chunk:
                            break
                        rf.write(chunk)
                        sent += len(chunk)

        # Проверка размера на сервере
        try:
            final_remote_size = int(self._sftp.stat(remote_part).st_size)
        except Exception as e:
            self.logger.error(f"Не удалось проверить размер remote файла: {e}")
            return False

        if final_remote_size != file_size:
            self.logger.error(f"Размер remote файла не совпал: remote={final_remote_size}, local={file_size}")
            return False

        # Финализация (rename + done marker)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        final_name = f"{timestamp}_{filename}"
        remote_final = f"{remote_dir}/{final_name}"
        remote_done = f"{remote_dir}/{upload_id}.done"

        self._sftp.rename(remote_part, remote_final)
        with self._sftp.open(remote_done, "wb") as df:
            df.write((final_name + "\n").encode("utf-8"))

        self.logger.info(f"Файл отправлен и финализирован: {remote_final}")
        return True

    async def upload(self, path: str) -> bool:
        attempt = 0
        while True:
            try:
                ok = await asyncio.to_thread(self._upload_blocking, path)
                return ok
            except Exception as e:
                attempt += 1
                self.logger.warning(f"Разрыв/ошибка SFTP: {type(e).__name__}: {e}. Переподключаюсь и продолжу...")
                await self.disconnect()
                if self.config.max_retries and attempt >= self.config.max_retries:
                    self.logger.error("Достигнут лимит попыток, загрузка не завершена")
                    return False
                await asyncio.sleep(self.config.retry_delay_sec)


class ImageClient:
    """Класс клиента для отправки изображений на сервер"""
    
    def __init__(
        self,
        server_ip="130.49.146.15",
        server_port=8888,
        username="lorett",
        password="lorett",
        client_name="default_client",
        log_level="info",
    ):
        # Создаем директорию для логов
        logs_dir = "/root/lorett/GroundLinkMonitorClient/logs"
        os.makedirs(logs_dir, exist_ok=True)
        
        # Инициализация логгера
        logger_config = {
            'log_level': log_level,
            'path_log': f'/root/lorett/GroundLinkMonitorClient/logs/image_client_{client_name}_'
        }
        self.logger = Logger(logger_config)
        self.config = ClientConfig(
            server_ip=server_ip,
            server_port=server_port,
            username=username,
            password=password,
            client_name=client_name,
            log_level=log_level,
        )
        self._uploader = SFTPUploader(self.config, self.logger)
    
    async def send_image(self, image_path):
        return await self._uploader.upload(image_path)
    
    async def disconnect(self):
        """Закрывает соединение с сервером"""
        await self._uploader.disconnect()
    
    async def __aenter__(self):
        """Async контекстный менеджер: вход"""
        await self._uploader.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async контекстный менеджер: выход"""
        await self.disconnect()
        return False


if __name__ == "__main__":
    """Основная функция"""
    if len(sys.argv) < 2:
        print("Использование: python MainClient.py <путь_к_изображению>")
        print("Пример: python MainClient.py C:\\path\\to\\image.jpg")
        sys.exit(1)
    
    image_path = sys.argv[1]
    
    client = ImageClient(client_name=CLIENT_NAME)
    
    async def _main():
        try:
            success = await client.send_image(image_path)
            return 0 if success else 1
        finally:
            await client.disconnect()

    sys.exit(asyncio.run(_main()))
