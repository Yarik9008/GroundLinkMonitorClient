#!/usr/bin/env python3
"""
Клиент для отправки изображений на сервер
"""
from dataclasses import dataclass
import struct
import sys
import os
import asyncio
import socket
import hashlib
from typing import Optional, Tuple
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

@dataclass(frozen=True)
class ClientConfig:
    server_ip: str = "130.49.146.15"
    server_port: int = 8888
    client_name: str = "default_client"
    log_level: str = "info"

    # Производительность
    chunk_size: int = 4 * 1024 * 1024  # 4 MB
    stream_limit: int = 8 * 1024 * 1024
    file_buffering: int = 4 * 1024 * 1024

    # Буферизация записи: не вызываем drain на каждый чанк
    write_buffer_high: int = 32 * 1024 * 1024
    write_buffer_low: int = 8 * 1024 * 1024

    # Таймауты (секунды)
    connect_timeout: float = 10.0
    response_timeout: float = 30.0
    offset_timeout: float = 120.0
    drain_timeout: float = 30.0

    # Повторы при разрыве связи
    max_retries: int = 0  # 0 = бесконечно
    retry_delay_sec: float = 2.0


class ProtocolV2:
    @staticmethod
    def pack_string(value: str) -> bytes:
        b = value.encode("utf-8")
        return struct.pack("!I", len(b)) + b

    @staticmethod
    def pack_u64(value: int) -> bytes:
        return struct.pack("!Q", int(value))

    @staticmethod
    async def read_u64(reader: asyncio.StreamReader) -> int:
        data = await reader.readexactly(8)
        return struct.unpack("!Q", data)[0]

    @staticmethod
    def build_header(client_name: str, filename: str, file_size: int, upload_id: str) -> bytes:
        # client_name(str) + file_size(u64) + filename(str) + upload_id(str)
        return (
            ProtocolV2.pack_string(client_name)
            + ProtocolV2.pack_u64(file_size)
            + ProtocolV2.pack_string(filename)
            + ProtocolV2.pack_string(upload_id)
        )


class AsyncConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.transport = getattr(writer, "transport", None)

    def tune_socket(self, recv_buf: int = 4 * 1024 * 1024, send_buf: int = 4 * 1024 * 1024) -> None:
        sock = self.writer.get_extra_info("socket")
        if isinstance(sock, socket.socket):
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, recv_buf)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buf)
            except Exception:
                pass

    def set_write_buffer_limits(self, high: int, low: int) -> None:
        if self.transport is None:
            return
        try:
            self.transport.set_write_buffer_limits(high=high, low=low)
        except Exception:
            pass

    async def close(self) -> None:
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass


class ResumableUploader:
    def __init__(self, config: ClientConfig, logger: Logger):
        self.config = config
        self.logger = logger
        self._conn: Optional[AsyncConnection] = None

    @staticmethod
    def compute_upload_id(client_name: str, filename: str, file_size: int, path: str) -> str:
        st = os.stat(path)
        mtime_ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))
        seed = f"{client_name}|{filename}|{file_size}|{mtime_ns}".encode("utf-8")
        return hashlib.sha256(seed).hexdigest()

    async def connect(self) -> bool:
        try:
            self.logger.info(f"Подключение к серверу {self.config.server_ip}:{self.config.server_port}...")
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(
                    self.config.server_ip,
                    self.config.server_port,
                    limit=max(self.config.stream_limit, self.config.chunk_size * 2),
                ),
                timeout=self.config.connect_timeout,
            )
            conn = AsyncConnection(reader, writer)
            conn.tune_socket()
            conn.set_write_buffer_limits(high=self.config.write_buffer_high, low=self.config.write_buffer_low)
            self._conn = conn
            self.logger.info("Подключение установлено")
            return True
        except asyncio.TimeoutError:
            self.logger.error(f"Таймаут при подключении к серверу {self.config.server_ip}:{self.config.server_port}")
            return False
        except ConnectionRefusedError:
            self.logger.error(f"Не удалось подключиться к серверу {self.config.server_ip}:{self.config.server_port}")
            self.logger.warning("Убедитесь, что сервер запущен")
            return False
        except Exception as e:
            self.logger.error(f"Ошибка при подключении: {e}")
            return False

    async def disconnect(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None
            self.logger.debug("Соединение закрыто")

    async def _drain_if_needed(self) -> None:
        if not self._conn:
            raise ConnectionError("Нет подключения к серверу")
        await asyncio.wait_for(self._conn.writer.drain(), timeout=self.config.drain_timeout)

    async def _send_file_region(self, file_obj, size: int, show_progress: bool, desc: str) -> int:
        if not self._conn:
            raise ConnectionError("Нет подключения к серверу")
        conn = self._conn
        sent_total = 0

        def wrote(chunk: bytes) -> None:
            if conn.transport is not None:
                conn.transport.write(chunk)
            else:
                conn.writer.write(chunk)

        async def maybe_drain() -> None:
            if conn.transport is not None and conn.transport.get_write_buffer_size() <= self.config.write_buffer_high:
                return
            await self._drain_if_needed()

        if show_progress and TQDM_AVAILABLE:
            bar_format = "{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
            with tqdm(total=size, unit="B", unit_scale=True, unit_divisor=1024, desc=desc, ncols=100, bar_format=bar_format) as pbar:
                while sent_total < size:
                    chunk = file_obj.read(min(self.config.chunk_size, size - sent_total))
                    if not chunk:
                        break
                    wrote(chunk)
                    await maybe_drain()
                    sent_total += len(chunk)
                    pbar.update(len(chunk))
        else:
            while sent_total < size:
                chunk = file_obj.read(min(self.config.chunk_size, size - sent_total))
                if not chunk:
                    break
                wrote(chunk)
                await maybe_drain()
                sent_total += len(chunk)

        await self._drain_if_needed()
        return sent_total

    async def upload(self, path: str) -> bool:
        if not os.path.exists(path):
            self.logger.error(f"Файл {path} не найден")
            return False

        filename = os.path.basename(path)
        file_size = os.path.getsize(path)
        upload_id = self.compute_upload_id(self.config.client_name, filename, file_size, path)

        attempt = 0
        while True:
            try:
                if not self._conn:
                    if not await self.connect():
                        return False

                assert self._conn is not None
                conn = self._conn

                self.logger.info(f"Загрузка: {filename} ({file_size} байт), upload_id={upload_id}, попытка={attempt+1}")

                conn.writer.write(ProtocolV2.build_header(self.config.client_name, filename, file_size, upload_id))
                await conn.writer.drain()

                try:
                    offset = await asyncio.wait_for(ProtocolV2.read_u64(conn.reader), timeout=self.config.offset_timeout)
                except asyncio.TimeoutError as e:
                    raise ConnectionError(f"Таймаут ожидания offset от сервера ({self.config.offset_timeout}s)") from e

                if offset > file_size:
                    raise ConnectionError(f"Сервер вернул некорректный offset={offset} > size={file_size}")

                remaining = file_size - offset
                if remaining > 0:
                    self.logger.info(f"Продолжаю с offset={offset}, осталось отправить {remaining} байт")
                    with open(path, "rb", buffering=self.config.file_buffering) as f:
                        f.seek(offset)
                        sent = await self._send_file_region(
                            f, remaining, show_progress=True, desc=f"Отправка {filename} (resume {offset})"
                        )
                    if sent != remaining:
                        raise ConnectionError(f"Отправлено {sent}/{remaining} байт (ожидалось {remaining})")
                else:
                    self.logger.info("Сервер сообщает: файл уже полностью загружен, жду подтверждение")

                try:
                    response = await asyncio.wait_for(conn.reader.readexactly(2), timeout=self.config.response_timeout)
                except asyncio.TimeoutError as e:
                    raise ConnectionError(f"Таймаут ожидания ответа OK/ER ({self.config.response_timeout}s)") from e

                if response == b"OK":
                    self.logger.info("Сервер подтвердил получение файла")
                    return True
                if response == b"ER":
                    self.logger.error("Сервер вернул ошибку")
                    return False
                self.logger.warning(f"Неожиданный ответ от сервера: {response!r}")
                return False

            except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, OSError, ConnectionError, asyncio.TimeoutError) as e:
                attempt += 1
                self.logger.warning(f"Разрыв/ошибка передачи: {type(e).__name__}: {e}. Переподключаюсь и продолжу...")
                await self.disconnect()
                if self.config.max_retries and attempt >= self.config.max_retries:
                    self.logger.error("Достигнут лимит попыток, загрузка не завершена")
                    return False
                await asyncio.sleep(self.config.retry_delay_sec)


class ImageClient:
    """Класс клиента для отправки изображений на сервер"""
    
    def __init__(self, server_ip="130.49.146.15", server_port=8888, client_name="default_client", log_level="info"):
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
            client_name=client_name,
            log_level=log_level,
        )
        self._uploader = ResumableUploader(self.config, self.logger)
    
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
