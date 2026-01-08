#!/usr/bin/env python3
"""
Клиент для отправки изображений на сервер
"""
import struct
import sys
import os
import asyncio
import socket
import hashlib
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

# Размер чанка для передачи данных (1 MB) - должен совпадать с размером на сервере
# Больший чанк снижает накладные расходы на syscalls и копирование
CHUNK_SIZE = 1024 * 1024  # 1 MB

# Таймауты (секунды)
CONNECT_TIMEOUT = 10.0
RESPONSE_TIMEOUT = 30.0
OFFSET_TIMEOUT = 30.0
# Если канал "завис" (нет возможности сбросить данные в ОС), форсируем переподключение
DRAIN_TIMEOUT = 30.0

# Повторы при разрыве связи
MAX_RETRIES = 0  # 0 = бесконечно
RETRY_DELAY_SEC = 2.0


class ImageClient:
    """Класс клиента для отправки изображений на сервер"""
    
    def __init__(self, server_ip="130.49.146.15", server_port=8888, client_name="default_client", log_level="info"):
        """
        Инициализация клиента
        
        Args:
            server_ip: IP адрес сервера
            server_port: Порт сервера
            client_name: Имя клиента
            log_level: Уровень логирования (debug, info, warning, error, critical)
        """
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_name = client_name
        self.client_socket = None
        
        # Создаем директорию для логов
        logs_dir = "/root/lorett/GroundLinkMonitorClient/logs"
        os.makedirs(logs_dir, exist_ok=True)
        
        # Инициализация логгера
        logger_config = {
            'log_level': log_level,
            'path_log': f'/root/lorett/GroundLinkMonitorClient/logs/image_client_{client_name}_'
        }
        self.logger = Logger(logger_config)
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
    
    def _pack_string(self, string):
        """
        Упаковывает строку для отправки (сначала длина, затем данные)
        
        Args:
            string: Строка для отправки
        """
        string_bytes = string.encode('utf-8')
        return struct.pack('!I', len(string_bytes)) + string_bytes

    def _pack_u64(self, value: int) -> bytes:
        return struct.pack("!Q", int(value))

    async def _read_u64(self) -> int:
        if not self._reader:
            raise ConnectionError("Нет подключения к серверу")
        data = await self._reader.readexactly(8)
        return struct.unpack("!Q", data)[0]

    def _compute_upload_id(self, filename: str, file_size: int, image_path: str) -> str:
        st = os.stat(image_path)
        mtime_ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1_000_000_000))
        seed = f"{self.client_name}|{filename}|{file_size}|{mtime_ns}".encode("utf-8")
        return hashlib.sha256(seed).hexdigest()

    def _build_header(self, filename, image_size):
        """
        Собирает заголовок одним куском (меньше syscalls/пакетов).
        Протокол v2:
          client_name(str) + file_size(uint64) + filename(str) + upload_id(str)
        """
        raise RuntimeError("Используйте _build_header_v2()")

    def _build_header_v2(self, filename: str, file_size: int, upload_id: str) -> bytes:
        return (
            self._pack_string(self.client_name)
            + self._pack_u64(file_size)
            + self._pack_string(filename)
            + self._pack_string(upload_id)
        )

    async def _send_file_stream(self, file_obj, size, show_progress=False, desc="Отправка"):
        """
        Потоково отправляет файл, не читая его целиком в память.
        """
        if not self._writer:
            raise ConnectionError("Нет подключения к серверу")

        writer = self._writer
        sent_total = 0

        if show_progress and TQDM_AVAILABLE:
            bar_format = '{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
            with tqdm(total=size, unit='B', unit_scale=True, unit_divisor=1024,
                      desc=desc, ncols=100, bar_format=bar_format) as pbar:
                while sent_total < size:
                    chunk = file_obj.read(min(CHUNK_SIZE, size - sent_total))
                    if not chunk:
                        break
                    writer.write(chunk)
                    # Если сеть "зависла", drain может ждать бесконечно — ограничиваем
                    await asyncio.wait_for(writer.drain(), timeout=DRAIN_TIMEOUT)
                    sent_total += len(chunk)
                    pbar.update(len(chunk))
        else:
            while sent_total < size:
                chunk = file_obj.read(min(CHUNK_SIZE, size - sent_total))
                if not chunk:
                    break
                writer.write(chunk)
                await asyncio.wait_for(writer.drain(), timeout=DRAIN_TIMEOUT)
                sent_total += len(chunk)

        return sent_total
    
    async def connect(self):
        """
        Подключается к серверу
        
        Returns:
            bool: True если подключение успешно, False в противном случае
        """
        try:
            self.logger.info(f"Подключение к серверу {self.server_ip}:{self.server_port}...")

            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.server_ip, self.server_port),
                timeout=CONNECT_TIMEOUT,
            )

            sock = self._writer.get_extra_info("socket")
            if isinstance(sock, socket.socket):
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
                except Exception:
                    pass

            self.logger.info("Подключение установлено")
            return True
        except asyncio.TimeoutError:
            self.logger.error(f"Таймаут при подключении к серверу {self.server_ip}:{self.server_port}")
            return False
        except ConnectionRefusedError:
            self.logger.error(f"Не удалось подключиться к серверу {self.server_ip}:{self.server_port}")
            self.logger.warning("Убедитесь, что сервер запущен")
            return False
        except Exception as e:
            self.logger.error(f"Ошибка при подключении: {e}")
            return False
    
    async def send_image(self, image_path):
        """
        Отправляет изображение на сервер
        
        Args:
            image_path: Путь к изображению
            
        Returns:
            bool: True если отправка успешна, False в противном случае
        """
        if not os.path.exists(image_path):
            self.logger.error(f"Файл {image_path} не найден")
            return False

        filename = os.path.basename(image_path)
        file_size = os.path.getsize(image_path)
        upload_id = self._compute_upload_id(filename=filename, file_size=file_size, image_path=image_path)

        attempt = 0
        while True:
            try:
                if not self._writer or not self._reader:
                    if not await self.connect():
                        return False

                if not self._writer or not self._reader:
                    raise ConnectionError("Нет подключения к серверу")

                self.logger.info(f"Загрузка: {filename} ({file_size} байт), upload_id={upload_id}, попытка={attempt+1}")

                # Отправляем заголовок v2 и получаем оффсет для продолжения
                self._writer.write(self._build_header_v2(filename=filename, file_size=file_size, upload_id=upload_id))
                await self._writer.drain()

                offset = await asyncio.wait_for(self._read_u64(), timeout=OFFSET_TIMEOUT)
                if offset > file_size:
                    raise ConnectionError(f"Сервер вернул некорректный offset={offset} > size={file_size}")

                remaining = file_size - offset
                if remaining > 0:
                    self.logger.info(f"Продолжаю с offset={offset}, осталось отправить {remaining} байт")
                    with open(image_path, "rb", buffering=4 * 1024 * 1024) as f:
                        f.seek(offset)
                        sent = await self._send_file_stream(
                            f,
                            remaining,
                            show_progress=True,
                            desc=f"Отправка {filename} (resume {offset})",
                        )
                    if sent != remaining:
                        raise ConnectionError(f"Отправлено {sent}/{remaining} байт (ожидалось {remaining})")
                else:
                    self.logger.info("Сервер сообщает: файл уже полностью загружен, жду подтверждение")

                response = await asyncio.wait_for(self._reader.readexactly(2), timeout=RESPONSE_TIMEOUT)
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
                self.logger.warning(f"Разрыв/ошибка передачи: {e}. Переподключаюсь и продолжу...")
                await self.disconnect()
                if MAX_RETRIES and attempt >= MAX_RETRIES:
                    self.logger.error("Достигнут лимит попыток, загрузка не завершена")
                    return False
                await asyncio.sleep(RETRY_DELAY_SEC)
    
    async def disconnect(self):
        """Закрывает соединение с сервером"""
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None
            self.logger.debug("Соединение закрыто")
    
    async def __aenter__(self):
        """Async контекстный менеджер: вход"""
        await self.connect()
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
