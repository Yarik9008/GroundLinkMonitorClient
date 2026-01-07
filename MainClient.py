#!/usr/bin/env python3
"""
Клиент для отправки изображений на сервер
"""
import socket
import struct
import sys
import os
from datetime import datetime
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
    
    def _pack_string(self, string):
        """
        Упаковывает строку для отправки (сначала длина, затем данные)
        
        Args:
            string: Строка для отправки
        """
        string_bytes = string.encode('utf-8')
        return struct.pack('!I', len(string_bytes)) + string_bytes

    def _send_header(self, filename, image_size):
        """
        Отправляет заголовок одним sendall (меньше syscalls/пакетов).
        Формат: client_name(str) + image_size(uint32) + filename(str)
        """
        header = (
            self._pack_string(self.client_name) +
            struct.pack('!I', image_size) +
            self._pack_string(filename)
        )
        self.client_socket.sendall(header)

    def _send_file_stream(self, file_obj, size, show_progress=False, desc="Отправка"):
        """
        Потоково отправляет файл, не читая его целиком в память.
        """
        sent_total = 0
        if show_progress and TQDM_AVAILABLE:
            bar_format = '{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
            with tqdm(total=size, unit='B', unit_scale=True, unit_divisor=1024,
                      desc=desc, ncols=100, bar_format=bar_format) as pbar:
                while sent_total < size:
                    chunk = file_obj.read(min(CHUNK_SIZE, size - sent_total))
                    if not chunk:
                        break
                    self.client_socket.sendall(chunk)
                    sent_total += len(chunk)
                    pbar.update(len(chunk))
        else:
            while sent_total < size:
                chunk = file_obj.read(min(CHUNK_SIZE, size - sent_total))
                if not chunk:
                    break
                self.client_socket.sendall(chunk)
                sent_total += len(chunk)

        return sent_total
    
    def _send_data(self, data, show_progress=False, desc="Отправка"):
        """
        Отправляет данные в сокет чанками для корректного отслеживания прогресса
        
        Args:
            data: Данные для отправки (bytes)
            show_progress: Показывать ли прогресс-бар
            desc: Описание для прогресс-бара
        
        Returns:
            int: Количество отправленных байт
        """
        total_sent = 0
        data_len = len(data)
        # Размер чанка для отправки - используем общую константу
        chunk_size = CHUNK_SIZE
        
        if show_progress and TQDM_AVAILABLE:
            # bar_format с отображением скорости: {rate_fmt} показывает текущую скорость
            bar_format = '{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
            with tqdm(total=data_len, unit='B', unit_scale=True, unit_divisor=1024, 
                      desc=desc, ncols=100, bar_format=bar_format) as pbar:
                while total_sent < data_len:
                    try:
                        # Определяем размер текущего чанка
                        end = min(total_sent + chunk_size, data_len)
                        chunk = data[total_sent:end]
                        # Используем sendall для гарантированной отправки всего чанка
                        self.client_socket.sendall(chunk)
                        sent = len(chunk)
                        total_sent += sent
                        pbar.update(sent)
                    except socket.timeout:
                        self.logger.error("Таймаут при отправке данных")
                        raise ConnectionError("Превышено время ожидания при отправке данных")
                    except socket.error as e:
                        self.logger.error(f"Ошибка сокета при отправке: {e}")
                        raise
        else:
            while total_sent < data_len:
                try:
                    end = min(total_sent + chunk_size, data_len)
                    chunk = data[total_sent:end]
                    self.client_socket.sendall(chunk)
                    total_sent += len(chunk)
                except socket.timeout:
                    raise ConnectionError("Превышено время ожидания при отправке данных")
                except socket.error as e:
                    raise ConnectionError(f"Ошибка сокета при отправке: {e}")
                
        return total_sent
    
    def connect(self):
        """
        Подключается к серверу
        
        Returns:
            bool: True если подключение успешно, False в противном случае
        """
        try:
            self.logger.info(f"Подключение к серверу {self.server_ip}:{self.server_port}...")
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Устанавливаем таймауты для предотвращения зависания
            self.client_socket.settimeout(30.0)  # 30 секунд на операции
            # Отключаем алгоритм Нейгла для немедленной отправки данных
            self.client_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # Увеличиваем размер приемного буфера для повышения производительности
            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)  # 4 MB
            # Увеличиваем размер отправного буфера
            self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)  # 4 MB
            self.client_socket.connect((self.server_ip, self.server_port))
            self.logger.info("Подключение установлено")
            return True
        except socket.timeout:
            self.logger.error(f"Таймаут при подключении к серверу {self.server_ip}:{self.server_port}")
            return False
        except ConnectionRefusedError:
            self.logger.error(f"Не удалось подключиться к серверу {self.server_ip}:{self.server_port}")
            self.logger.warning("Убедитесь, что сервер запущен")
            return False
        except Exception as e:
            self.logger.error(f"Ошибка при подключении: {e}")
            return False
    
    def send_image(self, image_path):
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
        
        if not self.client_socket:
            if not self.connect():
                return False
        
        try:
            filename = os.path.basename(image_path)
            image_size = os.path.getsize(image_path)
            
            self.logger.debug(f"Имя клиента: {self.client_name}")
            self.logger.info(f"Отправка изображения: {filename} ({image_size} байт)")

            # Заголовок одним sendall
            self._send_header(filename=filename, image_size=image_size)

            # Отправляем файл потоково (без загрузки целиком в RAM)
            with open(image_path, 'rb', buffering=4 * 1024 * 1024) as f:
                total_sent = self._send_file_stream(f, image_size, show_progress=True, desc=f"Отправка {filename}")
            
            self.logger.info(f"Изображение отправлено ({total_sent} байт)")
            
            # Проверяем, что все данные были отправлены
            if total_sent != image_size:
                self.logger.error(f"Несоответствие размера: отправлено {total_sent} байт, ожидалось {image_size} байт")
                return False
            
            # Ждем подтверждение от сервера с таймаутом
            original_timeout = self.client_socket.gettimeout()
            try:
                self.client_socket.settimeout(10.0)  # 10 секунд на получение ответа
                response = self.client_socket.recv(2)
                if response == b'OK':
                    self.logger.info("Сервер подтвердил получение изображения")
                    result = True
                elif response == b'ER':
                    self.logger.error("Сервер вернул ошибку (возможно, размер изображения слишком большой)")
                    result = False
                else:
                    self.logger.warning(f"Получен неожиданный ответ от сервера: {response}")
                    result = False
            except socket.timeout:
                self.logger.warning("Таймаут при ожидании ответа от сервера")
                result = False
            except socket.error as e:
                self.logger.error(f"Ошибка при получении ответа от сервера: {e}")
                result = False
            finally:
                # Восстанавливаем исходный таймаут
                self.client_socket.settimeout(original_timeout)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Ошибка при отправке изображения: {e}")
            return False
    
    def disconnect(self):
        """Закрывает соединение с сервером"""
        if self.client_socket:
            self.client_socket.close()
            self.client_socket = None
            self.logger.debug("Соединение закрыто")
    
    def __enter__(self):
        """Контекстный менеджер: вход"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Контекстный менеджер: выход"""
        self.disconnect()


if __name__ == "__main__":
    """Основная функция"""
    if len(sys.argv) < 2:
        print("Использование: python MainClient.py <путь_к_изображению>")
        print("Пример: python MainClient.py C:\\path\\to\\image.jpg")
        sys.exit(1)
    
    image_path = sys.argv[1]
    
    client = ImageClient(client_name=CLIENT_NAME)
    
    try:
        success = client.send_image(image_path)
        sys.exit(0 if success else 1)
    finally:
        client.disconnect()
