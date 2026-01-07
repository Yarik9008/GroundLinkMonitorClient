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

# имя клиента по умолчанию
CLIENT_NAME = "default_client"


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
    
    def _send_string(self, string):
        """
        Отправляет строку в сокет (сначала длина, затем данные)
        
        Args:
            string: Строка для отправки
        """
        string_bytes = string.encode('utf-8')
        self.client_socket.send(struct.pack('!I', len(string_bytes)))
        self.client_socket.send(string_bytes)
    
    def _send_data(self, data):
        """
        Отправляет данные в сокет
        
        Args:
            data: Данные для отправки (bytes)
        """
        total_sent = 0
        while total_sent < len(data):
            sent = self.client_socket.send(data[total_sent:])
            if sent == 0:
                raise ConnectionError("Соединение разорвано")
            total_sent += sent
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
            self.client_socket.connect((self.server_ip, self.server_port))
            self.logger.info("Подключение установлено")
            return True
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
            # Читаем изображение
            with open(image_path, 'rb') as f:
                image_data = f.read()
            
            image_size = len(image_data)
            filename = os.path.basename(image_path)
            
            self.logger.debug(f"Имя клиента: {self.client_name}")
            self.logger.info(f"Отправка изображения: {filename} ({image_size} байт)")
            
            # Отправляем имя клиента
            self._send_string(self.client_name)
            
            # Отправляем размер изображения
            self.client_socket.send(struct.pack('!I', image_size))
            
            # Отправляем имя файла
            self._send_string(filename)
            
            # Отправляем само изображение
            total_sent = self._send_data(image_data)
            
            self.logger.info(f"Изображение отправлено ({total_sent} байт)")
            
            # Ждем подтверждение от сервера
            response = self.client_socket.recv(2)
            if response == b'OK':
                self.logger.info("Сервер подтвердил получение изображения")
                return True
            else:
                self.logger.warning("Получен неожиданный ответ от сервера")
                return False
            
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
