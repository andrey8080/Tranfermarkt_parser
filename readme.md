# Парсер Trasfermarkt

Этот проект представляет собой парсер для сбора статистических данных всех игроков всех клубов из заданной по URL лиги.

## Структура проекта

- `league_data.json`: Пример данных лиги.
- `parser.py`: Основной скрипт парсера.
- `requirements.txt` Файл с зависимостями

## Установка

1. Клонируйте репозиторий:
    ```sh
    git clone https://github.com/andrey8080/Tranfermarkt_parser
    ```
2. Перейдите в директорию проекта:
    ```sh
    cd Tranfermarkt_parser
    ```
3. Установите необходимые зависимости:
    ```sh
    pip install -r requirements.txt
    ```

## Использование

Для запуска парсера выполните следующую команду:

```sh
python3 parser.py  <URL лиги>
```