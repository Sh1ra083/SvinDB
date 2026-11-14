# SvinDB для Linux x86_64

Готовая сборка для 64-разрядных Linux-систем с glibc 2.35 или новее.
Релизный бинарник собирается на Ubuntu 22.04 для x86_64.

## Запуск

Распакуйте архив и выполните:

```bash
chmod +x svindb
./svindb serve ./data 19300
```

Узел слушает `127.0.0.1:19300`. Каталог `data` в текущем рабочем
каталоге используется для документов, WAL и сегментов и создаётся при первом обычном запуске.

Ограниченные режимы:

```bash
./svindb serve ./data 19300 --read-only
./svindb serve ./data 19300 --no-maintain
```

Явный каталог данных:

```bash
./svindb serve /var/lib/svindb 19300
```

## Проверка файла

```bash
sha256sum -c SHA256SUMS
```

Исходный код и полная документация:
<https://github.com/Sh1ra083/SvinDB>

Лицензия: Apache License 2.0.
