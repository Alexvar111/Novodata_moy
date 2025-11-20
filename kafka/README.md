Описание пайплайна
Поток событий пользователей проходит 4 шага:
-Генерация событий и отправка в Kafka (топик user_events).
-Чтение из Kafka и запись в PostgreSQL в таблицу user_logins с флагом sent_to_kafka BOOLEAN DEFAULT FALSE.
-Выгрузка из PostgreSQL только неотправленных записей (sent_to_kafka IS NOT TRUE) во второй Kafka-топик user_events_to_clickhouse с последующей пометкой этих строк sent_to_kafka = TRUE в PostgreSQL.
-Чтение подготовленных сообщений из user_events_to_clickhouse и запись в ClickHouse в таблицу user_logins.

Запуск(шаги):
1.Запустить генерацию событий в Kafka:
В терминале запустить: python producer_kafka_pg.py

2.Чтение в PostgreSQL:
В терминале запустить: python consumer_kafka_pg.py

3.Проверка в PostgreSQL:
-SELECT COUNT(*) FROM user_logins;
-SELECT sent_to_kafka, COUNT(*) FROM user_logins GROUP BY sent_to_kafka;
Все новые строки будут с sent_to_kafka = FALSE.

4.Миграция PostgreSQL → Kafka c флагом:
-В терминале запустить: python producer_pg_kafka.py

5.Проверка в PostgreSQL:
-SELECT sent_to_kafka, COUNT(*) FROM user_logins GROUP BY sent_to_kafka;
Должны появиться строки с TRUE.

6.Загрузка в ClickHouse:
-В терминале запустить: python consumer_kafka_clickhouse.py

7.Проверка в ClickHouse:
-SELECT sent_to_kafka, COUNT(*) FROM user_logins GROUP BY sent_to_kafka;
Количество TRUE должно соответствовать PostgreSQL, а при повторных запусках миграционного продюсера новые дубли не появляются.