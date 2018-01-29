## Проксирующий сервис гарантированной доставки сообщенийю

Для работы используется:
- RabbitMQ;
- PostgreSQL

## Общая идея:

Сервис состоит из 2 компонентов:
- consumer - подписывается на очередь в RabbitMQ и сохраняет полученные сообщения в таблицу "messages".
- starter - сервис, выбирающий из базы данных сообщения в статусе "NEW" и "FAILURE" и запускающий по ним цепочку задач.
Статус FAILURE проставляется при возникновении ожидаемой ошибки в ходе обработки сообщения (недоступен сервис аутентификации, 
не удалось отправить сообщение, etc).
В случае возникновения критической ошибки, сообщению проставляется статус FATAL, а ошибка пишется в таблицу "errors".

Ожидаемый формат входящего сообщения:
```
{
  "user_id": уникальный идентификатор пользователя,
  "auth_key": ключ авторизации в сервисе отправки сообщений,
  "message": сообщение,
  "attachments": [
    {
      "name": имя прикрепленного файла,
      "url": url для доступа к файлу на файловом сервере компании
    }
  ]
}
```