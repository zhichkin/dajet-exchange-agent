# Подписка на сообщения внешними системами

Точка доступа (exchange) типа "topic" создаётся на сервере RabbitMQ заранее.
Имя точки доступа указывается в настройке **ExchangeName** файла **appsettings.json**,
секция **ProducerSettings**.

Пример создания точки доступа на языке C#:
```C#
string exchangeName = "dajet.exchange";
string exchangeType = "topic";
bool durable = true;
bool autoDelete = false;
IDictionary<string, object> arguments = null;

channel.ExchangeDeclare(exchangeName, exchangeType, durable, autoDelete, arguments);
```

Очередь сообщений подписчик создаёт сам. Шаблон привязки (binding key) очереди к точке обмена
подписчик указывает на основании ключа маршрутизации (routing key), который указывается издателем.

Издатель (служба обмена DaJet Exchange) для каждого публикуемого сообщения указывает ключ маршрутизации вида:

**[Вид объекта 1С].[Имя объекта 1С]**

По сути своей это наименование типа сообщения. Например:
- Справочник.Номенклатура
- Документ.ЗаказКлиента
- РегистрСведений.ЦеныНоменклатуры
- РегистрНакопления.ОстаткиТоваровНаСкладах

Таким образом, используя шаблон привязки очередей, подписчик может подписаться на получение сообщений
в разные очереди. Для этого он самостоятельно привязывает свою очередь к точке обмена. Например:
- Подписка на все сообщения: **#**
- Подписка только на справочники: **Справочник.*** или **Справочник.#**
- Подписка на конкретный тип сообщения: **Справочник.Номенклатура**

Пример создания очереди и её подписки на события типа "Справочник.Номенклатура" на языке C#:
```C#
string exchangeName = "dajet.exchange";
string queueName = "dajet.goods";
string routingKey = "Справочник.Номенклатура";
bool durable = true;
bool exclusive = false;
bool autoDelete = false;
IDictionary<string, object> arguments = null;

QueueDeclareOk queue = channel.QueueDeclare(queueName, durable, exclusive, autoDelete, arguments);

channel.QueueBind(queueName, exchangeName, routingKey, arguments);
```

Кроме этого издатель заполняет следующие атрибуты сообщений и заголовки:
- **Type** - тип сообщения, например, "Справочник.Номенклатура".
- **MessageId** - идентификатор (uuid) сообщения.
- **ContentType** - всегда "application/json".
- **ContentEncoding** - всегда "UTF-8".
- **DeliveryMode** - всегда 2 (persistent).
- **OperationType** - заголовок сообщения. Перечисление: "INSERT", "UPDATE", "DELETE".

![Пример заполнения атрибутов и заголовков сообщений](https://github.com/zhichkin/dajet-exchange-agent/blob/main/doc/message-headers-example.png)