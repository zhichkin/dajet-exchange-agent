using System;
using RabbitMQ.Client;

namespace DaJet.RabbitMQ.Producer
{
    public interface IMessageProducer : IDisposable
    {
        MessageProducerSettings Settings { get; }
        void Configure(MessageProducerSettings settings);
        bool QueueExists();
        void CreateQueue();
        void SendMessage(string messageBody);
    }
    public sealed class MessageProducer: IMessageProducer
    {
        private IModel Channel { get; set; }
        private IConnection Connection { get; set; }
        public MessageProducerSettings Settings { get; private set; }
        public void Configure(MessageProducerSettings settings)
        {
            Dispose();

            Settings = settings;

            IConnectionFactory factory = new ConnectionFactory()
            {
                HostName = Settings.HostName,
                UserName = Settings.UserName,
                Password = Settings.Password,
                Port = Settings.PortNumber
            };
            Connection = factory.CreateConnection();
            Channel = Connection.CreateModel();
        }
        public bool QueueExists()
        {
            bool exists = true;
            try
            {
                QueueDeclareOk queue = Channel.QueueDeclarePassive(Settings.QueueName);
            }
            catch
            {
                exists = false;
            }
            return exists;
        }
        public void CreateQueue()
        {
            if (Channel.IsClosed)
            {
                Channel.Dispose();
                Channel = Connection.CreateModel();
            }
            Channel.ExchangeDeclare(Settings.ExchangeName, ExchangeType.Direct, true, false, null);
            QueueDeclareOk queue = Channel.QueueDeclare(Settings.QueueName, true, false, false, null);
            if (queue == null)
            {
                throw new InvalidOperationException($"Creating \"{Settings.QueueName}\" queue failed.");
            }
            Channel.QueueBind(Settings.QueueName, Settings.ExchangeName, Settings.RoutingKey, null);
        }
        public void SendMessage(string messageBody)
        {
            byte[] message = System.Text.Encoding.UTF8.GetBytes(messageBody);

            IBasicProperties properties = Channel.CreateBasicProperties();
            properties.ContentType = "application/json";
            properties.DeliveryMode = 2; // persistent

            Channel.BasicPublish(
                Settings.ExchangeName,
                Settings.RoutingKey,
                basicProperties: properties,
                body: message);
        }
        public void Dispose()
        {
            if (Channel != null)
            {
                Channel.Close();
                Channel.Dispose();
                Channel = null;
            }

            if (Connection != null)
            {
                Connection.Close();
                Connection.Dispose();
                Connection = null;
            }
        }
    }
}