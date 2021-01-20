using System;
using System.Text;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace DaJet.RabbitMQ.Producer
{
    public interface IMessageProducer : IDisposable
    {
        void SendMessage(string messageBody);
    }
    public sealed class MessageProducer: IMessageProducer
    {
        private IModel Channel { get; set; }
        private IConnection Connection { get; set; }
        private MessageProducerSettings Settings { get; set; }
        public MessageProducer(IOptions<MessageProducerSettings> options)
        {
            Settings = options.Value;
        }
        private bool QueueExists()
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
        private void CreateQueue()
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
            if (Connection == null || Channel == null)
            {
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
            
            if (Channel != null && Channel.IsClosed)
            {
                Channel.Dispose();
                Channel = Connection.CreateModel();
            }

            IBasicProperties properties = Channel.CreateBasicProperties();
            properties.ContentType = "application/json";
            properties.DeliveryMode = 2; // persistent

            byte[] message = Encoding.UTF8.GetBytes(messageBody);

            Channel.BasicPublish(Settings.ExchangeName, Settings.RoutingKey, properties, message);
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