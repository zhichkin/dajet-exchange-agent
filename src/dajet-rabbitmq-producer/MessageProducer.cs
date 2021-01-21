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
        private IBasicProperties Properties { get; set; }
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


        private IConnection CreateConnection()
        {
            IConnectionFactory factory = new ConnectionFactory()
            {
                HostName = Settings.HostName,
                UserName = Settings.UserName,
                Password = Settings.Password,
                Port = Settings.PortNumber
            };
            return factory.CreateConnection();
        }
        private void InitializeConnection()
        {
            if (Connection == null)
            {
                Connection = CreateConnection();
            }
            else if (!Connection.IsOpen)
            {
                Connection.Dispose();
                Connection = CreateConnection();
            }
        }
        private void InitializeBasicProperties()
        {
            if (Channel == null) return;
            if (Channel.IsClosed) return;

            Properties = Channel.CreateBasicProperties();
            Properties.ContentType = "application/json";
            Properties.DeliveryMode = 2; // persistent
        }
        private void InitializeChannel()
        {
            InitializeConnection();

            if (Channel == null)
            {
                Channel = Connection.CreateModel();
                InitializeBasicProperties();
            }
            else if (Channel.IsClosed)
            {
                Channel.Dispose();
                Channel = Connection.CreateModel();
                InitializeBasicProperties();
            }
        }
        

        public void SendMessage(string messageBody)
        {
            InitializeChannel();

            byte[] message = Encoding.UTF8.GetBytes(messageBody);

            Channel.BasicPublish(Settings.ExchangeName, Settings.RoutingKey, Properties, message);
        }
        public void Dispose()
        {
            if (Channel != null)
            {
                if (!Channel.IsClosed)
                {
                    Channel.Close();
                }
                Channel.Dispose();
                Channel = null;
            }

            if (Connection != null)
            {
                if (Connection.IsOpen)
                {
                    Connection.Close();
                }
                Connection.Dispose();
                Connection = null;
            }
        }
    }
}