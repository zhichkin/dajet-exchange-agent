using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace DaJet.RabbitMQ.Producer
{
    public interface IMessageProducer : IDisposable
    {
        bool QueueExists();
        void CreateQueue();
        void Publish(List<DaJetMessage> batch);
    }
    public sealed class MessageProducer: IMessageProducer
    {
        private const string DEFAULT_EXCHANGE_NAME = "exchange";
        private const string PUBLISHER_CONFIRMATION_ERROR_MESSAGE = "The sending of the message has not been confirmed. Check the availability of the message broker.";
        private const string PUBLISHER_CONFIRMATION_TIMEOUT_MESSAGE = "The sending of the messages has been timed out. Check the availability of the message broker.";

        private IModel Channel { get; set; }
        private IConnection Connection { get; set; }
        private MessageProducerSettings Settings { get; set; }
        public MessageProducer(IOptions<MessageProducerSettings> options)
        {
            Settings = options.Value;
        }

        #region "Service methods"

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
        private void InitializeChannel()
        {
            InitializeConnection();

            if (Channel == null)
            {
                Channel = Connection.CreateModel();
            }
            else if (Channel.IsClosed)
            {
                Channel.Dispose();
                Channel = Connection.CreateModel();
            }
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
        public bool QueueExists()
        {
            InitializeChannel();

            bool exists = true;
            try
            {
                QueueDeclareOk queue = Channel.QueueDeclarePassive(Settings.QueueName);
            }
            catch (Exception error)
            {
                if (!error.Message.Contains("NOT_FOUND"))
                {
                    throw error;
                }
                exists = false;
            }
            return exists;
        }
        public void CreateQueue()
        {
            InitializeChannel();

            Channel.ExchangeDeclare(Settings.ExchangeName, ExchangeType.Direct, true, false, null);
            QueueDeclareOk queue = Channel.QueueDeclare(Settings.QueueName, true, false, false, null);
            if (queue == null)
            {
                throw new InvalidOperationException($"Creating \"{Settings.QueueName}\" queue failed.");
            }
            Channel.QueueBind(Settings.QueueName, Settings.ExchangeName, Settings.RoutingKey, null);
        }
        
        #endregion

        #region "Multi-threaded message sending"

        private bool ConnectionIsBlocked = false;

        private IConnection CreateConnection()
        {
            IConnectionFactory factory = new ConnectionFactory()
            {
                HostName = Settings.HostName,
                VirtualHost = Settings.VirtualHost,
                UserName = Settings.UserName,
                Password = Settings.Password,
                Port = Settings.PortNumber
            };
            return factory.CreateConnection();
        }

        private void ConfigureConnection()
        {
            if (Connection != null && Connection.IsOpen) return;
            if (Connection != null) Connection.Dispose();

            Connection = CreateConnection();
            Connection.ConnectionBlocked += HandleConnectionBlocked;
            Connection.ConnectionUnblocked += HandleConnectionUnblocked;
        }
        private void HandleConnectionBlocked(object sender, ConnectionBlockedEventArgs args)
        {
            ConnectionIsBlocked = true;
            FileLogger.Log("Connection blocked: " + args.Reason);

            if (SendingCancellation != null)
            {
                try
                {
                    SendingCancellation.Cancel();
                }
                catch
                {
                    // SendingCancellation can be already disposed ...
                }
            }
        }
        private void HandleConnectionUnblocked(object sender, EventArgs args)
        {
            ConnectionIsBlocked = false;
            FileLogger.Log("Connection unblocked.");
        }

        private CancellationTokenSource SendingCancellation;
        private ConcurrentQueue<Exception> SendingExceptions;
        
        private List<ProducerChannel> ProducerChannels;
        private Dictionary<string, Queue<DaJetMessage>> ProducerQueues;

        private string CreateExchangeName(string routingKey)
        {
            if (string.IsNullOrWhiteSpace(routingKey))
            {
                return Settings.ExchangeName;
            }
            return Settings.ExchangeName.Replace(DEFAULT_EXCHANGE_NAME, routingKey);
        }
        private ProducerChannel CreateProducerChannel()
        {
            IModel channel = Connection.CreateModel();
            channel.ConfirmSelect();
            return new ProducerChannel()
            {
                Channel = channel,
                Properties = CreateMessageProperties(channel)
            };
        }
        private IBasicProperties CreateMessageProperties(IModel channel)
        {
            IBasicProperties properties = channel.CreateBasicProperties();
            properties.ContentType = "application/json";
            properties.DeliveryMode = 2; // persistent
            properties.ContentEncoding = "UTF-8";
            return properties;
        }
        private void SetOperationTypeHeader(DaJetMessage message, IBasicProperties properties)
        {
            if (string.IsNullOrWhiteSpace(message.OperationType)) return;

            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }

            if (!properties.Headers.TryAdd("OperationType", message.OperationType))
            {
                properties.Headers["OperationType"] = message.OperationType;
            }
        }

        private void ConfigureProducerChannels()
        {
            int channelMax = Environment.ProcessorCount;
            if (Connection.ChannelMax > 0 && Connection.ChannelMax < channelMax)
            {
                channelMax = Connection.ChannelMax;
            }

            if (ProducerChannels == null)
            {
                ProducerChannels = new List<ProducerChannel>(channelMax);
            }

            if (ProducerChannels.Count == 0)
            {
                for (int i = 0; i < channelMax; i++)
                {
                    ProducerChannels.Add(CreateProducerChannel());
                }
            }
            else
            {
                for (int i = 0; i < ProducerChannels.Count; i++)
                {
                    ProducerChannels[i].Queues.Clear();
                    if (!ProducerChannels[i].IsHealthy)
                    {
                        ProducerChannels[i].Channel.Dispose();
                        ProducerChannels[i] = CreateProducerChannel();
                    }
                }
            }
        }
        private void ConfigureProducerQueues(List<DaJetMessage> batch)
        {
            if (ProducerQueues != null)
            {
                ProducerQueues.Clear();
            }
            else
            {
                ProducerQueues = new Dictionary<string, Queue<DaJetMessage>>();
            }

            foreach (DaJetMessage message in batch)
            {
                Queue<DaJetMessage> queue;
                if (!ProducerQueues.TryGetValue(message.MessageType, out queue))
                {
                    queue = new Queue<DaJetMessage>();
                    ProducerQueues.Add(message.MessageType, queue);
                }
                queue.Enqueue(message);
            }
        }
        private void AssignProducerQueuesToChannels()
        {
            int nextChannel = 0;
            int maxChannels = ProducerChannels.Count;
            if (maxChannels == 0) return;

            foreach (Queue<DaJetMessage> queue in ProducerQueues.Values)
            {
                if (nextChannel == maxChannels)
                {
                    nextChannel = 0;
                }

                ProducerChannels[nextChannel].Queues.Add(queue);

                nextChannel++;
            }

            ProducerQueues.Clear();
        }

        public void Publish(List<DaJetMessage> batch)
        {
            if (batch == null || batch.Count == 0) return;

            if (ConnectionIsBlocked)
            {
                throw new OperationCanceledException("Connection was blocked: sending messages operation is canceled.");
            }

            bool throwException;
            SendingExceptions = new ConcurrentQueue<Exception>();

            ConfigureConnection();
            ConfigureProducerChannels();
            ConfigureProducerQueues(batch);
            AssignProducerQueuesToChannels();

            int messagesSent = 0;
            int messagesToBeSent = batch.Count;
            try
            {
                messagesSent = PublishMessagesInParallel();

                throwException = (messagesSent != messagesToBeSent);
            }
            catch (Exception error)
            {
                throwException = true;
                SendingExceptions.Enqueue(error);
            }

            LogExceptions();

            if (throwException)
            {
                throw new OperationCanceledException("Sending messages operation was canceled.");
            }
            else
            {
                FileLogger.Log(string.Format("{0} messages have been published successfully.", messagesSent));
            }
        }
        private int PublishMessagesInParallel()
        {
            int messagesSent = 0;

            using (SendingCancellation = new CancellationTokenSource())
            {
                Task<int>[] tasks = new Task<int>[ProducerChannels.Count];

                for (int channelId = 0; channelId < ProducerChannels.Count; channelId++)
                {
                    tasks[channelId] = Task.Factory.StartNew(
                        PublishMessagesInBackground,
                        channelId,
                        SendingCancellation.Token,
                        TaskCreationOptions.LongRunning,
                        TaskScheduler.Default);
                }

                Task.WaitAll(tasks, SendingCancellation.Token);

                foreach (Task<int> task in tasks)
                {
                    messagesSent += task.Result;
                }
            }

            return messagesSent;
        }
        private int PublishMessagesInBackground(object id)
        {
            int channelId = (int)id;

            ProducerChannel producerChannel = ProducerChannels[channelId];

            int messagesSent = 0;

            foreach (Queue<DaJetMessage> queue in producerChannel.Queues)
            {
                while (queue.TryDequeue(out DaJetMessage message))
                {
                    if (SendingCancellation.IsCancellationRequested) return 0;

                    try
                    {
                        PublishMessage(producerChannel, message);
                        messagesSent++;
                    }
                    catch (Exception error)
                    {
                        SendingExceptions.Enqueue(error);
                        SendingCancellation.Cancel();
                    }
                }
            }

            if (messagesSent > 0)
            {
                WaitForConfirms(producerChannel);
            }

            return messagesSent;
        }

        private void PublishMessage(ProducerChannel channel, DaJetMessage message)
        {
            string mapping = null;
            if (Settings.MessageTypeRouting != null)
            {
                _ = Settings.MessageTypeRouting.TryGetValue(message.MessageType, out mapping);
            }

            string routingKey = string.Empty;
            if (mapping == null)
            {
                routingKey = message.MessageType;
            }

            string exchangeName = CreateExchangeName(mapping);

            byte[] messageBytes = Encoding.UTF8.GetBytes(message.MessageBody);

            channel.Properties.Type = message.MessageType;
            channel.Properties.MessageId = message.Uuid.ToString();
            SetOperationTypeHeader(message, channel.Properties);
            
            channel.Channel.BasicPublish(exchangeName, routingKey, channel.Properties, messageBytes);
        }
        private void WaitForConfirms(ProducerChannel channel)
        {
            try
            {
                bool confirmed = channel.Channel.WaitForConfirms(TimeSpan.FromSeconds(10), out bool timedout);
                if (!confirmed)
                {
                    if (timedout)
                    {
                        SendingExceptions.Enqueue(new OperationCanceledException(PUBLISHER_CONFIRMATION_TIMEOUT_MESSAGE));
                    }
                    else
                    {
                        SendingExceptions.Enqueue(new OperationCanceledException(PUBLISHER_CONFIRMATION_ERROR_MESSAGE));
                    }
                    SendingCancellation.Cancel();
                }
            }
            catch (OperationInterruptedException rabbitError)
            {
                SendingExceptions.Enqueue(rabbitError);
                if (string.IsNullOrWhiteSpace(rabbitError.Message) || !rabbitError.Message.Contains("NOT_FOUND"))
                {
                    SendingCancellation.Cancel();
                }
            }
            catch (Exception error)
            {
                SendingExceptions.Enqueue(error);
                SendingCancellation.Cancel();
            }
        }
        
        private void LogExceptions()
        {
            if (SendingExceptions.Count > 0)
            {
                while (SendingExceptions.TryDequeue(out Exception error))
                {
                    FileLogger.Log(ExceptionHelper.GetErrorText(error));
                }
            }
            SendingExceptions.Clear();
        }

        #endregion
    }
}