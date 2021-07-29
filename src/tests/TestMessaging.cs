using DaJet.RabbitMQ.Producer;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;

namespace tests
{
    [TestClass]
    public sealed class TestMessaging
    {
        private IOptions<MessageProducerSettings> Settings { get; set; }

        public TestMessaging()
        {
            InitializeTestSettings();
        }
        private void InitializeTestSettings()
        {
            MessageProducerSettings settings = new MessageProducerSettings()
            {
                HostName = "localhost",
                VirtualHost = "",
                UserName = "guest",
                Password = "guest",
                PortNumber = 5672,
                RoutingKey = "",
                QueueName = "dajet-queue",
                ExchangeName = "dajet-exchange"
            };
            Settings = Options.Create(settings);
        }

        [TestMethod] public void TestCreateQueue()
        {
            List<string> queues = new List<string>()
            {
                "accord.dajet.goods",
                "accord.dajet.prices",
                "accord.dajet.regions",
                "accord.dajet.counterparties"
            };

            using (IMessageProducer producer = new MessageProducer(Settings))
            {
                foreach (string queueName in queues)
                {
                    Settings.Value.QueueName = queueName;
                    Settings.Value.ExchangeName = queueName;

                    if (producer.QueueExists())
                    {
                        Console.WriteLine("Queue " + queueName + " exists.");
                    }
                    else
                    {
                        Console.WriteLine("Queue " + queueName + " is not found.");
                        producer.CreateQueue();
                        Console.WriteLine("Queue " + queueName + " created successfully.");

                        if (producer.QueueExists())
                        {
                            Console.WriteLine("Queue " + queueName + " exists.");
                        }
                        else
                        {
                            Console.WriteLine("Queue " + queueName + " does not exist.");
                        }
                    }
                }
                
            }
        }

        [TestMethod] public void ConfigureTopicExchange()
        {
            MessageProducerSettings settings = new MessageProducerSettings()
            {
                HostName = "",
                VirtualHost = "",
                UserName = "",
                Password = "",
                PortNumber = 5672,
                RoutingKey = "",
                QueueName = "",
                ExchangeName = ""
            };
            //Settings = Options.Create(settings);

            IConnectionFactory factory = new ConnectionFactory()
            {
                HostName = settings.HostName,
                VirtualHost = settings.VirtualHost,
                UserName = settings.UserName,
                Password = settings.Password,
                Port = settings.PortNumber
            };
            IConnection connection = factory.CreateConnection();

            IModel channel = connection.CreateModel();

            if (TryCreateExchange(channel, settings))
            {
                Console.WriteLine($"Exchange [{settings.ExchangeName}] declared successfully.");
            }
            else { DisposeResources(connection, channel); return; }

            //try
            //{
            //    QueueDeclareOk result = channel.QueueDeclarePassive(settings.QueueName);
            //    Console.WriteLine("Queue name = " + result.QueueName);
            //    Console.WriteLine("Message count = " + result.MessageCount.ToString());
            //    Console.WriteLine("Consumer count = " + result.ConsumerCount.ToString());
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine($"Queue [{settings.QueueName}] not found.");
            //    Console.WriteLine(ex.Message);
            //}

            //Console.WriteLine();

            //try
            //{
            //    channel.ExchangeDeclarePassive(settings.ExchangeName);
            //    Console.WriteLine("Exchange name = " + settings.ExchangeName);
            //}
            //catch (Exception ex)
            //{
            //    Console.WriteLine($"Exchange [{settings.ExchangeName}] not found.");
            //    Console.WriteLine(ex.Message);
            //}

            //Console.WriteLine();

            //UnbindQueue(channel, settings);

            //Console.WriteLine();

            //DeleteQueue(channel, settings);

            //Console.WriteLine();

            //DeleteExchange(channel, settings);

            DisposeResources(connection, channel);
        }
        private void DisposeResources(IConnection connection, IModel channel)
        {
            if (channel != null)
            {
                channel.Close();
                channel.Dispose();
            }
            if (connection != null)
            {
                connection.Close();
                connection.Dispose();
            }
        }
        private bool TryCreateExchange(IModel channel, MessageProducerSettings settings)
        {
            bool result = true;
            try
            {
                channel.ExchangeDeclare(settings.ExchangeName, ExchangeType.Topic, true, false, null);
            }
            catch (Exception ex)
            {
                result = false;
                Console.WriteLine("Exchange declare error:");
                Console.WriteLine(ex.Message);
            }
            return result;
        }
        private void UnbindQueue(IModel channel, MessageProducerSettings settings)
        {
            try
            {
                channel.QueueUnbind(settings.QueueName, settings.ExchangeName, string.Empty);
                Console.WriteLine($"Queue [{settings.QueueName}] unbined from exchange [{settings.ExchangeName}] successfeully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Queue unbind error:");
                Console.WriteLine(ex.Message);
            }
        }
        private void DeleteQueue(IModel channel, MessageProducerSettings settings)
        {
            try
            {
                uint purged = channel.QueueDelete(settings.QueueName);
                Console.WriteLine($"Queue [{settings.QueueName}] deleted. {purged} messages purged.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Queue delete error:");
                Console.WriteLine(ex.Message);
            }
        }
        private void DeleteExchange(IModel channel, MessageProducerSettings settings)
        {
            try
            {
                channel.ExchangeDelete(settings.ExchangeName);
                Console.WriteLine($"Exchange [{settings.ExchangeName}] deleted.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Exchange delete error:");
                Console.WriteLine(ex.Message);
            }
        }

        [TestMethod] public void GetHostInfo()
        {
            HttpClient http = new HttpClient()
            {
                BaseAddress = new Uri("")
            };
            string userName = "";
            string password = "";
            byte[] authToken = Encoding.ASCII.GetBytes($"{userName}:{password}");
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(authToken));

            HttpResponseMessage response = http.GetAsync("/api/exchanges/accord").Result;
            Console.WriteLine(response.Content.ReadAsStringAsync().Result);
        }
    }
}