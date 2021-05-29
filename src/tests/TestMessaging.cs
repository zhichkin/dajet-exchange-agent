using DaJet.RabbitMQ.Producer;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

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

        [TestMethod]
        public void TestCreateQueue()
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
    }
}