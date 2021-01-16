using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;

namespace DaJet.RabbitMQ.Producer
{
    public static class Program
    {
        private static int _counter = 0;
        private static MessageProducerSettings Settings { get; set; }
        private static IMessageProducer MessageProducer { get; set; }
        static void Main(string[] args)
        {
            Console.WriteLine("Initializing settings ...");
            InitializeSettings();

            Console.WriteLine("Initializing message producer ...");
            InitializeMessageProducer();

            using (MessageProducer)
            {
                Console.WriteLine("Configuring message producer ...");
                MessageProducer.Configure(Settings);
                if (!MessageProducer.QueueExists())
                {
                    MessageProducer.CreateQueue();
                }

                Console.WriteLine("RabbitMQ producer is running ...");
                Console.WriteLine("Press any key to send message.");
                Console.WriteLine("Press ESCAPE key to exit program.");

                ConsoleKeyInfo key = Console.ReadKey(false);
                while (key.Key != ConsoleKey.Escape)
                {
                    DoSomethingUseful();

                    key = Console.ReadKey(false);
                }
            }
        }
        private static void InitializeSettings()
        {
            Settings = new MessageProducerSettings();
        }
        private static void InitializeMessageProducer()
        {
            MessageProducer = new MessageProducer();
        }
        private static string ProduceMessage()
        {
            string message;

            using (MemoryStream stream = new MemoryStream())
            using (Utf8JsonWriter writer = new Utf8JsonWriter(stream))
            {
                _counter++;

                message = JsonSerializer.Serialize(
                    new TestMessage()
                    {
                        Body = $"test message {_counter}"
                    });
            }

            return message;
        }
        private static void DoSomethingUseful()
        {
            //string message = ProduceMessage();
            //MessageProducer.SendMessage(message);
            //Console.WriteLine(message);

            Console.WriteLine("Receiving messages ...");
            int messagesCount = 1000;
            messagesCount = ReceiveMessages(messagesCount);
            Console.WriteLine($"{messagesCount} messages received.");
        }



        private static int ReceiveMessages(int count)
        {
            List<DaJetMessage> messages = new List<DaJetMessage>();

            MessageConsumer consumer = new MessageConsumer(MessageProducer);
            MessageConsumerSettings settings = new MessageConsumerSettings()
            {
                ServerName = "ZHICHKIN",
                DatabaseName = "my_exchange"
            };
            consumer.Configure(settings);

            messages = consumer.ReceiveMessages(1000, out string errorMessage);
            if (!string.IsNullOrEmpty(errorMessage))
            {
                Console.WriteLine(errorMessage);
            }

            return messages.Count;
        }
    }
}