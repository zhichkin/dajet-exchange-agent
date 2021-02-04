using DaJet.RabbitMQ.Producer;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
            IMessageProducer producer = new MessageProducer(Settings);
            producer.CreateQueue();
            producer.Dispose();
        }
    }
}