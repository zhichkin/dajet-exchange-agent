namespace DaJet.RabbitMQ.Producer
{
    public sealed class MessageProducerSettings
    {
        public string HostName { get; set; } = "localhost";
        public string UserName { get; set; } = "guest";
        public string Password { get; set; } = "guest";
        public int PortNumber { get; set; } = 5672;
        public string QueueName { get; set; } = "dajet-queue";
        public string RoutingKey { get; set; } = string.Empty;
        public string ExchangeName { get; set; } = "dajet-exchange";
    }
}