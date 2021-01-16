namespace DaJet.RabbitMQ.Producer
{
    public sealed class MessageConsumerSettings
    {
        public string ServerName { get; set; }
        public string DatabaseName { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
    }
}