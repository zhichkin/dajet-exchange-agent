using System;

namespace DaJet.RabbitMQ.Producer
{
    public sealed class DaJetMessage
    {
        public long UTC { get; set; }
        public Guid Uuid { get; set; }
        public string MessageType { get; set; }
        public string MessageBody { get; set; }
        public string OperationType { get; set; }
    }
}