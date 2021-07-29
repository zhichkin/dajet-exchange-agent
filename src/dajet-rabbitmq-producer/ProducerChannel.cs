using RabbitMQ.Client;
using System.Collections.Generic;

namespace DaJet.RabbitMQ.Producer
{
    public sealed class ProducerChannel
    {
        public IModel Channel { get; set; }
        public IBasicProperties Properties { get; set; }
        public List<Queue<DaJetMessage>> Queues { get; } = new List<Queue<DaJetMessage>>();
        public bool IsHealthy
        {
            get
            {
                return (Channel != null && Channel.IsOpen);
            }
        }
    }
}