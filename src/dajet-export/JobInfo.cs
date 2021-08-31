using RabbitMQ.Client;
using System.Collections.Generic;

namespace DaJet.Export
{
    public sealed class JobInfo
    {
        public IModel Channel { get; set; }
        public IBasicProperties Properties { get; set; }
        public Queue<BatchInfo> Batches { get; set; } = new Queue<BatchInfo>();
    }
}