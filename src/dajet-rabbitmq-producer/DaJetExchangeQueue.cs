using System.Collections.Generic;

namespace DaJet.RabbitMQ.Producer
{
    public sealed class DaJetExchangeQueue
    {
        public string ObjectName { get; set; }
        public string TableName { get; set; }
        public List<PropertyToFieldMap> Properties { get; } = new List<PropertyToFieldMap>();
    }
    public sealed class PropertyToFieldMap
    {
        public string Name { get; set; }
        public string Field { get; set; }
    }
}