using RabbitMQ.Client;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;

namespace DaJet.Export
{
    public sealed class JobInfo
    {
        public List<BatchInfo> Batches { get; set; } = new List<BatchInfo>();
        public IModel Channel { get; set; }
        public IBasicProperties Properties { get; set; }
        public MemoryStream Stream { get; set; }
        public Utf8JsonWriter Writer { get; set; }
        public AutoResetEvent ConfirmEvent { get; set; } = new AutoResetEvent(false);
    }
}