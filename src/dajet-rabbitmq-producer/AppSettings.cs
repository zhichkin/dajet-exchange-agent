namespace DaJet.RabbitMQ.Producer
{
    public sealed class AppSettings
    {
        public int LogSize { get; set; } = 64 * 1024; // 64 Kb = 65536 bytes
        public int CriticalErrorDelay { get; set; } = 300000; // 5 minutes
        public int MessagesPerTransaction { get; set; } = 1000;
        public int ReceivingMessagesPeriodicity { get; set; } = 60000; // 1 minute
        public int WaitForNotificationTimeout { get; set; } = 180000; // 3 minutes
    }
}