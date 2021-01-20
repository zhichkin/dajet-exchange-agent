using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DaJet.RabbitMQ.Producer
{
    public sealed class Worker : BackgroundService
    {
        private AppSettings Settings { get; set; }
        private ILogger<Worker> Logger { get; set; }
        private IServiceProvider Services { get; set; }

        public Worker(IServiceProvider serviceProvider, IOptions<AppSettings> options, ILogger<Worker> logger)
        {
            Logger = logger;
            Settings = options.Value;
            Services = serviceProvider;
        }
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            Logger.LogInformation("{time} Worker is started.", DateTime.Now);
            return base.StartAsync(cancellationToken);
        }
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            Logger.LogInformation("{time} Worker is stoped.", DateTime.Now);
            return base.StopAsync(cancellationToken);
        }
        
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ReceiveMessages(out string errorMessage);
                if (!string.IsNullOrEmpty(errorMessage))
                {
                    Logger.LogInformation("{time} {error}", FormatDateTime(DateTime.Now), errorMessage);
                    Logger.LogInformation("{time} Critical error delay of {delay} seconds started.",
                        FormatDateTime(DateTime.Now),
                        Settings.CriticalErrorDelay / 1000);
                    await Task.Delay(Settings.CriticalErrorDelay, stoppingToken);
                }

                int resultCode = AwaitNotification(Settings.WaitForNotificationTimeout);
                if (resultCode == 1) // notifications are not supported by database
                {
                    await Task.Delay(Settings.ReceivingMessagesPeriodicity, stoppingToken);
                }
            }
        }
        private string FormatDateTime(DateTime input) { return input.ToString("yyyy-MM-dd HH:mm:ss"); }
        private void ReceiveMessages(out string errorMessage)
        {
            int messagesReceived = 0;
            errorMessage = string.Empty;

            Logger.LogInformation("{time} Start receiving messages.", FormatDateTime(DateTime.Now));

            try
            {
                IMessageConsumer consumer = Services.GetService<IMessageConsumer>();
                messagesReceived = consumer.ReceiveMessages(Settings.MessagesPerTransaction, out errorMessage);
                while (messagesReceived > 0)
                {
                    messagesReceived += consumer.ReceiveMessages(Settings.MessagesPerTransaction, out errorMessage);
                }
            }
            catch (Exception error)
            {
                errorMessage += (string.IsNullOrEmpty(errorMessage) ? string.Empty : Environment.NewLine)
                    + ExceptionHelper.GetErrorText(error);
            }

            Logger.LogInformation("{time} {count} messages received.", FormatDateTime(DateTime.Now), messagesReceived);
        }
        private int AwaitNotification(int timeout)
        {
            int resultCode = 0;
            string errorMessage = string.Empty;

            Logger.LogInformation("{time} Start awaiting notification ...", FormatDateTime(DateTime.Now));

            try
            {
                IMessageConsumer consumer = Services.GetService<IMessageConsumer>();
                resultCode = consumer.AwaitNotification(timeout, out errorMessage);
            }
            catch (Exception error)
            {
                errorMessage += (string.IsNullOrEmpty(errorMessage) ? string.Empty : Environment.NewLine)
                    + ExceptionHelper.GetErrorText(error);
            }

            if (!string.IsNullOrEmpty(errorMessage))
            {
                Logger.LogInformation("{time} {error}", FormatDateTime(DateTime.Now), errorMessage);
            }

            if (resultCode == 0)
            {
                Logger.LogInformation("{time} Notification received successfully.", FormatDateTime(DateTime.Now));
            }
            else if (resultCode == 1)
            {
                Logger.LogInformation("{time} Notifications are not supported.", FormatDateTime(DateTime.Now));
            }
            else if (resultCode == 2)
            {
                Logger.LogInformation("{time} No notification received.", FormatDateTime(DateTime.Now));
            }

            return resultCode;
        }
    }
}