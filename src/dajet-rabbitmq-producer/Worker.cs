using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DaJet.RabbitMQ.Producer
{
    public sealed class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        private MessageProducerSettings ProducerSettings { get; set; }
        private IMessageProducer MessageProducer { get; set; }

        private MessageConsumerSettings ConsumerSettings { get; set; }
        private IMessageConsumer MessageConsumer { get; set; }

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("{time} Worker started.", DateTime.Now);

            _logger.LogInformation("Initializing producer settings ...");
            InitializeProducerSettings();

            _logger.LogInformation("Initializing message producer ...");
            InitializeMessageProducer();

            _logger.LogInformation("Initializing consumer settings ...");
            InitializeConsumerSettings();

            _logger.LogInformation("Initializing message consumer ...");
            InitializeMessageConsumer();

            return base.StartAsync(cancellationToken);
        }
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("{time} Worker is stoped.", DateTime.Now);
            return base.StopAsync(cancellationToken);
        }
        
        private void InitializeProducerSettings()
        {
            ProducerSettings = new MessageProducerSettings();
        }
        private void InitializeMessageProducer()
        {
            MessageProducer = new MessageProducer();
            MessageProducer.Configure(ProducerSettings);
        }
        private void InitializeConsumerSettings()
        {
            ConsumerSettings = new MessageConsumerSettings()
            {
                ServerName = "ZHICHKIN",
                DatabaseName = "my_exchange"
            };
        }
        private void InitializeMessageConsumer()
        {
            MessageConsumer = new MessageConsumer(MessageProducer);
            MessageConsumer.Configure(ConsumerSettings);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    _logger.LogInformation("{time} Start receiving messages.", DateTimeOffset.Now);
                    int messagesReceived = ReceiveMessages(10);
                    _logger.LogInformation("{time} {count} messages received.", DateTimeOffset.Now, messagesReceived);
                    
                    while (messagesReceived > 0)
                    {
                        _logger.LogInformation("{time} Start receiving messages.", DateTimeOffset.Now);
                        messagesReceived = ReceiveMessages(10);
                        _logger.LogInformation("{time} {count} messages received.", DateTimeOffset.Now, messagesReceived);
                    }
                }
                catch (Exception error)
                {
                    _logger.LogInformation("{time} {error}", DateTime.Now, ExceptionHelper.GetErrorText(error));
                    break;
                }

                try
                {
                    _logger.LogInformation("{time} Start awaiting notification ...", DateTime.Now);
                    int resultCode = AwaitNotification(300000); // 5 minutes
                    if (resultCode == 0)
                    {
                        _logger.LogInformation("{time} Notification received successfully.", DateTime.Now);
                    }
                    else if (resultCode == 1)
                    {
                        // notifications are not supported by database
                        await Task.Delay(60000, stoppingToken); // 1 minute
                        _logger.LogInformation("{time} 1 minute waiting elapsed.", DateTimeOffset.Now);
                    }
                    else if (resultCode == 2)
                    {
                        _logger.LogInformation("{time} No notification received.", DateTime.Now);
                    }
                }
                catch (Exception error)
                {
                    _logger.LogInformation("{time} {error}", DateTime.Now, ExceptionHelper.GetErrorText(error));
                    break;
                }
            }
            _logger.LogInformation("{time} Execution is interrupted.", DateTime.Now);
        }
        private int ReceiveMessages(int messageCount)
        {
            int messagesReceived = MessageConsumer.ReceiveMessages(messageCount, out string errorMessage);
            
            if (!string.IsNullOrEmpty(errorMessage))
            {
                _logger.LogInformation("{time} {error}", DateTime.Now, errorMessage);
            }

            return messagesReceived;
        }
        private int AwaitNotification(int timeout)
        {
            int resultCode = MessageConsumer.AwaitNotification(timeout, out string errorMessage);

            if (!string.IsNullOrEmpty(errorMessage))
            {
                _logger.LogInformation("{time} {error}", DateTime.Now, errorMessage);
            }

            return resultCode;
        }
    }
}