﻿using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.IO;
using System.Reflection;

namespace DaJet.RabbitMQ.Producer
{
    public static class Program
    {
        static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }
        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                .UseWindowsService()
                .ConfigureServices((context, services) =>
                {
                    services.AddOptions();
                    ConfigureAppSettings(services);
                    services.AddSingleton<IMessageProducer, MessageProducer>();
                    services.AddSingleton<IMessageConsumer, MessageConsumer>();
                    services.AddHostedService<Worker>();
                });
        }
        private static void ConfigureAppSettings(IServiceCollection services)
        {
            Assembly asm = Assembly.GetExecutingAssembly();
            string appCatalogPath = Path.GetDirectoryName(asm.Location);

            AppSettings settings = new AppSettings();
            IConfigurationRoot config = new ConfigurationBuilder()
                .SetBasePath(appCatalogPath)
                .AddJsonFile("appsettings.json", optional: false)
                .Build();
            config.Bind(settings);

            FileLogger.LogSize = settings.LogSize;

            services.Configure<AppSettings>(config);
            services.Configure<DaJetExchangeQueue>(config.GetSection("DaJetExchangeQueue"));
            services.Configure<MessageConsumerSettings>(config.GetSection("ConsumerSettings"));
            services.Configure<MessageProducerSettings>(config.GetSection("ProducerSettings"));
        }
    }
}