using Accord.GraphQL;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System.IO;
using System.Reflection;

namespace DaJet.RabbitMQ.Producer
{
    public static class Program
    {
        private static AppSettings AppSettings { get; set; } = new AppSettings();
        public static void Main(string[] args)
        {
            InitializeAppSettings();

            FileLogger.LogSize = AppSettings.LogSize;

            FileLogger.Log("Hosting service is started.");
            CreateHostBuilder(args).Build().Run();
            FileLogger.Log("Hosting service is stopped.");
        }
        private static void InitializeAppSettings()
        {
            Assembly asm = Assembly.GetExecutingAssembly();
            string catalogPath = Path.GetDirectoryName(asm.Location);

            IConfigurationRoot config = new ConfigurationBuilder()
                .SetBasePath(catalogPath)
                .AddJsonFile("appsettings.json", optional: false)
                .Build();

            config.Bind(AppSettings);

            if (string.IsNullOrWhiteSpace(AppSettings.AppCatalog))
            {
                AppSettings.AppCatalog = catalogPath;
            }
        }
        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                .UseWindowsService()
                .ConfigureAppConfiguration(config =>
                {
                    config.Sources.Clear();
                    config
                        .SetBasePath(AppSettings.AppCatalog)
                        .AddJsonFile("appsettings.json", optional: false);
                })
                .ConfigureServices(ConfigureServices);
        }
        private static void ConfigureServices(HostBuilderContext context, IServiceCollection services)
        {
            services
                .AddOptions()
                .AddSingleton(Options.Create(AppSettings))
                .Configure<HostOptions>(context.Configuration.GetSection(nameof(HostOptions)))
                .Configure<DaJetExchangeQueue>(context.Configuration.GetSection("DaJetExchangeQueue"))
                .Configure<MessageConsumerSettings>(context.Configuration.GetSection("ConsumerSettings"))
                .Configure<MessageProducerSettings>(context.Configuration.GetSection("ProducerSettings"));

            services.AddSingleton<IMessageProducer, MessageProducer>();
            services.AddSingleton<IMessageConsumer, MessageConsumer>();
            services.AddHostedService<Worker>();

            if (AppSettings.UseGraphQL)
            {
                services
                    .Configure<GraphQLSettings>(context.Configuration.GetSection("GraphQL"))
                    .AddHostedService<AccordGraphQLService>();
            }
        }

        //private static void ConfigureProducerSettings(IServiceCollection services)
        //{
        //    IConfigurationRoot config = new ConfigurationBuilder()
        //        .SetBasePath(AppSettings.AppCatalog)
        //        .AddJsonFile("producer-settings.json", optional: false)
        //        .Build();
        //    services.Configure<MessageProducerSettings>(config);
        //}
    }
}