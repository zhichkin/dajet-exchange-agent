using Accord.GraphQL.Model;
using DaJet.Database.Messaging;
using DaJet.FileLogger;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using System.Threading;
using System.Threading.Tasks;

namespace Accord.GraphQL
{
    public sealed class AccordGraphQLService : BackgroundService
    {
        private const string LAST_UPDATED_SETTING_NAME = "LastUpdated";

        private GraphQLSettings Settings { get; set; }
        
        private readonly IAccordGraphQLClient client = new AccordGraphQLClient();
        private readonly IDatabaseMessageProducer producer = new DatabaseMessageProducer();
        private readonly SqlLiteSettingsProvider SettingsProvider = new SqlLiteSettingsProvider();

        public AccordGraphQLService(IOptions<GraphQLSettings> options)
        {
            Settings = options.Value;
        }
        public override Task StartAsync(CancellationToken cancellationToken)
        {
            FileLogger.Log("GraphQL: initializing ...");
            FileLogger.Log($"GraphQL: {Settings.Source}");
            FileLogger.Log($"GraphQL: {Settings.Target}");
            FileLogger.Log($"GraphQL: {Settings.IncomingQueue}");

            try
            {
                InitilaizeService();
            }
            catch (Exception error)
            {
                FileLogger.Log("GraphQL: initialization failed.");
                FileLogger.Log(error);
                return Task.CompletedTask;
            }

            FileLogger.Log("GraphQL: service is running.");

            return base.StartAsync(cancellationToken);
        }
        public override Task StopAsync(CancellationToken cancellationToken)
        {
            FileLogger.Log("GraphQL: service is stopped.");
            return base.StopAsync(cancellationToken);
        }
        protected override Task ExecuteAsync(CancellationToken cancellationToken)
        {
            // Running the job in the background
            _ = Task.Run(async () =>
            {
                await DoWork(cancellationToken);
            },
            cancellationToken);

            // Return completed task to let other services to run
            return Task.CompletedTask;
        }

        private void InitilaizeService()
        {
            SettingsProvider.UseCatalogPath(FileLogger.CatalogPath);

            client
                .UseServerAddress(Settings.Source)
                .UseCredentials(Settings.UserName, Settings.Password);

            IMetadataService metadata = new MetadataService();

            metadata
                    .UseConnectionString(Settings.Target)
                    .UseDatabaseProvider(DatabaseProvider.SQLServer);

            InfoBase infoBase = metadata.OpenInfoBase();

            producer
                .UseConnectionString(metadata.ConnectionString)
                .UseDatabaseProvider(metadata.DatabaseProvider)
                .Initialize(infoBase, Settings.IncomingQueue);

            FileLogger.Log(producer.InsertMessageScript);
        }
        private async Task DoWork(CancellationToken cancellationToken)
        {
            int delaySeconds;

            while (!cancellationToken.IsCancellationRequested)
            {
                List<string> errors = await TryDoWork();

                FileLogger.Log(errors);

                delaySeconds = GetDelaySeconds(errors);

                await Task.Delay(TimeSpan.FromSeconds(delaySeconds), cancellationToken);
            }
        }
        private int GetDelaySeconds(List<string> errors)
        {
            if (errors != null && errors.Count > 0)
            {
                return Settings.RetryDelay;
            }
            
            return Settings.Periodicity;
        }
        private async Task<List<string>> TryDoWork()
        {
            List<string> errors;

            try
            {
                errors = await DownloadUpdatedProducts(); 
            }
            catch (Exception error)
            {
                errors = new List<string>();
                errors.Add(FileLogger.GetErrorText(error));
            }

            return errors;
        }

        private async Task<List<string>> DownloadUpdatedProducts()
        {
            List<string> errors = new List<string>();

            JsonSerializerOptions options = new JsonSerializerOptions()
            {
                WriteIndented = false,
                Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
            };

            int total = 0;
            int pageNum = 0;
            int perPage = 1000;
            int counter = 0;
            DateTime lastUpdated = DateTime.MinValue;
            DateTime dateUtc = SettingsProvider.GetSetting<DateTime>(LAST_UPDATED_SETTING_NAME);

            FileLogger.Log($"GraphQL: current timestamp = {dateUtc:yyyy-MM-dd HH:mm:ss}");

            do
            {
                pageNum++;
                ProductSearch result = await client.GetUpdatedProducts(dateUtc, pageNum, perPage);

                if (total == 0)
                {
                    total = result.pageInfo.total;

                    FileLogger.Log($"GraphQL: loading {total} products ...");
                }

                foreach (Product product in result.items)
                {
                    DatabaseIncomingMessage message = new DatabaseIncomingMessage()
                    {
                        Sender = "AC",
                        OperationType = "UPDATE",
                        MessageType = "Accord.Product",
                        MessageBody = JsonSerializer.Serialize(product, options)
                    };

                    producer.InsertMessage(message);
                    counter++;

                    if (product.updatedAt > lastUpdated)
                    {
                        lastUpdated = product.updatedAt;
                    }
                }

                FileLogger.Log($"GraphQL: {counter} products loaded.");

                total -= result.pageInfo.perPage;

            } while (total > 0);

            SettingsProvider.SetSetting(LAST_UPDATED_SETTING_NAME, lastUpdated);

            FileLogger.Log($"GraphQL: new timestamp = {lastUpdated:yyyy-MM-dd HH:mm:ss}");

            FileLogger.Log($"GraphQL: loaded {counter} products.");

            return errors;
        }
    }
}