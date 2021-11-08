using Accord.GraphQL;
using Accord.GraphQL.Model;
using DaJet.Database.Messaging;
using DaJet.FileLogger;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using System.Threading.Tasks;

namespace tests
{
    [TestClass] public sealed class TestGraphQL
    {
        private readonly IMetadataService metadata = new MetadataService();
        private readonly IAccordGraphQLClient client = new AccordGraphQLClient();
        private readonly IDatabaseMessageProducer producer = new DatabaseMessageProducer();

        private const string USERNAME = "";
        private const string PASSWORD = "";
        private const string API_BASE_URI = "";

        private const string CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=cerberus;Integrated Security=True";

        public TestGraphQL()
        {
            client
                .UseCredentials(USERNAME, PASSWORD)
                .UseServerAddress(API_BASE_URI);
        }

        [TestMethod] public async Task TestProductSearch()
        {
            int pageNum = 1;
            int perPage = 10;
            DateTime dateUtc = new DateTime(2021, 10, 15);
            ProductSearch result = await client.GetUpdatedProducts(dateUtc, pageNum, perPage);

            if (result == null)
            {
                Console.WriteLine($"Failed to get products list.");
                return;
            }

            Console.WriteLine();
            Console.WriteLine($"Start date: {dateUtc:yyyy-MM-dd}");
            Console.WriteLine($"Page number: {result.pageInfo.page}");
            Console.WriteLine($"Items per page: {result.pageInfo.perPage}");
            Console.WriteLine($"Total items: {result.pageInfo.total}");

            int counter = 0;
            Console.WriteLine();
            foreach (Product product in result.items)
            {
                counter++;
                Console.WriteLine($"{counter}. Product {product.id} ({product.code})");
                Console.WriteLine($"{product.name} is MDLP = {product.isMdlp}");
                Console.WriteLine($"Last updated: {product.updatedAt:yyyy-MM-dd HH:mm:ss}");
                foreach (GtinCode gtin in product.gtinCodes)
                {
                    Console.WriteLine($"- {gtin.code}");
                }
                Console.WriteLine();
            }
        }

        [TestMethod] public async Task TestDatabaseProducer()
        {
            metadata
                .UseConnectionString(CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.SQLServer);

            InfoBase infoBase = metadata.OpenInfoBase();

            producer
                .UseConnectionString(CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .Initialize(infoBase, "РегистрСведений.DaJetExchangeВходящаяОчередь");

            Console.WriteLine(producer.InsertMessageScript);

            //int pageNum = 1;
            //int perPage = 1000;
            //DateTime dateUtc = new DateTime(2021, 1, 1);
            //ProductSearch result = await client.GetUpdatedProducts(dateUtc, pageNum, perPage);

            //if (result == null)
            //{
            //    Console.WriteLine($"Failed to get products list.");
            //    return;
            //}

            //Console.WriteLine();
            //Console.WriteLine($"Start date: {dateUtc:yyyy-MM-dd}");
            //Console.WriteLine($"Page number: {result.pageInfo.page}");
            //Console.WriteLine($"Items per page: {result.pageInfo.perPage}");
            //Console.WriteLine($"Total items: {result.pageInfo.total}");

            JsonSerializerOptions options = new JsonSerializerOptions()
            {
                WriteIndented = false,
                Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
            };

            int total = 0;
            int pageNum = 0;
            int perPage = 100;
            DateTime dateUtc = new DateTime(2021, 10, 15);
            do
            {
                pageNum++;
                ProductSearch result = await client.GetUpdatedProducts(dateUtc, pageNum, perPage);

                if (total == 0)
                {
                    Console.WriteLine();
                    Console.WriteLine($"Start date: {dateUtc:yyyy-MM-dd}");
                    Console.WriteLine($"Page number: {result.pageInfo.page}");
                    Console.WriteLine($"Items per page: {result.pageInfo.perPage}");
                    Console.WriteLine($"Total items: {result.pageInfo.total}");

                    total = result.pageInfo.total;
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
                }

                //total -= result.pageInfo.perPage;
                total = 0;

            } while (total > 0);

            Console.WriteLine();
            Console.WriteLine("The end =)");
        }

        [TestMethod] public void TestSqlLiteSettingsProvider()
        {
            SqlLiteSettingsProvider settings = new SqlLiteSettingsProvider();
            settings.UseCatalogPath(FileLogger.CatalogPath);
            
            DateTime value = settings.GetSetting<DateTime>("LastUpdated");

            Console.WriteLine("Current value = " + value.ToString("yyyy-MM-dd HH:mm:ss"));

            //DateTime newValue = DateTime.UtcNow;
            //newValue = new DateTime(newValue.Year, newValue.Month, newValue.Day, newValue.Hour, newValue.Minute, newValue.Second);
            //settings.SetSetting("LastUpdated", newValue);
            //DateTime getValue = settings.GetSetting<DateTime>("LastUpdated");

            //Console.WriteLine($"New value = {newValue}");
            //Console.WriteLine($"Get value = {getValue}");
            //Console.WriteLine($"New value == Get value >> {newValue == getValue}");
        }
    }
}