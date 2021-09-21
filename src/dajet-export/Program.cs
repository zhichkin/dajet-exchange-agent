using DaJet.Export.Справочник;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using Microsoft.IO;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using System.Threading;
using System.Threading.Tasks;

namespace DaJet.Export
{
    public static class Program
    {
        private static object ConsoleSyncRoot = new object();
        private const string PRESS_ANY_KEY_TO_EXIT_MESSAGE = "Press any key to exit.";
        private const string SERVER_IS_NOT_DEFINED_ERROR = "Server address is not defined.";
        private const string DATABASE_IS_NOT_DEFINED_ERROR = "Database name is not defined.";

        private static IMetadataService metadata = new MetadataService();
        
        private static int RowsLimit = 0;
        private static int BatchSize = 1000;
        private static int WaitForConfirmsTimeout = 30; // seconds

        private const string TopicExchangeName = "accord.dajet.exchange"; // "dajet-exchange";
        private const string RoutingKey = "Справочник.Партии"; // "РегистрСведений.Тестовый";

        // MultipleActiveResultSets=True

        public static int Main(string[] args)
        {
            //args = new string[] { "--ms", "ZHICHKIN", "--db", "cerberus" };
            //args = new string[] { "--ms", "ZHICHKIN", "--db", "cerberus", "--batch-size", "33000" };

            RootCommand command = new RootCommand()
            {
                new Option<string>("--ms", "Microsoft SQL Server address or name"),
                new Option<string>("--db", "Database name"),
                new Option<string>("--u", "User name (Windows authentication is used if not defined)"),
                new Option<string>("--p", "User password if SQL Server authentication is used"),
                new Option<int>("--batch-size", "Number of rows to export in one batch (default is 1000)"),
                new Option<int>("--rows-limit", "Total number of rows to export from database (default is all rows)")
            };
            command.Description = "DaJet Agent Export Tool";
            command.Handler = CommandHandler.Create<string, string, string, string, int, int>(ExecuteCommand);
            return command.Invoke(args);
        }
        private static void ShowErrorMessage(string errorText)
        {
            lock (ConsoleSyncRoot)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(errorText);
                Console.ForegroundColor = ConsoleColor.White;
            }
        }
        private static void ShowConsoleMessage(string messageText)
        {
            lock (ConsoleSyncRoot)
            {
                Console.WriteLine(messageText);
            }
        }
        private static void ShowSuccessMessage(string errorText)
        {
            lock (ConsoleSyncRoot)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(errorText);
                Console.ForegroundColor = ConsoleColor.White;
            }
        }
        private static void ExecuteCommand(string ms, string db, string u, string p, int batchSize, int rowsLimit)
        {
            if (string.IsNullOrWhiteSpace(ms))
            {
                ShowErrorMessage(SERVER_IS_NOT_DEFINED_ERROR); return;
            }
            if (string.IsNullOrWhiteSpace(db))
            {
                ShowErrorMessage(DATABASE_IS_NOT_DEFINED_ERROR); return;
            }
            if (batchSize > 0)
            {
                BatchSize = batchSize;
            }
            if (rowsLimit > 0)
            {
                RowsLimit = rowsLimit;
            }

            metadata
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .ConfigureConnectionString(ms, db, u, p);

            ShowConsoleMessage($"Open metadata for database \"{db}\" on server \"{ms}\" ...");
            Stopwatch watch = new Stopwatch();
            watch.Start();
            InfoBase infoBase = metadata.OpenInfoBase();
            watch.Stop();
            ShowConsoleMessage($"Metadata is opened successfully.");
            ShowConsoleMessage($"Elapsed: {watch.ElapsedMilliseconds} ms");

            //GetTableNames(infoBase);

            CatalogExporter exporter = new CatalogExporter()
            {
                ReportSuccess = (batch) =>
                {
                    ShowConsoleMessage($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {batch.RowNumber1}-{batch.RowNumber2} confirmed successfully.");
                },
                ReportFailure = (batch) =>
                {
                    ShowErrorMessage($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {batch.RowNumber1}-{batch.RowNumber2} failed to confirm.");
                }
            }
            .UseDatabase(metadata.ConnectionString)
            .ConfigureRabbitMQ("localhost", "/", "guest", "guest")
            .UseInfoBase(infoBase)
            .UseCatalog("Партии")
            .UseExchange(TopicExchangeName)
            .UseBinding(RoutingKey);

            try
            {
                watch.Reset();
                watch.Start();
                int messagesSent = exporter.ExportCatalogToRabbitMQ(BatchSize);
                watch.Stop();

                ShowSuccessMessage($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Messages sent: {messagesSent}");
                ShowSuccessMessage($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Elapsed: {watch.ElapsedMilliseconds} ms");

                //Документ_Чек_Exporter exporter = new Документ_Чек_Exporter(metadata);
                //exporter.ExportDocuments();
            }
            catch (Exception error)
            {
                ShowErrorMessage(error.Message);
                ShowErrorMessage(error.StackTrace);
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();

            ShowConsoleMessage(PRESS_ANY_KEY_TO_EXIT_MESSAGE);
            Console.ReadKey(false);
        }
    }
}