using DaJet.Metadata;
using DaJet.Metadata.Model;
using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.Linq;

namespace DaJet.Setup
{
    public static class Program
    {
        private const string INCOMING_QUEUE_NAME = "DaJetExchangeВходящаяОчередь";
        private const string OUTGOING_QUEUE_NAME = "DaJetExchangeИсходящаяОчередь";

        private const string SERVER_IS_NOT_DEFINED_ERROR = "Server address is not defined.";
        private const string DATABASE_IS_NOT_DEFINED_ERROR = "Database name is not defined.";
        
        public static int Main(string[] args)
        {
            args = new string[] { "--ms", "zhichkin", "--db", "cerberus", "--verbose" };
            //args = new string[] { "--pg", "127.0.0.1", "--db", "test_node_2", "--usr", "postgres", "--pwd", "postgres" };

            RootCommand command = new RootCommand()
            {
                new Option<string>("--ms", "Microsoft SQL Server address or name"),
                new Option<string>("--pg", "PostgresSQL server address or name"),
                new Option<string>("--db", "Database name"),
                new Option<string>("--usr", "User name (Windows authentication is used if not defined)"),
                new Option<string>("--pwd", "User password if SQL Server authentication is used"),
                new Option<bool>("--verbose", "Verbose mode: shows detailed output in console window")
            };
            command.Description = "DaJet Exchange Setup Utility 1.0";
            command.Handler = CommandHandler.Create<string, string, string, string, string, bool>(ExecuteCommand);
            return command.Invoke(args);
        }
        private static void ShowErrorMessage(string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(message);
            Console.ForegroundColor = ConsoleColor.White;
        }
        private static void ShowSuccessMessage(string message)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(message);
            Console.ForegroundColor = ConsoleColor.White;
        }
        private static void ExecuteCommand(string ms, string pg, string db, string usr, string pwd, bool verbose)
        {
            if (string.IsNullOrWhiteSpace(ms) && string.IsNullOrWhiteSpace(pg))
            {
                ShowErrorMessage(SERVER_IS_NOT_DEFINED_ERROR); return;
            }

            if (string.IsNullOrWhiteSpace(db))
            {
                ShowErrorMessage(DATABASE_IS_NOT_DEFINED_ERROR); return;
            }

            IMetadataService metadataService = ConfigureMetadataService(ms, pg, db, usr, pwd);

            if (TryOpenInfoBase(metadataService, out InfoBase infoBase, out string errorMessage))
            {
                ShowInfoBaseInfo(infoBase);
                SetupDatabase(infoBase, metadataService);
            }
            else
            {
                ShowErrorMessage(errorMessage);
            }

            if (verbose)
            {
                //Console.WriteLine("verbose mode");
            }
        }
        private static IMetadataService ConfigureMetadataService(string ms, string pg, string db, string usr, string pwd)
        {
            IMetadataService metadataService = new MetadataService();

            if (!string.IsNullOrWhiteSpace(ms))
            {
                metadataService
                    .UseDatabaseProvider(DatabaseProvider.SQLServer)
                    .ConfigureConnectionString(ms, db, usr, pwd);
            }
            else if (!string.IsNullOrWhiteSpace(pg))
            {
                metadataService
                    .UseDatabaseProvider(DatabaseProvider.PostgreSQL)
                    .ConfigureConnectionString(pg, db, usr, pwd);
            }

            return metadataService;
        }
        private static bool TryOpenInfoBase(IMetadataService metadataService, out InfoBase infoBase, out string errorMessage)
        {
            errorMessage = null;

            try
            {
                infoBase = metadataService.OpenInfoBase();
            }
            catch (Exception error)
            {
                infoBase = null;
                errorMessage = ExceptionHelper.GetErrorText(error);
            }

            return (errorMessage == null);
        }
        private static void ShowInfoBaseInfo(InfoBase infoBase)
        {
            ConfigInfo config = infoBase.ConfigInfo;

            Console.WriteLine();
            Console.WriteLine("Name = " + config.Name);
            Console.WriteLine("Alias = " + config.Alias);
            Console.WriteLine("Comment = " + config.Comment);
            Console.WriteLine("Version = " + config.Version);
            Console.WriteLine("ConfigVersion = " + config.ConfigVersion);
            Console.WriteLine();
        }
        private static void SetupDatabase(InfoBase infoBase, IMetadataService metadataService)
        {
            SetupIncomingQueue(infoBase, metadataService);
            SetupOutgoingQueue(infoBase, metadataService);
        }
        
        
        
        private static void SetupIncomingQueue(InfoBase infoBase, IMetadataService metadataService)
        {
            ApplicationObject metaObject = infoBase
                .InformationRegisters.Values
                .Where(o => o.Name == INCOMING_QUEUE_NAME)
                .FirstOrDefault();

            if (metaObject == null)
            {
                ShowErrorMessage($"Metadata object \"РегистрСведений.{INCOMING_QUEUE_NAME}\" is not found.");
                return;
            }

            if (IncomingQueueInterfaceIsValid(metaObject))
            {
                ShowSuccessMessage($"РегистрСведений.{INCOMING_QUEUE_NAME}");
            }
        }
        private static bool IncomingQueueInterfaceIsValid(ApplicationObject queue)
        {
            return true;
        }



        private static void SetupOutgoingQueue(InfoBase infoBase, IMetadataService metadataService)
        {
            ApplicationObject metaObject = infoBase
                .InformationRegisters.Values
                .Where(o => o.Name == OUTGOING_QUEUE_NAME)
                .FirstOrDefault();

            if (metaObject == null)
            {
                ShowErrorMessage($"Metadata object \"РегистрСведений.{OUTGOING_QUEUE_NAME}\" is not found.");
                return;
            }

            if (IncomingQueueInterfaceIsValid(metaObject))
            {
                ShowSuccessMessage($"РегистрСведений.{OUTGOING_QUEUE_NAME}");
            }
        }
    }
}