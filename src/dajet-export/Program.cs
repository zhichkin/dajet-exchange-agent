using DaJet.Export.Справочник;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
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
        private const string PRESS_ANY_KEY_TO_EXIT_MESSAGE = "Press any key to exit.";
        private const string SERVER_IS_NOT_DEFINED_ERROR = "Server address is not defined.";
        private const string DATABASE_IS_NOT_DEFINED_ERROR = "Database name is not defined.";

        private static IMetadataService metadata = new MetadataService();
        private static ApplicationObject catalog;

        private static IConnection RmqConnection;
        
        public static int Main(string[] args)
        {
            args = new string[] { "--ms", "ZHICHKIN", "--db", "cerberus" };

            InitializeConnection();

            RootCommand command = new RootCommand()
            {
                new Option<string>("--ms", "Microsoft SQL Server address or name"),
                new Option<string>("--db", "Database name"),
                new Option<string>("--u", "User name (Windows authentication is used if not defined)"),
                new Option<string>("--p", "User password if SQL Server authentication is used")
            };
            command.Description = "DaJet Agent Export Tool";
            command.Handler = CommandHandler.Create<string, string, string, string>(ExecuteCommand);
            return command.Invoke(args);
        }
        private static void ShowErrorMessage(string errorText)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(errorText);
            Console.ForegroundColor = ConsoleColor.White;
        }
        private static void ExecuteCommand(string ms, string db, string u, string p)
        {
            if (string.IsNullOrWhiteSpace(ms))
            {
                ShowErrorMessage(SERVER_IS_NOT_DEFINED_ERROR); return;
            }
            if (string.IsNullOrWhiteSpace(db))
            {
                ShowErrorMessage(DATABASE_IS_NOT_DEFINED_ERROR); return;
            }

            metadata
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .ConfigureConnectionString(ms, db, u, p);

            Console.WriteLine($"Open metadata for database \"{db}\" on server \"{ms}\" ...");
            Stopwatch watch = new Stopwatch();
            watch.Start();
            InfoBase infoBase = metadata.OpenInfoBase();
            watch.Stop();
            Console.WriteLine($"Metadata is opened successfully.");
            Console.WriteLine($"Elapsed: {watch.ElapsedMilliseconds} ms");

            Export_Справочник_Партии(metadata, infoBase);

            //GetTableNames(infoBase);

            Console.WriteLine(PRESS_ANY_KEY_TO_EXIT_MESSAGE);
            Console.ReadKey(false);
        }

        private static void InitializeConnection()
        {
            IConnectionFactory factory = new ConnectionFactory()
            {
                HostName = "localhost",
                VirtualHost = "/",
                UserName = "guest",
                Password = "guest",
                Port = 5672
            };
            RmqConnection = factory.CreateConnection();
        }
        private static IModel CreateChannel()
        {
            IModel channel = RmqConnection.CreateModel();
            channel.ConfirmSelect();
            return channel;
        }
        private static IBasicProperties CreateMessageProperties(IModel channel)
        {
            IBasicProperties properties = channel.CreateBasicProperties();
            properties.ContentType = "application/json";
            properties.DeliveryMode = 2; // persistent
            properties.ContentEncoding = "UTF-8";
            properties.AppId = "ERP";
            properties.Type = "Справочник.Партии";
            SetOperationTypeHeader(properties);
            return properties;
        }
        private static void SetOperationTypeHeader(IBasicProperties properties)
        {
            if (properties.Headers == null)
            {
                properties.Headers = new Dictionary<string, object>();
            }

            if (!properties.Headers.TryAdd("OperationType", "INSERT"))
            {
                properties.Headers["OperationType"] = "INSERT";
            }
        }


        private static string GetPropertyType(MetadataProperty property)
        {
            if (property.PropertyType.IsMultipleType) return "object";
            else if (property.PropertyType.IsUuid) return "Guid";
            else if (property.PropertyType.CanBeString) return "string";
            else if (property.PropertyType.CanBeNumeric) return "decimal";
            else if (property.PropertyType.CanBeBoolean) return "bool";
            else if (property.PropertyType.CanBeDateTime) return "DateTime";
            else if (property.PropertyType.CanBeReference) return "Guid";

            return "object";
        }
        private static string GetPropertyName(MetadataProperty property)
        {
            if (property.Name == "Ссылка") return "Ref";
            else if (property.Name == "ВерсияДанных") return string.Empty;
            else if (property.Name == "Предопределённый") return string.Empty;
            else if (property.Name == "ПометкаУдаления") return "DeletionMark";
            else if (property.Name == "Владелец") return "Owner";
            else if (property.Name == "Код") return "Code";
            else if (property.Name == "Наименование") return "Description";

            return property.Name;
        }

        private static string BuildClassSourceCode(ApplicationObject metaObject)
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine($"public sealed class {metaObject.Name}");
            script.AppendLine("{");

            for (int i = 0; i < metaObject.Properties.Count; i++)
            {
                MetadataProperty property = metaObject.Properties[i];

                string propertyType = GetPropertyType(property);
                string propertyName = GetPropertyName(property);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }

                script.AppendLine($"public {propertyType} {propertyName} {{ get; set; }}");
            }

            script.AppendLine("}");

            return script.ToString();
        }

        private static string Build_DeleteTable_Command(ApplicationObject metaObject)
        {
            StringBuilder script = new StringBuilder();

            script.AppendLine("IF EXISTS(SELECT 1 FROM sys.tables WHERE name = 'Справочник_Партии')");
            script.AppendLine("BEGIN");
            script.AppendLine("\tDROP TABLE [Справочник_Партии];");
            script.AppendLine("END;");

            return script.ToString();
        }
        private static string Build_CreateTable_Command(ApplicationObject metaObject)
        {
            StringBuilder script = new StringBuilder();

            script.AppendLine("CREATE TABLE [Справочник_Партии]");
            script.AppendLine("(");
            script.AppendLine("\t[RowNumber] int IDENTITY(1,1) PRIMARY KEY,");

            for (int i = 0; i < metaObject.Properties.Count; i++)
            {
                MetadataProperty property = metaObject.Properties[i];

                string propertyType = GetPropertyType(property);
                string propertyName = GetPropertyName(property);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }

                for (int ii = 0; ii < property.Fields.Count; ii++)
                {
                    DatabaseField field = property.Fields[ii];

                    if (propertyType == "Guid")
                    {
                        script.Append($"\t[{propertyName}] nvarchar(36) NOT NULL");
                    }
                    else if (propertyType == "bool")
                    {
                        script.Append($"\t[{propertyName}] bit NOT NULL");
                    }
                    else if (propertyType == "decimal")
                    {
                        script.Append($"\t[{propertyName}] numeric({field.Precision},{field.Scale}) NOT NULL");
                    }
                    else if (propertyType == "DateTime")
                    {
                        script.Append($"\t[{propertyName}] datetime2 NOT NULL");
                    }
                    else if (propertyType == "string")
                    {
                        script.Append($"\t[{propertyName}] {field.TypeName}({field.Length}) NOT NULL");
                    }
                    else
                    {
                        continue;
                    }

                    if (i != (metaObject.Properties.Count - 1))
                    {
                        script.Append(",");
                    }
                    else if (ii != (property.Fields.Count - 1))
                    {
                        script.Append(",");
                    }

                    script.AppendLine();
                }
            }

            script.AppendLine(");");

            return script.ToString();
        }
        private static string Build_InsertTable_Command(ApplicationObject metaObject)
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine("INSERT [Справочник_Партии]");
            script.AppendLine("(");
            
            for (int i = 0; i < metaObject.Properties.Count; i++)
            {
                MetadataProperty property = metaObject.Properties[i];

                string propertyType = GetPropertyType(property);
                string propertyName = GetPropertyName(property);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }

                for (int ii = 0; ii < property.Fields.Count; ii++)
                {
                    DatabaseField field = property.Fields[ii];

                    if (propertyType == "Guid"
                        || propertyType == "bool"
                        || propertyType == "decimal"
                        || propertyType == "DateTime"
                        || propertyType == "string")
                    {
                        script.Append($"\t[{propertyName}]");
                    }
                    else
                    {
                        continue;
                    }

                    if (i != (metaObject.Properties.Count - 1))
                    {
                        script.Append(",");
                    }
                    else if (ii != (property.Fields.Count - 1))
                    {
                        script.Append(",");
                    }

                    script.AppendLine();
                }
            }

            script.AppendLine(")");

            script.AppendLine(Build_SelectSource_Command(metaObject));

            script.Append(";");

            return script.ToString();
        }
        private static string Build_SelectSource_Command(ApplicationObject metaObject)
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine("SELECT");

            for (int i = 0; i < metaObject.Properties.Count; i++)
            {
                MetadataProperty property = metaObject.Properties[i];

                string propertyType = GetPropertyType(property);
                string propertyName = GetPropertyName(property);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }

                for (int ii = 0; ii < property.Fields.Count; ii++)
                {
                    DatabaseField field = property.Fields[ii];

                    if (propertyType == "Guid")
                    {
                        script.Append($"\t[dbo].[fn_sql_to_1c_uuid]({field.Name}) AS [{propertyName}]");
                    }
                    else if (propertyType == "bool")
                    {
                        script.Append($"\tCAST({field.Name} AS bit) AS [{propertyName}]");
                    }
                    else
                    {
                        script.Append($"\t{field.Name} AS [{propertyName}]");
                    }

                    if (i != (metaObject.Properties.Count - 1))
                    {
                        script.Append(",");
                    }
                    else if (ii != (property.Fields.Count - 1))
                    {
                        script.Append(",");
                    }

                    script.AppendLine();
                }
            }

            script.AppendLine("FROM");
            script.Append("\t");
            script.Append(metaObject.TableName);
            script.Append(";");

            return script.ToString();
        }
        private static string Build_SelectTarget_Command(ApplicationObject metaObject)
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine("SELECT");

            for (int i = 0; i < metaObject.Properties.Count; i++)
            {
                MetadataProperty property = metaObject.Properties[i];

                string propertyType = GetPropertyType(property);
                string propertyName = GetPropertyName(property);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }

                script.Append($"\t[{propertyName}]");

                if (i != (metaObject.Properties.Count - 1))
                {
                    script.Append(",");
                }
                script.AppendLine();
            }

            script.AppendLine("FROM");
            script.Append("\t");
            script.Append("[Справочник_Партии]");
            script.Append(";");

            return script.ToString();
        }
        private static string Build_SelectMaxRowNumber_Command()
        {
            StringBuilder script = new StringBuilder();

            script.AppendLine("SELECT MAX(RowNumber) AS [MaxRowNumber] FROM [Справочник_Партии];");

            return script.ToString();
        }
        private static string Build_SelectBatch_Command(ApplicationObject metaObject)
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine("SELECT");

            for (int i = 0; i < metaObject.Properties.Count; i++)
            {
                MetadataProperty property = metaObject.Properties[i];

                string propertyType = GetPropertyType(property);
                string propertyName = GetPropertyName(property);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }

                script.Append($"\t[{propertyName}]");

                if (i != (metaObject.Properties.Count - 1))
                {
                    script.Append(",");
                }
                script.AppendLine();
            }

            script.AppendLine("FROM");
            script.AppendLine("\t[Справочник_Партии]");
            script.AppendLine("WHERE");
            script.Append("\t[RowNumber] BETWEEN @RowNumber1 AND @RowNumber2;");

            return script.ToString();
        }

        private static void DeleteTargetTable(IMetadataService metadata, ApplicationObject metaObject)
        {
            string script = Build_DeleteTable_Command(metaObject);

            using (SqlConnection connection = new SqlConnection(metadata.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = script;
                    command.CommandTimeout = 60; // seconds

                    int rowsAffected = command.ExecuteNonQuery();
                }
            }
        }
        private static void CreateTargetTable(IMetadataService metadata, ApplicationObject metaObject)
        {
            string script = Build_CreateTable_Command(metaObject);

            using (SqlConnection connection = new SqlConnection(metadata.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = script;
                    command.CommandTimeout = 60; // seconds

                    int rowsAffected = command.ExecuteNonQuery();
                }
            }
        }
        private static void InsertTargetTable(IMetadataService metadata, ApplicationObject metaObject)
        {
            string script = Build_InsertTable_Command(metaObject);

            using (SqlConnection connection = new SqlConnection(metadata.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = script;
                    command.CommandTimeout = 600; // seconds

                    int rowsAffected = command.ExecuteNonQuery();
                }
            }
        }
        private static int GetMaxRowNumber(IMetadataService metadata)
        {
            int rowCount = 0;

            using (SqlConnection connection = new SqlConnection(metadata.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = Build_SelectMaxRowNumber_Command();
                    command.CommandTimeout = 60; // seconds

                    rowCount = (int)command.ExecuteScalar();
                }
            }

            return rowCount;
        }
        private static List<T> SelectCatalogItems<T>(IMetadataService metadata, ApplicationObject metaObject) where T : new()
        {
            List<T> list = new List<T>();

            using (SqlConnection connection = new SqlConnection(metadata.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = Build_SelectTarget_Command(metaObject);
                    command.CommandTimeout = 60; // seconds
                    
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            T item = new T();

                            MapDataToObject(reader, item, metaObject);

                            list.Add(item);
                        }
                        reader.Close();
                    }
                }
            }

            return list;
        }
        private static void MapDataToObject<T>(SqlDataReader reader, T item, ApplicationObject metaObject)
        {
            Type type = typeof(T);

            for (int i = 0; i < metaObject.Properties.Count; i++)
            {
                MetadataProperty property = metaObject.Properties[i];

                string propertyType = GetPropertyType(property);
                string propertyName = GetPropertyName(property);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }

                PropertyInfo proxy = type.GetProperty(propertyName);
                if (proxy == null)
                {
                    continue;
                }

                object value = null;
                if (propertyType == "Guid")
                {
                    value = new Guid(reader.GetString(propertyName));
                }
                else if (propertyType == "string")
                {
                    value = reader.GetString(propertyName);
                }
                else if (propertyType == "decimal")
                {
                    value = reader.GetDecimal(propertyName);
                }
                else if (propertyType == "bool")
                {
                    value = reader.GetBoolean(propertyName);
                }
                else if (propertyType == "DateTime")
                {
                    value = reader.GetDateTime(propertyName).AddYears(-2000);
                }

                proxy.SetValue(item, value);
            }
        }

        private static string SerializeCatalogItem(Партии item)
        {
            JsonWriterOptions options = new JsonWriterOptions
            {
                Indented = false,
                Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
            };

            MemoryStream stream = new MemoryStream();
            Utf8JsonWriter writer = new Utf8JsonWriter(stream, options);

            writer.WriteStartObject();
            writer.WriteString("#type", "jcfg:CatalogObject.Партии");

            writer.WritePropertyName("#value");
            writer.WriteStartObject();

            Type type = typeof(Партии);
            foreach (PropertyInfo property in type.GetProperties())
            {
                if (property.Name == "Owner")
                {
                    SerializePropertyOwner(writer, item, property);
                    continue;
                }

                writer.WritePropertyName(property.Name);
                if (property.PropertyType == typeof(string))
                {
                    writer.WriteStringValue((string)property.GetValue(item));
                }
                else if (property.PropertyType == typeof(Guid))
                {
                    writer.WriteStringValue(((Guid)property.GetValue(item)).ToString());
                }
                else if (property.PropertyType == typeof(decimal))
                {
                    writer.WriteNumberValue((decimal)property.GetValue(item));
                }
                else if (property.PropertyType == typeof(DateTime))
                {
                    writer.WriteStringValue(((DateTime)property.GetValue(item)).ToString("yyyy-MM-ddThh:mm:ss"));
                }
                else if (property.PropertyType == typeof(bool))
                {
                    writer.WriteBooleanValue((bool)property.GetValue(item));
                }
                else
                {
                    writer.WriteNullValue();
                }
            }

            writer.WriteEndObject();

            writer.WriteEndObject();
            writer.Flush();

            string json = Encoding.UTF8.GetString(stream.ToArray());

            return json;
        }
        private static void SerializePropertyOwner(Utf8JsonWriter writer, Партии item, PropertyInfo property)
        {
            writer.WritePropertyName("Owner");
            writer.WriteStartObject();

            writer.WriteString("#type", "jcfg:CatalogRef.Номенклатура");

            writer.WriteString("#value", ((Guid)property.GetValue(item)).ToString());

            writer.WriteEndObject();
        }

        private static void Export_Справочник_Партии(IMetadataService metadata, InfoBase infoBase)
        {
            catalog = infoBase.Catalogs.Values.Where(c => c.Name == "Партии").FirstOrDefault();

            if (catalog == null)
            {
                Console.WriteLine("Справочник \"Партии\" не найден."); return;
            }

            try
            {
                DeleteTargetTable(metadata, catalog);
                Console.WriteLine("Таблица \"[Справочник_Партии]\" удалена.");
                CreateTargetTable(metadata, catalog);
                Console.WriteLine("Таблица \"[Справочник_Партии]\" создана.");
                InsertTargetTable(metadata, catalog);
                Console.WriteLine("Таблица \"[Справочник_Партии]\" скопирована.");
            }
            catch (Exception error)
            {
                ShowErrorMessage(error.Message); return;
            }

            Stopwatch watch = new Stopwatch();
            watch.Start();

            try
            {
                ExecuteJob_Справочник_Партии(metadata);
                Console.WriteLine("Таблица \"[Справочник_Партии]\" выгружена.");
            }
            catch (Exception error)
            {
                ShowErrorMessage(error.Message); return;
            }

            watch.Stop();
            Console.WriteLine("Elapsed = " + watch.ElapsedMilliseconds.ToString());

            try
            {
                DeleteTargetTable(metadata, catalog);
                Console.WriteLine("Таблица \"[Справочник_Партии]\" удалена.");
            }
            catch (Exception error)
            {
                ShowErrorMessage(error.Message); return;
            }
        }
        private static void ExecuteJob_Справочник_Партии(IMetadataService metadata)
        {
            int MaxRowNumber = GetMaxRowNumber(metadata);
            int MaxThreads = Environment.ProcessorCount;
            
            if (MaxRowNumber == 0)
            {
                return;
            }

            Console.WriteLine($"Max row number = {MaxRowNumber}");

            List<JobInfo> jobs = new List<JobInfo>(MaxThreads);
            for (int i = 0; i < MaxThreads; i++)
            {
                JobInfo job = new JobInfo()
                {
                    Channel = CreateChannel()
                };
                job.Properties = CreateMessageProperties(job.Channel);

                jobs.Add(job);
            }

            int end = 0;
            int step = 4;
            int start = 1;
            int nextThread = 0;
            while (MaxRowNumber >= start)
            {
                end = start + step - 1;

                BatchInfo batch = new BatchInfo()
                {
                    RowNumber1 = start,
                    RowNumber2 = end
                };

                jobs[nextThread].Batches.Enqueue(batch);

                nextThread++;
                if (nextThread == MaxThreads)
                {
                    nextThread = 0;
                }

                start = start + step;
            }

            //for (int i = 0; i < MaxThreads; i++)
            //{
            //    Queue<BatchInfo> queue = jobs[i];
            //    Console.WriteLine($"Thread {i}:");
            //    while (queue.Count > 0)
            //    {
            //        BatchInfo batch = queue.Dequeue();
            //        Console.WriteLine($"\tBatch from {batch.RowNumber1} to {batch.RowNumber2}");
            //    }
            //}

            int messagesSent = ExecuteJobsInParallel(jobs);
            Console.WriteLine($"Messages sent = {messagesSent}");
        }
        private static int ExecuteJobsInParallel(List<JobInfo> jobs)
        {
            int messagesSent = 0;

            using (var SendingCancellation = new CancellationTokenSource())
            {
                Task<int>[] tasks = new Task<int>[jobs.Count];

                for (int i = 0; i < jobs.Count; i++)
                {
                    tasks[i] = Task.Factory.StartNew(
                        ExecuteJobsInBackground,
                        jobs[i],
                        SendingCancellation.Token,
                        TaskCreationOptions.LongRunning,
                        TaskScheduler.Default);
                }

                Task.WaitAll(tasks, SendingCancellation.Token);

                foreach (Task<int> task in tasks)
                {
                    messagesSent += task.Result;
                }
            }

            return messagesSent;
        }
        private static int ExecuteJobsInBackground(object job)
        {
            if (!(job is JobInfo info))
            {
                return 0;
            }

            int counter = 0;
            while (info.Batches.Count > 0)
            {
                counter += ExecuteJob(info.Channel, info.Properties, info.Batches.Dequeue());
                
                WaitForConfirms(info.Channel); // wait for each 1000-th message ?
            }

            return counter;
        }
        private static int ExecuteJob(IModel channel, IBasicProperties properties, BatchInfo batch)
        {
            int messagesSent = 0;

            using (SqlConnection connection = new SqlConnection(metadata.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = Build_SelectBatch_Command(catalog);
                    command.CommandTimeout = 60; // seconds
                    command.Parameters.AddWithValue("RowNumber1", batch.RowNumber1);
                    command.Parameters.AddWithValue("RowNumber2", batch.RowNumber2);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            SendMessage(channel, properties, reader.GetString("Description"));
                            messagesSent++;

                            //T item = new T();
                            //MapDataToObject(reader, item, catalog);
                            //list.Add(item);
                        }
                        reader.Close();
                    }
                }
            }

            return messagesSent;
        }
        private static void SendMessage(IModel channel, IBasicProperties properties, string message)
        {
            byte[] messageBytes = Encoding.UTF8.GetBytes(message);

            properties.MessageId = Guid.NewGuid().ToString();
            
            channel.BasicPublish("dajet-exchange", "РегистрСведений.Тестовый", properties, messageBytes);

            Console.WriteLine($"Message sent: {message}, thread: {Thread.CurrentThread.ManagedThreadId}");

            //List<Партии> list = SelectCatalogItems<Партии>(metadata, metaObject);

            //Console.WriteLine($"Справочник \"Партии\" = {list.Count} items.");

            //int counter = 0;
            //foreach (Партии item in list)
            //{
            //    string json = SerializeCatalogItem(item);

            //    if (counter == 0)
            //    {
            //        Console.WriteLine(json);
            //    }
            //}
        }
        private static void WaitForConfirms(IModel channel)
        {
            try
            {
                bool confirmed = channel.WaitForConfirms(TimeSpan.FromSeconds(10), out bool timedout);
                if (!confirmed)
                {
                    //if (timedout)
                    //{
                    //    SendingExceptions.Enqueue(new OperationCanceledException(PUBLISHER_CONFIRMATION_TIMEOUT_MESSAGE));
                    //}
                    //else
                    //{
                    //    SendingExceptions.Enqueue(new OperationCanceledException(PUBLISHER_CONFIRMATION_ERROR_MESSAGE));
                    //}
                    //SendingCancellation.Cancel();
                }
            }
            catch (OperationInterruptedException rabbitError)
            {
                //SendingExceptions.Enqueue(rabbitError);
                //if (string.IsNullOrWhiteSpace(rabbitError.Message) || !rabbitError.Message.Contains("NOT_FOUND"))
                //{
                //    SendingCancellation.Cancel();
                //}
            }
            catch (Exception error)
            {
                //SendingExceptions.Enqueue(error);
                //SendingCancellation.Cancel();
            }
        }


        private static void GetTableNames(InfoBase infoBase)
        {
            ApplicationObject catalog = infoBase.Catalogs.Values.Where(c => c.Name == "Партии").FirstOrDefault();
            if (catalog == null)
            {
                Console.WriteLine("Справочник \"Партии\" не найден.");
            }
            else
            {
                Console.WriteLine($"Справочник \"Партии\": {catalog.TableName}");
            }

            ApplicationObject document = infoBase.Documents.Values.Where(c => c.Name == "Чеки").FirstOrDefault();
            if (document == null)
            {
                Console.WriteLine("Документ \"Чеки\" не найден.");
            }
            else
            {
                Console.WriteLine($"Документ \"Чеки\": {document.TableName}");
                foreach (TablePart table in document.TableParts)
                {
                    Console.WriteLine($"- Табличная часть \"{table.Name}\": {table.TableName}");
                }
            }
        }
    }
}