using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using Microsoft.IO;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;
using System.Threading;
using System.Threading.Tasks;

namespace DaJet.Export
{
    public sealed class CatalogExporter
    {
        private string _sql_script_SelectSource;
        private string _sql_script_DeleteTable;
        private string _sql_script_CreateTable;
        private string _sql_script_InsertTable;
        private string _sql_script_SelectMaxRowNumber;
        private string _sql_script_SelectBatch;

        private InfoBase _infoBase;
        private ApplicationObject _catalog;
        private string _connectionString;
        private RecyclableMemoryStreamManager _streamManager = new RecyclableMemoryStreamManager();

        public Action<BatchInfo> ReportSuccess;
        public Action<BatchInfo> ReportFailure;

        public CatalogExporter() { }

        public CatalogExporter UseInfoBase(InfoBase infoBase)
        {
            _infoBase = infoBase ?? throw new ArgumentNullException(nameof(infoBase));

            return this;
        }
        public CatalogExporter UseDatabase(string connectionString)
        {
            _connectionString = connectionString;
            return this;
        }
        public CatalogExporter UseCatalog(string catalogName)
        {
            _catalog = null;

            foreach (var item in _infoBase.Catalogs)
            {
                if (item.Value.Name == catalogName)
                {
                    _catalog = item.Value;
                    break;
                }
            }

            if (_catalog == null)
            {
                throw new Exception($"Catalog \"{catalogName}\" is not found.");
            }

            BuildSqlScripts();

            return this;
        }
        
        public CatalogExporter ConfigureRabbitMQ(string hostName, string virtualHost, string userName, string password)
        {
            RmqHostName = hostName;
            RmqVirtualHost = virtualHost;
            RmqUserName = userName;
            RmqPassword = password;
            return this;
        }
        public CatalogExporter UseExchange(string exchange)
        {
            RmqExchange = exchange;
            return this;
        }
        public CatalogExporter UseBinding(string binding)
        {
            RmqBinding = binding;
            return this;
        }

        private string GetPropertyType(MetadataProperty property)
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
        private string GetPropertyName(MetadataProperty property)
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

        private void BuildSqlScripts()
        {
            string sourceTable = _catalog.TableName;
            string targetTable = $"Catalog_{_catalog.Name}";

            Build_SelectSource_Script(sourceTable);

            Build_DeleteTable_Script(targetTable);
            Build_CreateTable_Script(targetTable);
            Build_InsertTable_Script(targetTable);
            
            Build_SelectMaxRowNumber_Script(targetTable);
            Build_SelectBatch_Script(targetTable);
        }
        private void Build_SelectSource_Script(string sourceTable)
        {
            StringBuilder script = new StringBuilder();
            
            script.AppendLine("SELECT");
            
            for (int i = 0; i < _catalog.Properties.Count; i++)
            {
                MetadataProperty property = _catalog.Properties[i];

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

                    if (i != (_catalog.Properties.Count - 1))
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
            script.Append(sourceTable);
            script.Append(";");

            _sql_script_SelectSource = script.ToString();
        }
        private void Build_DeleteTable_Script(string targetTable)
        {
            StringBuilder script = new StringBuilder();

            script.AppendLine($"IF EXISTS(SELECT 1 FROM sys.tables WHERE name = '{targetTable}')");
            script.AppendLine("BEGIN");
            script.AppendLine($"\tDROP TABLE [{targetTable}];");
            script.AppendLine("END;");

            _sql_script_DeleteTable = script.ToString();
        }
        private void Build_CreateTable_Script(string targetTable)
        {
            StringBuilder script = new StringBuilder();

            script.AppendLine($"CREATE TABLE [{targetTable}]");
            script.AppendLine("(");
            script.AppendLine("\t[RowNumber] int IDENTITY(1,1) PRIMARY KEY,");

            for (int i = 0; i < _catalog.Properties.Count; i++)
            {
                MetadataProperty property = _catalog.Properties[i];

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

                    if (i != (_catalog.Properties.Count - 1))
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

            _sql_script_CreateTable = script.ToString();
        }
        private void Build_InsertTable_Script(string targetTable)
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine($"INSERT [{targetTable}]");
            script.AppendLine("(");

            for (int i = 0; i < _catalog.Properties.Count; i++)
            {
                MetadataProperty property = _catalog.Properties[i];

                string propertyType = GetPropertyType(property);
                string propertyName = GetPropertyName(property);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }

                for (int ii = 0; ii < property.Fields.Count; ii++)
                {
                    //DatabaseField field = property.Fields[ii];

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

                    if (i != (_catalog.Properties.Count - 1))
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

            script.AppendLine(_sql_script_SelectSource);

            script.Append(";");

            _sql_script_InsertTable = script.ToString();
        }
        private void Build_SelectMaxRowNumber_Script(string targetTable)
        {
            _sql_script_SelectMaxRowNumber = $"SELECT MAX(RowNumber) AS [MaxRowNumber] FROM [{targetTable}];";
        }
        private void Build_SelectBatch_Script(string targetTable)
        {
            StringBuilder script = new StringBuilder();

            script.AppendLine("SELECT");

            for (int i = 0; i < _catalog.Properties.Count; i++)
            {
                MetadataProperty property = _catalog.Properties[i];

                string propertyName = GetPropertyName(property);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }

                script.Append($"\t[{propertyName}]");

                if (i != (_catalog.Properties.Count - 1))
                {
                    script.Append(",");
                }
                script.AppendLine();
            }

            script.AppendLine("FROM");
            script.AppendLine($"\t[{targetTable}]");
            script.AppendLine("WHERE");
            script.Append("\t[RowNumber] BETWEEN @RowNumber1 AND @RowNumber2;");

            _sql_script_SelectBatch = script.ToString();
        }

        private void DeleteTargetTable()
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = _sql_script_DeleteTable;
                    command.CommandTimeout = 60; // seconds

                    int rowsAffected = command.ExecuteNonQuery();
                }
            }
        }
        private void CreateTargetTable()
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = _sql_script_CreateTable;
                    command.CommandTimeout = 60; // seconds

                    int rowsAffected = command.ExecuteNonQuery();
                }
            }
        }
        private void InsertTargetTable()
        {
            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = _sql_script_InsertTable;
                    command.CommandTimeout = 600; // seconds

                    int rowsAffected = command.ExecuteNonQuery();
                }
            }
        }
        private int GetMaxRowNumber()
        {
            int rowCount = 0;

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = _sql_script_SelectMaxRowNumber;
                    command.CommandTimeout = 60; // seconds

                    rowCount = (int)command.ExecuteScalar();
                }
            }

            return rowCount;
        }
        
        private int ExportData(JobInfo job, BatchInfo batch)
        {
            int messagesSent = 0;

            using (SqlConnection connection = new SqlConnection(_connectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = _sql_script_SelectBatch;
                    command.CommandTimeout = 180; // seconds
                    command.Parameters.AddWithValue("RowNumber1", batch.RowNumber1);
                    command.Parameters.AddWithValue("RowNumber2", batch.RowNumber2);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            SendData(reader, job);

                            messagesSent++;
                        }
                        reader.Close();
                    }
                }
            }

            return messagesSent;
        }
        private void SendData(SqlDataReader reader, JobInfo job)
        {
            job.Writer.Reset();
            job.Stream.Position = 0;

            MapDataToJson(reader, job.Writer);

            job.Writer.Flush();

            ReadOnlyMemory<byte> messageBytes = new ReadOnlyMemory<byte>(job.Stream.GetBuffer(), 0, (int)job.Writer.BytesCommitted);

            job.Properties.MessageId = Guid.NewGuid().ToString();

            job.Channel.BasicPublish(RmqExchange, RmqBinding, job.Properties, messageBytes);
        }

        private List<JobInfo> LIST_OF_JOBS;
        public int ExportCatalogToRabbitMQ(int batchSize)
        {
            int messagesSent;

            try
            {
                DeleteTargetTable();
                FileLogger.Log("Table deleted.");

                CreateTargetTable();
                FileLogger.Log("Table created.");

                FileLogger.Log("Copy table: start.");
                Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Copy table: start.");
                InsertTargetTable();
                FileLogger.Log("Copy table: done.");
                Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Copy table: done.");
            }
            catch (Exception error)
            {
                throw error;
            }

            try
            {
                messagesSent = ExportDataToRabbitMQ(batchSize);
            }
            catch (Exception error)
            {
                throw error;
            }

            try
            {
                DeleteTargetTable();
                FileLogger.Log("Table deleted.");
                Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] Table deleted.");
            }
            catch (Exception error)
            {
                throw error;
            }

            return messagesSent;
        }
        private int ExportDataToRabbitMQ(int batchSize)
        {
            int maxRowNumber = GetMaxRowNumber();
            if (maxRowNumber == 0) return 0;

            int maxThreads = Environment.ProcessorCount;

            InitializeRabbitMQ();

            LIST_OF_JOBS = new List<JobInfo>(maxThreads);
            ConfigureJobs(maxThreads);
            ConfigureBatches(maxRowNumber, batchSize);

            FileLogger.Log("Execute jobs start.");

            int messagesSent = ExecuteJobsInParallel();

            FileLogger.Log("Execute jobs stop.");

            DisposeRabbitMQ();

            LIST_OF_JOBS.Clear();
            LIST_OF_JOBS = null;

            return messagesSent;
        }
        private void ConfigureJobs(int maxThreads)
        {
            JsonWriterOptions options = new JsonWriterOptions
            {
                Indented = false,
                Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
            };

            for (int i = 0; i < maxThreads; i++)
            {
                JobInfo job = new JobInfo()
                {
                    Channel = CreateChannel()
                };
                job.Properties = CreateMessageProperties(job.Channel);

                job.Stream = _streamManager.GetStream();
                job.Writer = new Utf8JsonWriter(job.Stream, options);

                LIST_OF_JOBS.Add(job);
            }
        }
        private void ConfigureBatches(int maxRowNumber, int batchSize)
        {
            int firstRow = 1;
            int nextThread = 0;
            int maxThreads = LIST_OF_JOBS.Count;

            while (maxRowNumber >= firstRow)
            {
                BatchInfo batch = new BatchInfo()
                {
                    RowNumber1 = firstRow,
                    RowNumber2 = firstRow + batchSize - 1
                };

                if (batch.RowNumber2 > maxRowNumber)
                {
                    batch.RowNumber2 = maxRowNumber;
                }

                LIST_OF_JOBS[nextThread].Batches.Add(batch);

                nextThread++;
                if (nextThread == maxThreads)
                {
                    nextThread = 0;
                }

                firstRow += batchSize;
            }
        }

        private int ExecuteJobsInParallel()
        {
            int messagesSent = 0;

            using (var SendingCancellation = new CancellationTokenSource())
            {
                Task<int>[] tasks = new Task<int>[LIST_OF_JOBS.Count];

                for (int i = 0; i < LIST_OF_JOBS.Count; i++)
                {
                    tasks[i] = Task.Factory.StartNew(
                        ExecuteJobInBackground,
                        LIST_OF_JOBS[i],
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
        private int ExecuteJobInBackground(object jobInfo)
        {
            if (!(jobInfo is JobInfo job))
            {
                return 0;
            }

            int counter = 0;

            for (int i = 0; i < job.Batches.Count; i++)
            {
                try
                {
                    BatchInfo batch = job.Batches[i];

                    FileLogger.Log($"Batch {batch.RowNumber1} - {batch.RowNumber2}: export start.");

                    batch.MessagesSent = ExportData(job, batch);
                    if (batch.MessagesSent == 0)
                    {
                        continue;
                    }
                    counter += batch.MessagesSent;

                    if (batch.MessagesSent == 1)
                    {
                        WaitForConfirms(job.Channel);
                        continue;
                    }

                    // TODO: Handle the situation when messages are confirmed immediately.

                    // Wait for publisher confirm signal from BasicAcksHandler
                    bool confirmed = job.ConfirmEvent.WaitOne(TimeSpan.FromMinutes(5));

                    if (batch.IsNacked)
                    {
                        FileLogger.Log($"Batch {batch.RowNumber1} - {batch.RowNumber2}: is Nacked! Retry batch.");
                        ReportFailure?.Invoke(batch);
                        // TODO: Retry batch.
                    }
                    else
                    {
                        if (confirmed)
                        {
                            FileLogger.Log($"Batch {batch.RowNumber1} - {batch.RowNumber2}: export confirmed!");
                            ReportSuccess?.Invoke(batch);
                        }
                        else
                        {
                            FileLogger.Log($"Batch {batch.RowNumber1} - {batch.RowNumber2}: export is not confirmed!");
                            ReportFailure?.Invoke(batch);
                        }
                    }
                }
                catch (Exception error)
                {
                    FileLogger.Log(error.Message);
                    throw error;
                }
                finally
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }

            return counter;
        }



        private IConnection RmqConnection;
        private string RmqHostName;
        private string RmqVirtualHost;
        private string RmqUserName;
        private string RmqPassword;
        private string RmqExchange;
        private string RmqBinding;
        private void InitializeRabbitMQ()
        {
            IConnectionFactory factory = new ConnectionFactory()
            {
                HostName = RmqHostName,
                VirtualHost = RmqVirtualHost,
                UserName = RmqUserName,
                Password = RmqPassword,
                Port = 5672
            };
            RmqConnection = factory.CreateConnection();
        }
        private void DisposeRabbitMQ()
        {
            FileLogger.Log("Dispose start.");
            Stopwatch watch = new Stopwatch();
            watch.Start();

            foreach (JobInfo job in LIST_OF_JOBS)
            {
                int channelNumber = job.Channel.ChannelNumber;

                job.ConfirmEvent.Dispose();
                job.ConfirmEvent = null;

                job.Writer.Dispose();
                job.Writer = null;

                job.Stream.Dispose();
                job.Stream = null;
                
                FileLogger.Log($"Channel #{channelNumber}: next publish sequence number = {job.Channel.NextPublishSeqNo}");

                job.Channel.Dispose();
                job.Channel = null;

                FileLogger.Log($"Channel #{channelNumber}: disposed.");
            }

            FileLogger.Log($"Connection: disposing ...");
            RmqConnection.Dispose();
            RmqConnection = null;
            FileLogger.Log($"Connection: disposed.");

            watch.Stop();
            FileLogger.Log("Dispose stop.");
            FileLogger.Log($"Disposing elapsed: {watch.ElapsedMilliseconds} ms");
        }
        private IModel CreateChannel()
        {
            IModel channel = RmqConnection.CreateModel();
            channel.ConfirmSelect();
            channel.BasicAcks += BasicAcksHandler;
            channel.BasicNacks += BasicNacksHandler;
            return channel;
        }
        private IBasicProperties CreateMessageProperties(IModel channel)
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
        private void SetOperationTypeHeader(IBasicProperties properties)
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
        private void WaitForConfirms(IModel channel)
        {
            try
            {
                bool confirmed = channel.WaitForConfirms(TimeSpan.FromSeconds(10), out bool timedout);
                if (!confirmed)
                {
                    if (timedout)
                    {
                        throw new Exception("Publisher confirmation timeout");
                    }
                    else
                    {
                        throw new Exception("Publisher confirmation error");
                    }
                    //SendingCancellation.Cancel();
                }
            }
            catch (OperationInterruptedException rabbitError)
            {
                throw new Exception("WaitForConfirms: " + rabbitError.Message);

                //if (string.IsNullOrWhiteSpace(rabbitError.Message) || !rabbitError.Message.Contains("NOT_FOUND"))
                //{
                //    SendingCancellation.Cancel();
                //}
            }
            catch (Exception error)
            {
                throw new Exception("WaitForConfirms: " + error.Message);
            }
        }
                
        private void BasicAcksHandler(object sender, BasicAckEventArgs args)
        {
            if (!(sender is IModel channel)) return;

            foreach (JobInfo job in LIST_OF_JOBS)
            {
                if (job.Channel.ChannelNumber == channel.ChannelNumber)
                {
                    if (channel.NextPublishSeqNo == args.DeliveryTag + 1)
                    {
                        // Send signal to ExecuteJobInBackground procedure
                        job.ConfirmEvent.Set();
                    }
                    break;
                }
            }
        }
        private void BasicNacksHandler(object sender, BasicNackEventArgs args)
        {
            if (!(sender is IModel channel)) return;

            foreach (JobInfo job in LIST_OF_JOBS)
            {
                if (job.Channel.ChannelNumber == channel.ChannelNumber)
                {
                    int deliveryTag = (int)args.DeliveryTag;

                    FileLogger.Log($"Channel #{channel.ChannelNumber}: Nack delivery tag = {args.DeliveryTag}, multiple = {args.Multiple}");

                    int messagesSent = 0;

                    foreach (BatchInfo batch in job.Batches)
                    {
                        messagesSent += batch.MessagesSent;

                        if (deliveryTag <= messagesSent)
                        {
                            batch.IsNacked = true;
                            break;
                        }
                    }
                    break;
                }
            }
        }

        private static void MapDataToJson(SqlDataReader reader, Utf8JsonWriter writer)
        {
            writer.WriteStartObject();
            writer.WriteString("#type", "jcfg:CatalogObject.Партии");

            writer.WritePropertyName("#value");
            writer.WriteStartObject();

            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (reader.GetName(i) == "Owner")
                {
                    writer.WritePropertyName("Owner");
                    writer.WriteStartObject();
                    writer.WriteString("#type", "jcfg:CatalogRef.Номенклатура");
                    writer.WriteString("#value", reader.GetString(i));
                    writer.WriteEndObject();
                }
                else
                {
                    writer.WritePropertyName(reader.GetName(i));

                    Type type = reader.GetFieldType(i);

                    if (type == typeof(decimal))
                    {
                        writer.WriteNumberValue(reader.GetDecimal(i));
                    }
                    else if (type == typeof(bool))
                    {
                        writer.WriteBooleanValue(reader.GetBoolean(i));
                    }
                    else if (type == typeof(DateTime))
                    {
                        DateTime dateTime = reader.GetDateTime(i).AddYears(-2000);
                        writer.WriteStringValue(dateTime.ToString("yyyy-MM-ddTHH:mm:ss"));
                    }
                    else if (type == typeof(string))
                    {
                        writer.WriteStringValue(reader.GetString(i));
                    }
                    else
                    {
                        writer.WriteNullValue();
                    }
                }
            }
            writer.WriteEndObject();
            writer.WriteEndObject();
        }
    }
}