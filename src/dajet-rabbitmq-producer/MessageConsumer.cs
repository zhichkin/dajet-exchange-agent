using Microsoft.Data.SqlClient;
using System;
using System.Data;
using System.Text;

namespace DaJet.RabbitMQ.Producer
{
    internal interface IMessageConsumer
    {
        void Configure(MessageConsumerSettings settings);
        int ReceiveMessages(int count, out string errorMessage);
        int AwaitNotification(int timeout, out string errorMessage);
    }
    internal sealed class MessageConsumer : IMessageConsumer
    {
        private MessageConsumerSettings Settings { get; set; }
        private IMessageProducer MessageProducer { get; set; }
        public MessageConsumer(IMessageProducer messageProducer)
        {
            MessageProducer = messageProducer;
        }
        public void Configure(MessageConsumerSettings settings)
        {
            Settings = settings;
        }
        private string BuildConnectionString()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder()
            {
                DataSource = Settings.ServerName,
                InitialCatalog = Settings.DatabaseName,
                PersistSecurityInfo = false
            };
            if (string.IsNullOrWhiteSpace(Settings.UserName))
            {
                builder.IntegratedSecurity = true;
            }
            else
            {
                builder.UserID = Settings.UserName;
                builder.Password = Settings.Password;
            }
            return builder.ToString();
        }

        public int ReceiveMessages(int messageCount, out string errorMessage)
        {
            int messagesRecevied = 0;
            errorMessage = string.Empty;
            {
                SqlDataReader reader = null;
                SqlTransaction transaction = null;
                SqlConnection connection = new SqlConnection(BuildConnectionString());
                
                SqlCommand command = connection.CreateCommand();
                command.CommandType = CommandType.Text;
                command.CommandText = ReceiveMessagesScript(messageCount);
                command.CommandTimeout = 60; // 1 minute
                
                try
                {
                    connection.Open();
                    transaction = connection.BeginTransaction();
                    command.Transaction = transaction;

                    reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        DaJetMessage message = ProduceMessage(reader);
                        MessageProducer.SendMessage(message.MessageBody);
                    }
                    reader.Close();
                    messagesRecevied = reader.RecordsAffected;

                    transaction.Commit();
                }
                catch (Exception error)
                {
                    errorMessage = ExceptionHelper.GetErrorText(error);
                    try
                    {
                        transaction.Rollback();
                    }
                    catch (Exception rollbackError)
                    {
                        errorMessage += Environment.NewLine + rollbackError.Message;
                    }
                }
                finally
                {
                    if (reader != null)
                    {
                        if (!reader.IsClosed && reader.HasRows)
                        {
                            command.Cancel();
                        }
                        reader.Dispose();
                    }
                    if (command != null) command.Dispose();
                    if (connection != null) connection.Dispose();
                }
            }
            return messagesRecevied;
        }
        private DaJetMessage ProduceMessage(SqlDataReader reader)
        {
            DaJetMessage message = new DaJetMessage()
            {
                Code = reader.IsDBNull("_Code") ? 0 : (long)reader.GetDecimal("_Code"),
                Version = reader.IsDBNull("_Version") ? 0 : BitConverter.ToInt64((byte[])reader["_Version"]),
                MessageType = reader.IsDBNull("ТипСообщения") ? string.Empty : reader.GetString("ТипСообщения"),
                MessageBody = reader.IsDBNull("ТелоСообщения") ? string.Empty : reader.GetString("ТелоСообщения"),
                OperationType = reader.IsDBNull("ТипОперации") ? string.Empty : reader.GetString("ТипОперации"),
                OperationDate = reader.IsDBNull("ДатаОперации") ? DateTime.MinValue : reader.GetDateTime("ДатаОперации")
            };
            return message;
        }
        private string ReceiveMessagesScript(int messageCount)
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine("WITH [CTE] AS");
            script.AppendLine("(");
            script.AppendLine($"SELECT TOP({messageCount})");
            script.AppendLine("[_Code]    AS [_Code],");
            script.AppendLine("[_Version] AS [_Version],");
            script.AppendLine("[_Fld82]   AS [ДатаОперации],");
            script.AppendLine("[_Fld83]   AS [ТипОперации],");
            script.AppendLine("[_Fld84]   AS [ТипСообщения],");
            script.AppendLine("[_Fld85]   AS [ТелоСообщения]");
            script.AppendLine("FROM");
            script.AppendLine("[dbo].[_Reference81] WITH (ROWLOCK)");
            script.AppendLine("ORDER BY");
            script.AppendLine("[_Code] ASC, [_IDRRef] ASC");
            script.AppendLine(")");
            script.AppendLine("DELETE [CTE]");
            script.AppendLine("OUTPUT deleted.[_Code], deleted.[_Version], deleted.[ДатаОперации],");
            script.AppendLine("deleted.[ТипОперации], deleted.[ТипСообщения], deleted.[ТелоСообщения];");
            return script.ToString();
        }

        public int AwaitNotification(int timeout, out string errorMessage)
        {
            int resultCode = 0;
            errorMessage = string.Empty;
            {
                SqlDataReader reader = null;
                SqlTransaction transaction = null;
                SqlConnection connection = new SqlConnection(BuildConnectionString());

                SqlCommand command = connection.CreateCommand();
                command.CommandType = CommandType.Text;
                command.CommandText = AwaitNotificationScript(timeout);
                command.CommandTimeout = timeout / 1000 + 1;

                try
                {
                    connection.Open();
                    transaction = connection.BeginTransaction();
                    command.Transaction = transaction;

                    reader = command.ExecuteReader();
                    if (reader.Read())
                    {
                        resultCode = 0; // notification received successfully
                        byte[] message_body = new byte[16];
                        Guid dialog_handle = reader.GetGuid("dialog_handle");
                        string message_type = reader.GetString("message_type");
                        long readBytes = reader.GetBytes("message_body", 0, message_body, 0, 16);
                    }
                    else
                    {
                        resultCode = 2; // no notification received
                    }
                    reader.Close();

                    transaction.Commit();
                }
                catch (Exception error)
                {
                    resultCode = 1; // notifications are not supported by database
                    errorMessage = ExceptionHelper.GetErrorText(error);
                    try
                    {
                        transaction.Rollback();
                    }
                    catch (Exception rollbackError)
                    {
                        errorMessage += Environment.NewLine + rollbackError.Message;
                    }
                }
                finally
                {
                    if (reader != null)
                    {
                        if (!reader.IsClosed && reader.HasRows)
                        {
                            command.Cancel();
                        }
                        reader.Dispose();
                    }
                    if (command != null) command.Dispose();
                    if (connection != null) connection.Dispose();
                }
            }
            return resultCode;
        }
        private string AwaitNotificationScript(int timeout)
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine("WAITFOR (RECEIVE TOP (1)");
            script.AppendLine("conversation_handle AS [dialog_handle],");
            script.AppendLine("message_type_name   AS [message_type],");
            script.AppendLine("message_body        AS [message_body]");
            script.AppendLine($"FROM [dajet-exchange-export-queue]), TIMEOUT {timeout};");
            return script.ToString();
        }
    }
}