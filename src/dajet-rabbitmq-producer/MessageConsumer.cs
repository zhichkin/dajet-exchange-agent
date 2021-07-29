﻿using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace DaJet.RabbitMQ.Producer
{
    internal interface IMessageConsumer
    {
        int ReceiveMessages(int count, out string errorMessage);
        int AwaitNotification(int timeout, out string errorMessage);
    }
    internal sealed class MessageConsumer : IMessageConsumer
    {
        private MessageConsumerSettings Settings { get; set; }
        private IMessageProducer MessageProducer { get; set; }
        private DaJetExchangeQueue QueueInfo { get; set; }
        public MessageConsumer(IMessageProducer messageProducer,
            IOptions<DaJetExchangeQueue> queueOptions,
            IOptions<MessageConsumerSettings> options)
        {
            Settings = options.Value;
            QueueInfo = queueOptions.Value;
            MessageProducer = messageProducer;
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

        private void RollbackTransaction(SqlTransaction transaction, SqlDataReader reader, out string errorMessage)
        {
            errorMessage = string.Empty;

            if (transaction == null) return;

            try
            {
                if (reader != null && !reader.IsClosed)
                {
                    reader.Close();
                }
                transaction.Rollback();
            }
            catch (Exception error)
            {
                errorMessage = ExceptionHelper.GetErrorText(error);
            }
        }
        private void DisposeDatabaseResources(SqlConnection connection, SqlDataReader reader, SqlCommand command)
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
                    List<DaJetMessage> batch = new List<DaJetMessage>();
                    while (reader.Read())
                    {
                        batch.Add(ProduceMessage(reader));
                    }
                    reader.Close();
                    messagesRecevied = reader.RecordsAffected;

                    if (batch.Count > 0)
                    {
                        MessageProducer.Publish(batch);
                    }

                    transaction.Commit();
                }
                catch (Exception error)
                {
                    errorMessage = ExceptionHelper.GetErrorText(error);

                    RollbackTransaction(transaction, reader, out string rollbackError);
                    
                    if (!string.IsNullOrEmpty(rollbackError))
                    {
                        errorMessage += Environment.NewLine + rollbackError;
                    }
                }
                finally
                {
                    DisposeDatabaseResources(connection, reader, command);
                }
            }
            return messagesRecevied;
        }
        private DaJetMessage ProduceMessage(SqlDataReader reader)
        {
            DaJetMessage message = new DaJetMessage()
            {
                UTC = reader.IsDBNull("МоментВремени") ? 0 : (long)reader.GetDecimal("МоментВремени"),
                Uuid = reader.IsDBNull("Идентификатор") ? Guid.Empty : reader.GetGuid("Идентификатор"),
                MessageType = reader.IsDBNull("ТипСообщения") ? string.Empty : reader.GetString("ТипСообщения"),
                MessageBody = reader.IsDBNull("ТелоСообщения") ? string.Empty : reader.GetString("ТелоСообщения"),
                OperationType = reader.IsDBNull("ТипОперации") ? string.Empty : reader.GetString("ТипОперации")
            };
            return message;
        }
        private string ReceiveMessagesScript(int messageCount)
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine("WITH [CTE] AS");
            script.AppendLine("(");
            script.AppendLine($"SELECT TOP({messageCount})");
            script.AppendLine($"[{QueueInfo.Properties.Where(p => p.Name == "МоментВремени").FirstOrDefault()?.Field}] AS [МоментВремени],");
            script.AppendLine($"CAST([{QueueInfo.Properties.Where(p => p.Name == "Идентификатор").FirstOrDefault()?.Field}] AS uniqueidentifier) AS [Идентификатор],");
            script.AppendLine($"[{QueueInfo.Properties.Where(p => p.Name == "ТипОперации").FirstOrDefault()?.Field}] AS [ТипОперации],");
            script.AppendLine($"[{QueueInfo.Properties.Where(p => p.Name == "ТипСообщения").FirstOrDefault()?.Field}] AS [ТипСообщения],");
            script.AppendLine($"[{QueueInfo.Properties.Where(p => p.Name == "ТелоСообщения").FirstOrDefault()?.Field}] AS [ТелоСообщения]");
            script.AppendLine("FROM");
            script.AppendLine($"[dbo].[{QueueInfo.TableName}] WITH (ROWLOCK, READPAST)");
            script.AppendLine("ORDER BY");
            script.AppendLine($"[{QueueInfo.Properties.Where(p => p.Name == "МоментВремени").FirstOrDefault()?.Field}] ASC,");
            script.AppendLine($"[{QueueInfo.Properties.Where(p => p.Name == "Идентификатор").FirstOrDefault()?.Field}] ASC");
            script.AppendLine(")");
            script.AppendLine("DELETE [CTE]");
            script.AppendLine("OUTPUT deleted.[МоментВремени], deleted.[Идентификатор],");
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
                command.CommandTimeout = timeout / 1000 + 10;

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
                    
                    RollbackTransaction(transaction, reader, out string rollbackError);

                    if (!string.IsNullOrEmpty(rollbackError))
                    {
                        errorMessage += Environment.NewLine + rollbackError;
                    }
                }
                finally
                {
                    DisposeDatabaseResources(connection, reader, command);
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