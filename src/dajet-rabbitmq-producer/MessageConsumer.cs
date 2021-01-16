using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DaJet.RabbitMQ.Producer
{
    internal interface IMessageConsumer
    {
        void Configure(MessageConsumerSettings settings);
        List<DaJetMessage> ReceiveMessages(int count, out string errorMessage);
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
        public List<DaJetMessage> ReceiveMessages(int count, out string errorMessage)
        {
            errorMessage = string.Empty;
            List<DaJetMessage> messages = new List<DaJetMessage>(count);

            StringBuilder script = new StringBuilder();
            script.AppendLine("WITH [CTE] AS");
            script.AppendLine("(");
            script.AppendLine($"SELECT TOP({count})");
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
            {
                SqlDataReader reader = null;
                SqlTransaction transaction = null;
                SqlConnection connection = new SqlConnection(BuildConnectionString());
                SqlCommand command = connection.CreateCommand();
                command.CommandType = CommandType.Text;
                command.CommandText = script.ToString();
                try
                {
                    connection.Open();
                    
                    transaction = connection.BeginTransaction();
                    command.Transaction = transaction;

                    reader = command.ExecuteReader();
                    while (reader.Read())
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
                        messages.Add(message);
                    }

                    if (reader != null)
                    {
                        if (reader.HasRows)
                        {
                            command.Cancel();
                        }
                        reader.Dispose();
                        reader = null;
                    }

                    foreach (DaJetMessage message in messages)
                    {
                        MessageProducer.SendMessage(message.MessageBody);
                    }

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
                        if (reader.HasRows)
                        {
                            command.Cancel();
                        }
                        reader.Dispose();
                    }
                    if (command != null) command.Dispose();
                    if (connection != null) connection.Dispose();
                }
            }
            return messages;
        }
    }
}