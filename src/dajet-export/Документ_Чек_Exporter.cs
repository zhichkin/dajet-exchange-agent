using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;

namespace DaJet.Export
{
    public sealed class Документ_Чек_Exporter
    {
        private const string DOCUMENT_NAME = "Чеки";
        private IMetadataService Metadata { get; }
        private ApplicationObject Document { get; set; }
        public Документ_Чек_Exporter(IMetadataService metadata)
        {
            Metadata = metadata;
        }
        private void InitializeMetaDocument()
        {
            if (Document != null) return;

            Console.WriteLine($"Opening metadata: {Metadata.ConnectionString} ...");
            Stopwatch watch = new Stopwatch();
            watch.Start();
            InfoBase infoBase = Metadata.OpenInfoBase();
            watch.Stop();
            Console.WriteLine($"Metadata is opened successfully in {watch.ElapsedMilliseconds} ms");

            Document = infoBase.Documents.Values.Where(c => c.Name == DOCUMENT_NAME).FirstOrDefault();

            if (Document == null)
            {
                Console.WriteLine($"Документ \"{DOCUMENT_NAME}\" не найден.");
            }
            else
            {
                Console.WriteLine($"Документ \"{DOCUMENT_NAME}\" найден.");
            }
        }
        private string GetMARSConnectionString()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(Metadata.ConnectionString)
            {
                MultipleActiveResultSets = true
            };
            return builder.ToString();
        }
        public void ExportDocuments()
        {
            InitializeMetaDocument();

            if (Document == null) return;

            List<DocumentBatchInfo> batches = GetBatchesInfo();
            if (batches.Count == 0)
            {
                Console.WriteLine($"Документы вида \"{Document.Name}\" для экспорта не найдены.");
                return;
            }

            foreach (DocumentBatchInfo batch in batches)
            {
                ExportBatch(batch);
            }
        }
        private void ExportBatch(DocumentBatchInfo batch)
        {
            TablePart table = Document.TableParts.Where(t => t.Name == "Реализация").FirstOrDefault();
            if (table == null)
            {
                Console.WriteLine("Табличная часть \"Реализация\" не найдена.");
                return;
            }

            using (SqlConnection connection = new SqlConnection(GetMARSConnectionString()))
            {
                connection.Open();

                using (SqlCommand command1 = connection.CreateCommand())
                {
                    command1.CommandType = CommandType.Text;
                    command1.CommandText = SelectDocuments_Script();
                    command1.CommandTimeout = 60; // seconds

                    command1.Parameters.AddWithValue("Period1", new DateTime(4021, 9, 3, 0, 0, 0));
                    command1.Parameters.AddWithValue("Period2", new DateTime(4021, 9, 4, 0, 0, 0));

                    using (SqlCommand command2 = connection.CreateCommand())
                    {
                        command2.CommandType = CommandType.Text;
                        command2.CommandText = SelectDocumentTablePart_Script(table);
                        command2.CommandTimeout = 60; // seconds

                        command2.Parameters.Add("Ref", SqlDbType.Binary, 16);

                        using (SqlDataReader reader1 = command1.ExecuteReader())
                        {
                            while (reader1.Read())
                            {
                                byte[] doc = (byte[])reader1["Ref"];
                                command2.Parameters["Ref"].Value = doc;

                                Console.WriteLine("Document = " + (new Guid(doc)).ToString());

                                using (SqlDataReader reader2 = command2.ExecuteReader())
                                {
                                    while (reader2.Read())
                                    {
                                        for (int f = 0; f < reader2.FieldCount; f++)
                                        {
                                            Console.WriteLine(reader2.GetName(f) + " = " + reader2.GetValue(f).ToString());
                                        }
                                    }
                                    reader2.Close();
                                }
                            }
                            reader1.Close();
                        }
                    }
                }
            }
        }

        private string SelectPeriods_Script()
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine("SELECT");
            script.AppendLine("\tYEAR(_Date_Time)  AS [Year],");
            script.AppendLine("\tMONTH(_Date_Time) AS [Month],");
            script.AppendLine("\tDAY(_Date_Time)   AS [Day],");
            script.AppendLine("\tCOUNT(*)          AS [Total]");
            script.AppendLine($"FROM {Document.TableName}");
            script.AppendLine("WHERE _Marked = 0x00");
            script.AppendLine("GROUP BY YEAR(_Date_Time), MONTH(_Date_Time), DAY(_Date_Time)");
            script.AppendLine("HAVING COUNT(*) > 0");
            script.Append("ORDER BY YEAR(_Date_Time) ASC, MONTH(_Date_Time) ASC, DAY(_Date_Time) ASC");
            script.Append(";");
            return script.ToString();
        }
        private string SelectDocuments_Script()
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine("SELECT TOP 3");
            script.AppendLine("\t_IDRRef AS [Ref]");
            script.AppendLine($"FROM {Document.TableName}");
            script.AppendLine("WHERE _Marked = 0x00");
            script.AppendLine("AND _Date_Time >= @Period1 AND _Date_Time < @Period2");
            script.Append(";");
            return script.ToString();
        }
        private string SelectDocumentTablePart_Script(TablePart table)
        {
            StringBuilder script = new StringBuilder();
            script.AppendLine("SELECT");
            script.AppendLine("\tT1.*");
            script.AppendLine($"FROM {Document.TableName} AS [T0]");
            script.AppendLine($"INNER JOIN {table.TableName} AS [T1]");
            script.AppendLine($"ON T0._IDRRef = T1.{Document.TableName}_IDRRef");
            script.AppendLine("WHERE T0._IDRRef = @Ref");
            script.AppendLine($"ORDER BY T1.{Document.TableName}_IDRRef ASC, T1._KeyField ASC");
            script.Append(";");
            return script.ToString();
        }

        private List<DocumentBatchInfo> GetBatchesInfo()
        {
            List<DocumentBatchInfo> list = new List<DocumentBatchInfo>();

            using (SqlConnection connection = new SqlConnection(Metadata.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = SelectPeriods_Script();
                    command.CommandTimeout = 60; // seconds

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            DocumentBatchInfo item = new DocumentBatchInfo()
                            {
                                Period1 = new DateTime(
                                    reader.GetInt32(0),
                                    reader.GetInt32(1),
                                    reader.GetInt32(2),
                                    0, 0, 0),
                                TotalCount = reader.GetInt32(3)
                            };
                            item.Period2 = item.Period1.AddDays(1);
                            list.Add(item);
                        }
                        reader.Close();
                    }
                }
            }

            return list;
        }
    }
}