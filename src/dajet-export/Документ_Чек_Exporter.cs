using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using Microsoft.IO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;

namespace DaJet.Export
{
    public sealed class Документ_Чек_Exporter
    {
        private const string DOCUMENT_NAME = "Чеки";

        private RecyclableMemoryStreamManager StreamManager = new RecyclableMemoryStreamManager();

        private IMetadataService Metadata { get; }
        private InfoBase InfoBase { get; set; }
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
            InfoBase = Metadata.OpenInfoBase();
            watch.Stop();
            Console.WriteLine($"Metadata is opened successfully in {watch.ElapsedMilliseconds} ms");

            Document = InfoBase.Documents.Values.Where(c => c.Name == DOCUMENT_NAME).FirstOrDefault();

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

            DataMapper mapper = new DataMapper(InfoBase, Document);

            List<DocumentBatchInfo> batches = GetBatchesInfo();
            if (batches.Count == 0)
            {
                Console.WriteLine($"Документы вида \"{Document.Name}\" для экспорта не найдены.");
                return;
            }

            JsonWriterOptions options = new JsonWriterOptions()
            {
                Indented = false,
                Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
            };

            using (MemoryStream stream = StreamManager.GetStream())
            {
                using (Utf8JsonWriter writer = new Utf8JsonWriter(stream, options))
                {
                    foreach (DocumentBatchInfo batch in batches)
                    {
                        ExportBatch(batch, mapper, stream, writer);
                    }
                }
            }
        }
        private void ExportBatch(DocumentBatchInfo batch, DataMapper mapper, MemoryStream stream, Utf8JsonWriter writer)
        {
            TablePart table = Document.TableParts.Where(t => t.Name == "Реализация").FirstOrDefault();
            if (table == null)
            {
                Console.WriteLine("Табличная часть \"Реализация\" не найдена.");
                return;
            }

            // InfoBase.ReferenceTypeCodes.TryGetValue()

            using (SqlConnection connection = new SqlConnection(GetMARSConnectionString()))
            {
                connection.Open();

                using (SqlCommand command1 = connection.CreateCommand())
                {
                    command1.CommandType = CommandType.Text;
                    //command1.CommandText = SelectDocuments_Script();
                    command1.CommandTimeout = 60; // seconds
                    mapper.ConfigureSelectCommand(command1);

                    //command1.Parameters.AddWithValue("Period1", batch.Period1);
                    //command1.Parameters.AddWithValue("Period2", batch.Period2);

                    using (SqlCommand command2 = connection.CreateCommand())
                    {
                        command2.CommandType = CommandType.Text;
                        command2.CommandText = SelectDocumentTablePart_Script(table);
                        command2.CommandTimeout = 600; // seconds

                        command2.Parameters.Add("Ref", SqlDbType.Binary, 16);

                        using (SqlDataReader reader1 = command1.ExecuteReader())
                        {
                            while (reader1.Read())
                            {
                                mapper.MapDataToJson(reader1, writer);
                                writer.Flush();
                                
                                ReadOnlySpan<byte> span = new ReadOnlySpan<byte>(stream.GetBuffer(), 0, (int)writer.BytesCommitted);

                                writer.Reset();
                                stream.Position = 0;

                                Console.WriteLine(Encoding.UTF8.GetString(span));

                                //command2.Parameters["Ref"].Value = reader1["Ref"];

                                ////Console.WriteLine("Document = " + (new Guid(doc)).ToString());

                                //using (SqlDataReader reader2 = command2.ExecuteReader())
                                //{
                                //    while (reader2.Read())
                                //    {
                                //        for (int f = 0; f < reader2.FieldCount; f++)
                                //        {
                                //            Console.WriteLine(reader2.GetName(f) + " = " + reader2.GetValue(f).ToString());
                                //        }
                                //    }
                                //    reader2.Close();
                                //}
                            }
                            reader1.Close();
                        }
                    }
                }
            }
        }



        private string GetPropertyName(MetadataProperty property)
        {
            if (property.Name == "Ссылка") return "Ref";
            else if (property.Name == "ВерсияДанных") return string.Empty;
            else if (property.Name == "ПометкаУдаления") return "DeletionMark";
            else if (property.Name == "Дата") return "Date";
            else if (property.Name == "Номер") return "Number";
            else if (property.Name == "Проведен") return "Posted";
            return property.Name;
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
            script.AppendLine("SELECT TOP 1");

            foreach (MetadataProperty property in Document.Properties)
            {
                string propertyName = GetPropertyName(property);

                foreach (DatabaseField field in property.Fields)
                {
                    script.Append($"\t{field.Name} AS [{propertyName}]");
                    //if(field.Purpose == FieldPurpose.)
                }
            }

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

        private void Serialize_Документ_Чеки()
        {
            JsonWriterOptions options = new JsonWriterOptions
            {
                Indented = false,
                Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
            };

            MemoryStream stream = new MemoryStream(2048);
            Utf8JsonWriter writer = new Utf8JsonWriter(stream, options);

            writer.WriteStartObject();
            writer.WriteString("#type", "jcfg:CatalogObject.Партии");

            writer.WritePropertyName("#value");
            writer.WriteStartObject();

            writer.Flush();

            ReadOnlySpan<byte> span = new ReadOnlySpan<byte>(stream.GetBuffer(), 0, (int)writer.BytesCommitted);

            writer.Reset();
            stream.Position = 0;
        }
    }
}