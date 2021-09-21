using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace DaJet.Export
{
    internal sealed class DataMapper
    {
        private const string CONST_TYPE = "#type";
        private const string CONST_VALUE = "#value";
        private const string CONST_TYPE_DOCUMENT = "jcfg:DocumentObject";
        private InfoBase InfoBase { get; }
        private ApplicationObject MetaObject { get; }

        private readonly List<PropertyMapper> PropertyMappers = new List<PropertyMapper>();

        private string SELECT_COMMAND_SCRIPT = string.Empty;

        internal DataMapper(InfoBase infoBase, ApplicationObject metaObject)
        {
            InfoBase = infoBase;
            MetaObject = metaObject;
            InitializeDataMapper();
        }

        private string GetPropertyName(MetadataProperty property)
        {
            if (property.Name == "Ссылка") return "Ref";
            else if (property.Name == "ВерсияДанных") return string.Empty;
            else if (property.Name == "ПометкаУдаления") return "DeletionMark";
            else if (property.Name == "Дата") return "Date";
            else if (property.Name == "Номер") return "Number";
            else if (property.Name == "Проведён") return "Posted";
            else if (property.Name == "ПериодНомера") return string.Empty;
            return property.Name;
        }

        internal void InitializeDataMapper()
        {
            int index = -1;
            for (int i = 0; i < MetaObject.Properties.Count; i++)
            {
                string propertyName = GetPropertyName(MetaObject.Properties[i]);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }

                PropertyMapper mapper = new PropertyMapper(InfoBase, MetaObject.Properties[i], propertyName);
                mapper.Initialize(ref index);
                PropertyMappers.Add(mapper);
            }
        }
        internal void ConfigureSelectCommand(SqlCommand command)
        {
            if (string.IsNullOrEmpty(SELECT_COMMAND_SCRIPT))
            {
                StringBuilder script = new StringBuilder();

                script.AppendLine("SELECT");

                for (int i = 0; i < PropertyMappers.Count; i++)
                {
                    PropertyMappers[i].BuildSelectCommand(script);
                }

                script.Remove(script.Length - 1, 1);

                script.AppendLine();
                script.AppendLine("FROM");
                script.AppendLine("\t" + MetaObject.TableName + ";");

                SELECT_COMMAND_SCRIPT = script.ToString();
            }

            command.CommandText = SELECT_COMMAND_SCRIPT;
        }
        internal void MapDataToJson(SqlDataReader reader, Utf8JsonWriter writer)
        {
            writer.WriteStartObject();
            writer.WriteString(CONST_TYPE, CONST_TYPE_DOCUMENT + "." + MetaObject.Name);
            writer.WritePropertyName(CONST_VALUE);
            
            writer.WriteStartObject();
            for (int i = 0; i < PropertyMappers.Count; i++)
            {
                PropertyMappers[i].MapDataToJson(reader, writer);
            }
            writer.WriteEndObject();

            writer.WriteEndObject();
        }
    }
    internal sealed class PropertyMapper
    {
        private const string CONST_TYPE = "#type";
        private const string CONST_VALUE = "#value";
        private const string CONST_TYPE_STRING = "jxs:string";
        private const string CONST_TYPE_DECIMAL = "jxs:decimal";
        private const string CONST_TYPE_BOOLEAN = "jxs:boolean";
        private const string CONST_TYPE_DATETIME = "jxs:dateTime";
        private const string CONST_TYPE_ENUM = "jcfg:EnumRef";
        private const string CONST_TYPE_CATALOG = "jcfg:CatalogRef";
        private const string CONST_TYPE_DOCUMENT = "jcfg:DocumentRef";

        private int ValueIndex = -1;
        private int NumberIndex = -1;
        private int StringIndex = -1;
        private int ObjectIndex = -1;
        private int BooleanIndex = -1;
        private int DateTimeIndex = -1;
        private int TypeCodeIndex = -1;
        private int DiscriminatorIndex = -1;

        private InfoBase InfoBase { get; }
        private MetadataProperty Property { get; }
        private string PropertyName { get; }
        private Enumeration Enumeration { get; set; }

        internal PropertyMapper(InfoBase infoBase, MetadataProperty property, string propertyName)
        {
            InfoBase = infoBase;
            Property = property;
            PropertyName = propertyName;
        }

        internal void Initialize(ref int index)
        {
            if (InfoBase.ReferenceTypeUuids.TryGetValue(Property.PropertyType.ReferenceTypeUuid, out ApplicationObject metaObject))
            {
                Enumeration = metaObject as Enumeration;
            }

            for (int i = 0; i < Property.Fields.Count; i++)
            {
                index++;

                FieldPurpose purpose = Property.Fields[i].Purpose;

                if (purpose == FieldPurpose.Value)
                {
                    ValueIndex = index;
                }
                else if (purpose == FieldPurpose.Discriminator)
                {
                    DiscriminatorIndex = index;
                }
                else if (purpose == FieldPurpose.TypeCode)
                {
                    TypeCodeIndex = index;
                }
                else if (purpose == FieldPurpose.String)
                {
                    StringIndex = index;
                }
                else if (purpose == FieldPurpose.Boolean)
                {
                    BooleanIndex = index;
                }
                else if (purpose == FieldPurpose.Object)
                {
                    ObjectIndex = index;
                }
                else if (purpose == FieldPurpose.Numeric)
                {
                    NumberIndex = index;
                }
                else if (purpose == FieldPurpose.DateTime)
                {
                    DateTimeIndex = index;
                }
                else
                {
                    // ingnore this kind of the field purpose !?
                }
            }
        }
        private string GetEnumValue(Guid uuid)
        {
            if (Enumeration == null) return string.Empty;

            for (int i = 0; i < Enumeration.Values.Count; i++)
            {
                if (Enumeration.Values[i].Uuid == uuid)
                {
                    return Enumeration.Values[i].Name;
                }
            }

            return string.Empty;
        }
        internal void BuildSelectCommand(StringBuilder script)
        {
            for (int i = 0; i < Property.Fields.Count; i++)
            {
                if (Property.Fields[i].Purpose == FieldPurpose.TypeCode ||
                    Property.Fields[i].Purpose == FieldPurpose.Discriminator)
                {
                    script.Append("CAST(");
                }

                script.Append(Property.Fields[i].Name);

                if (Property.Fields[i].Purpose == FieldPurpose.TypeCode ||
                    Property.Fields[i].Purpose == FieldPurpose.Discriminator)
                {
                    script.Append(" AS int)");
                }

                script.Append(",");
            }
        }
        internal void MapDataToJson(SqlDataReader reader, Utf8JsonWriter writer)
        {
            if (DiscriminatorIndex > -1)
            {
                MapMultipleValueToJson(reader, writer);
            }
            else if (TypeCodeIndex > -1)
            {
                MapObjectToJson(reader, writer);
            }
            else
            {
                MapSingleValueToJson(reader, writer);
            }
        }
        internal void MapSingleValueToJson(SqlDataReader reader, Utf8JsonWriter writer)
        {
            if (ValueIndex < 0 || Property.PropertyType.IsMultipleType) return;

            if (Property.PropertyType.IsUuid)
            {
                writer.WriteString(PropertyName, new Guid(Convert_1C_Uuid((byte[])reader.GetValue(ValueIndex))).ToString());
            }
            else if (Property.PropertyType.IsValueStorage)
            {
                writer.WriteString(PropertyName, Convert.ToBase64String((byte[])reader.GetValue(ValueIndex)));
            }
            else if (Property.PropertyType.CanBeString)
            {
                writer.WriteString(PropertyName, reader.GetString(ValueIndex));
            }
            else if (Property.PropertyType.CanBeBoolean)
            {
                writer.WriteBoolean(PropertyName, ((byte[])reader.GetValue(ValueIndex))[0] == 0 ? false : true);
            }
            else if (Property.PropertyType.CanBeNumeric)
            {
                writer.WriteNumber(PropertyName, reader.GetDecimal(ValueIndex));
            }
            else if (Property.PropertyType.CanBeDateTime)
            {
                DateTime dateTime = reader.GetDateTime(ValueIndex);
                if (dateTime.Year > 4000)
                {
                    dateTime = dateTime.AddYears(-2000);
                }
                writer.WriteString(PropertyName, dateTime.ToString("yyyy-MM-ddTHH:mm:ss"));
            }
            else if (Property.PropertyType.CanBeReference)
            {
                Guid uuid = new Guid(Convert_1C_Uuid((byte[])reader.GetValue(ValueIndex)));

                if (Enumeration != null)
                {
                    writer.WriteString(PropertyName, GetEnumValue(uuid));
                }
                else
                {
                    writer.WriteString(PropertyName, uuid.ToString());
                }
            }
            else
            {
                // undefined property type
            }
        }
        internal void MapMultipleValueToJson(SqlDataReader reader, Utf8JsonWriter writer)
        {
            int discriminator = reader.GetInt32(DiscriminatorIndex);

            if (discriminator == 1) // Неопределено
            {
                writer.WriteNull(PropertyName);
            }
            else if (discriminator == 2) // Булево
            {
                bool value = (((byte[])reader.GetValue(BooleanIndex))[0] == 0 ? false : true);

                writer.WritePropertyName(PropertyName);
                writer.WriteStartObject();
                writer.WriteString(CONST_TYPE, CONST_TYPE_BOOLEAN);
                writer.WriteBoolean(CONST_VALUE, value);
                writer.WriteEndObject();
            }
            else if (discriminator == 3) // Число
            {
                decimal numeric = reader.GetDecimal(NumberIndex);

                writer.WritePropertyName(PropertyName);
                writer.WriteStartObject();
                writer.WriteString(CONST_TYPE, CONST_TYPE_DECIMAL);
                writer.WriteNumber(CONST_VALUE, numeric);
                writer.WriteEndObject();
            }
            else if (discriminator == 4) // Дата
            {
                DateTime dateTime = reader.GetDateTime(DateTimeIndex);
                if (dateTime.Year > 4000)
                {
                    dateTime = dateTime.AddYears(-2000);
                }

                writer.WritePropertyName(PropertyName);
                writer.WriteStartObject();
                writer.WriteString(CONST_TYPE, CONST_TYPE_DATETIME);
                writer.WriteString(CONST_VALUE, dateTime.ToString("yyyy-MM-ddTHH:mm:ss"));
                writer.WriteEndObject();
            }
            else if (discriminator == 5) // Строка
            {
                writer.WritePropertyName(PropertyName);
                writer.WriteStartObject();
                writer.WriteString(CONST_TYPE, CONST_TYPE_STRING);
                writer.WriteString(CONST_VALUE, reader.GetString(StringIndex));
                writer.WriteEndObject();
            }
            else if (discriminator == 8) // Ссылка
            {
                MapObjectToJson(reader, writer);
            }
            else
            {
                // unknown discriminator
            }
        }
        internal void MapObjectToJson(SqlDataReader reader, Utf8JsonWriter writer)
        {
            string typeName = string.Empty;

            int typeCode = reader.GetInt32(TypeCodeIndex);
            
            bool isEnum = false;

            if (InfoBase.ReferenceTypeCodes.TryGetValue(typeCode, out ApplicationObject metaObject))
            {
                if (metaObject is Enumeration enumeration)
                {
                    isEnum = true;
                    typeName = CONST_TYPE_ENUM + "." + enumeration.Name;
                }
                else if (metaObject is Catalog catalog)
                {
                    typeName = CONST_TYPE_CATALOG + "." + catalog.Name;
                }
                else if (metaObject is Document document)
                {
                    typeName = CONST_TYPE_DOCUMENT + "." + document.Name;
                }
                else
                {
                    // unsupported application object
                }
            }
            else
            {
                // unknown type code
            }

            Guid uuid = new Guid(Convert_1C_Uuid((byte[])reader.GetValue(ObjectIndex)));

            writer.WritePropertyName(PropertyName);
            writer.WriteStartObject();
            writer.WriteString(CONST_TYPE, typeName);
            if (isEnum)
            {
                writer.WriteString(CONST_VALUE, GetEnumValue(uuid));
            }
            else
            {
                writer.WriteString(CONST_VALUE, uuid.ToString());
            }
            writer.WriteEndObject();
        }

        private byte[] Convert_1C_Uuid(byte[] uuid_1c)
        {
            // CAST(REVERSE(SUBSTRING(@uuid_sql, 9, 8)) AS binary(8)) + SUBSTRING(@uuid_sql, 1, 8)
            
            byte[] uuid = new byte[16];

            for (int i = 0; i < 8; i++)
            {
                uuid[i] = uuid_1c[15 - i];
                uuid[8 + i] = uuid_1c[i];
            }

            return uuid;
        }
    }
}