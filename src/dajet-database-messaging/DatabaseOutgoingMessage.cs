using System;
using System.ComponentModel.DataAnnotations.Schema;

namespace DaJet.Database.Messaging
{
    /// <summary>
    /// Табличный интерфейс исходящей очереди сообщений
    /// (непериодический независимый регистр сведений)
    /// </summary>
    public sealed class DatabaseOutgoingMessage
    {
        /// <summary>
        /// "НомерСообщения" Порядковый номер сообщения (генерируется средствами СУБД) - numeric(19,0)
        /// </summary>
        [Column("НомерСообщения")] [NotMapped] public long MessageNumber { get; set; } = 0L;
        /// <summary>
        /// "Отправитель" Код или UUID отправителя сообщения - nvarchar(36)
        /// </summary>
        [Column("Отправитель")] public string Sender { get; set; } = string.Empty;
        /// <summary>
        /// "Получатели" Коды или UUID получателей сообщения в формате CSV - nvarchar(max)
        /// </summary>
        [Column("Получатели")] public string Recipients { get; set; } = string.Empty;
        /// <summary>
        /// "ТипОперации" Тип операции: INSERT, UPDATE или DELETE - nvarchar(6)
        /// </summary>
        [Column("ТипОперации")] public string OperationType { get; set; } = string.Empty;
        /// <summary>
        /// "ТипСообщения" Тип сообщения, например, "Справочник.Номенклатура" - nvarchar(1024)
        /// </summary>
        [Column("ТипСообщения")] public string MessageType { get; set; } = string.Empty;
        /// <summary>
        /// "ТелоСообщения" Тело сообщения в формате JSON или XML - nvarchar(max)
        /// </summary>
        [Column("ТелоСообщения")] public string MessageBody { get; set; } = string.Empty;
        /// <summary>
        /// "ДатаВремя" Время создания сообщения - datetime2
        /// </summary>
        [Column("ДатаВремя")] public DateTime DateTimeStamp { get; set; } = DateTime.Now;
    }
}