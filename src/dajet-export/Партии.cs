using System;

namespace DaJet.Export
{
    namespace Справочник
    {
        public sealed class Партии
        {
            public Guid Ref { get; set; }
            public bool DeletionMark { get; set; }
            public Guid Owner { get; set; }
            public string Code { get; set; }
            public string Description { get; set; }
            public string Выдан { get; set; }
            public DateTime ГоденДо { get; set; }
            public Guid Документ { get; set; }
            public decimal ЗакупочнаяЦена { get; set; }
            public Guid Клиент { get; set; }
            public bool Комиссия { get; set; }
            public Guid НомерГТД { get; set; }
            public bool Подарок { get; set; }
            public Guid Производитель { get; set; }
            public decimal РеестроваяЦена { get; set; }
            public decimal СебестоимостьБезНДС { get; set; }
            public string Серия { get; set; }
            public string Сертификат { get; set; }
            public DateTime СертификатДо { get; set; }
            public bool Спецпризнак { get; set; }
            public Guid СтавкаНДС { get; set; }
            public decimal ТипДоступности { get; set; }
            public Guid Фирма { get; set; }
            public decimal ЦенаПроизводителя { get; set; }
            public string ШтрихКод { get; set; }
            public Guid Страна { get; set; }
            public DateTime ДатаСоздания { get; set; }
            public bool МДЛП { get; set; }
            public DateTime ДатаРеализацииПроизводителем { get; set; }
            public string ID_77 { get; set; }
        }
    }
}