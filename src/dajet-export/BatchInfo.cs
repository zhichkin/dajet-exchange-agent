namespace DaJet.Export
{
    public sealed class BatchInfo
    {
        public int RowNumber1 { get; set; }
        public int RowNumber2 { get; set; }
        public bool IsNacked { get; set; }
        public int MessagesSent { get; set; }
    }
}