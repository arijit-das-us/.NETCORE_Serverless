using System;
using System.Collections.Generic;
using System.Text;

namespace Test2DataImport
{
    public class Test2SummaryData
    {
        public string ClassName { get; set; }
        public string Name { get; set; }
        public double Max { get; set; }
        public string Value { get; set; }
        public double Min { get; set; }
        public double Time { get; set; }
        public string Status { get; set; }
        public string PN { get; set; }
        public string SN { get; set; }
        public string Messsage { get; set; }

    }
    public class Test2Summary
    {
        public string OriginalFilename { get; set; }
        //public string ASMSN { get; set; }
        public string AntennaSN { get; set; }
        public string AntennaPN { get; set; }
        public string TRCBSN { get; set; }
        public string KIRUSN { get; set; }
        public string FeedSN { get; set; }
        public DateTime DTG { get; set; }
        public string FileLocation { get; set; }
        public List<Test2SummaryData> data { get; set; }

    }
}
