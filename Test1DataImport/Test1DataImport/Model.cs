using System;
using System.Collections.Generic;
using System.Text;

namespace Test1DataImport
{
    public class Test1SummaryData
    {
        public string SerialNumber { get; set; }
        public string Type { get; set; }
        public double RxAvgdB { get; set; }
        public double TxAvgdB { get; set; }
        public double RxFreqPeakGHz { get; set; }
        public double TxFreqPeakGHz { get; set; }
        public double RxPeakdB { get; set; }
        public double TxPeakdB { get; set; }
        public string FileName { get; set; }


    }
    public class Test1Summary
    {
        public string OriginalFilename { get; set; }
        //public string ASMSN { get; set; }
        public string FeedSN { get; set; }
        public DateTime DTG { get; set; }
        public string FileLocation { get; set; }
        public List<Test1SummaryData> data { get; set; }

    }

    public class Test1RawData
    {
        public double Frequency { get; set; }
        public double RealS11 { get; set; }
        public double ImaginaryS11 { get; set; }
        //public double Magnitude { get; set; }
        //public double Linear { get; set; }
        //public double Phase { get; set; }
    }
    public class Test1Raw
    {
        public string OriginalFilename { get; set; }
        //public string ASMSN { get; set; }
        public string FeedSN { get; set; }
        public DateTime DTG { get; set; }
        public string FileLocation { get; set; }
        public List<Test1RawData> data { get; set; }

    }
}
