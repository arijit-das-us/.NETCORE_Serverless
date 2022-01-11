using System;
using System.Collections.Generic;
using System.Text;

namespace Test3DataImport
{
    public class Test3LiteData
    {
        public string Name { get; set; }
        public string Value { get; set; }
        public string PassFail { get; set; }
        public string Message { get; set; }



    }
    public class Test3Lite
    {
        public string OriginalFilename { get; set; }
        public string ASMSN { get; set; }
        public string ASMPN { get; set; }
        public string ControllerInfo { get; set; }
        public string SoftwareName { get; set; }
        public string SoftwareVersion { get; set; }
        public DateTime DTG { get; set; }
        public string FileLocation { get; set; }
        public List<Test3LiteData> data { get; set; }

    }

    public class Test3AdamData
    {
        public double RxCommandedFreq { get; set; }
        public double RxActualFreq { get; set; }
        public double RxActualCommandGain { get; set; }
        public double TxCommandedFreq { get; set; }
        public double TxActualFreq { get; set; }
        public double TxActualCommandGain { get; set; }
        public double RxMaxGainFreq { get; set; }
        public double RxMaxGain { get; set; }
        public double TxMaxGainFreq { get; set; }
        public double TxMaxGain { get; set; }
        public double Theta { get; set; }
        public double Lpa { get; set; }
        public double Phi { get; set; }
        public double Current { get; set; }
        public double Temp { get; set; }
        public double RH { get; set; }
        public string GatingNotGated { get; set; }
        public DateTime DateTime { get; set; }
        public string s2pFile { get; set; }
        public string RxJsonFileUsed { get; set; }
        public string TxJsonFileUsed { get; set; }

    }
    public class Test3Adam
    {
        public string OriginalFilename { get; set; }
        public string ASMSN { get; set; }
        public string ASMPN { get; set; }
        public string AntennaSN { get; set; }
        public string ControllerInfo { get; set; }
        public string SoftwareName { get; set; }
        public string SoftwareVersion { get; set; }
        public DateTime DTG { get; set; }
        public string FileLocation { get; set; }
        public List<Test3AdamData> data { get; set; }

    }
}
