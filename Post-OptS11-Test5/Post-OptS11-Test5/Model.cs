using System;
using System.Collections.Generic;
using System.Text;

namespace Post_OptS11_Test5
{
    public class Data
    {
        public string rx_phi { get; set; }
        public string rx_pol_type { get; set; }
        public string tx_frequency_ghz { get; set; }
        public string rx_frequency_ghz { get; set; }
        public string rx_pol { get; set; }
        public string tx_theta { get; set; }
        public string settling_time { get; set; }
        public string rx_theta { get; set; }
        public string tx_phi { get; set; }
        public string tx_pol { get; set; }
        public string tx_pol_type { get; set; }

        public static explicit operator List<object>(Data v)
        {
            throw new NotImplementedException();
        }
    }

 
    public class Rows
    {
        public double AtFrequency { get; set; }
        public double FreqGHz { get; set; }
        public double GainDB { get; set; }
        public double CurrentA { get; set; }
        public double Humidity { get; set; }
        public string s2pfile { get; set; }
        public string status { get; set; }
    }

    public class RootObject
    {

        public string start_time { get; set; }
        public string band { get; set; }
        public string Production_Mode { get; set; }
        public string Operated_by { get; set; }
        public string asm_firmware_version { get; set; }
        public string asm_number { get; set; }
        public string Receiver_Used { get; set; }
        public string Receiver_Status { get; set; }
        public string Calibration_file { get; set; }
        public List<Rows> rows { get; set; }
        public List<Data> data { get; set; }
        public string Date { get; set; }
        public string FileName { get; set; }
    }
}
