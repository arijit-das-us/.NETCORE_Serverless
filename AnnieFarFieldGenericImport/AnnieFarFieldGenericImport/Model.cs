using System;
using System.Collections.Generic;
using System.Text;

namespace AnnieFarFieldGenericImport
{
    public class AnnieFarFieldSummaryData
    {
        public double Tertiary { get; set; }
        public double Secondary { get; set; }
        public double Primary { get; set; }
        public double Frequency { get; set; }
        public double Mag_dB { get; set; }
        public double Phase_deg { get; set; }


    }

    public class ScanRow
    {
        public string aux_axis { get; set; }
        public string aux_position { get; set; }
        public string pri_axis { get; set; }
        public Double pri_start_1 { get; set; }
        public string pri_start_2 { get; set; }
        public string pri_start_3 { get; set; }
        public string pri_start_4 { get; set; }
        public string pri_start_5 { get; set; }
        public string pri_start_6 { get; set; }
        public Double pri_step_1 { get; set; }
        public string pri_step_2 { get; set; }
        public string pri_step_3 { get; set; }
        public string pri_step_4 { get; set; }
        public string pri_step_5 { get; set; }
        public string pri_step_6 { get; set; }
        public Double pri_stop_1 { get; set; }
        public string pri_stop_2 { get; set; }
        public string pri_stop_3 { get; set; }
        public string pri_stop_4 { get; set; }
        public string pri_stop_5 { get; set; }
        public string pri_stop_6 { get; set; }
        public Double rx_frequency_ghz { get; set; }
        public string rx_json { get; set; }
        public Double rx_phi { get; set; }
        public Double rx_lpa { get; set; }
        public string rx_pol_type { get; set; }
        public Double rx_theta { get; set; }
        public string sec_axis { get; set; }
        public Double sec_start_1 { get; set; }
        public string sec_start_2 { get; set; }
        public string sec_start_3 { get; set; }
        public string sec_start_4 { get; set; }
        public string sec_start_5 { get; set; }
        public string sec_start_6 { get; set; }
        public Double sec_step_1 { get; set; }
        public string sec_step_2 { get; set; }
        public string sec_step_3 { get; set; }
        public string sec_step_4 { get; set; }
        public string sec_step_5 { get; set; }
        public string sec_step_6 { get; set; }
        public Double sec_stop_1 { get; set; }
        public string sec_stop_2 { get; set; }
        public string sec_stop_3 { get; set; }
        public string sec_stop_4 { get; set; }
        public string sec_stop_5 { get; set; }
        public string sec_stop_6 { get; set; }
        public string ter_axis { get; set; }
        public Double ter_start_1 { get; set; }
        public string ter_start_2 { get; set; }
        public string ter_start_3 { get; set; }
        public string ter_start_4 { get; set; }
        public string ter_start_5 { get; set; }
        public string ter_start_6 { get; set; }
        public Double ter_step_1 { get; set; }
        public string ter_step_2 { get; set; }
        public string ter_step_3 { get; set; }
        public string ter_step_4 { get; set; }
        public string ter_step_5 { get; set; }
        public string ter_step_6 { get; set; }
        public Double ter_stop_1 { get; set; }
        public string ter_stop_2 { get; set; }
        public string ter_stop_3 { get; set; }
        public string ter_stop_4 { get; set; }
        public string ter_stop_6 { get; set; }
        public string ter_stop_5 { get; set; }
    }

    public class OrbitSwVersion
    {
        public string build_state { get; set; }
        public string module_version { get; set; }
        public string build_number { get; set; }
        public string server_version { get; set; }
        public string product_version { get; set; }
    }
    public class AnnieFarFieldSummary
    {
        public string jira { get; set; }
        public string asm_fw_version { get; set; }
        public string test_desg { get; set; }
        public string start_time { get; set; }
        public string nfs_sw_version { get; set; }
        public ScanRow scan_row { get; set; }
        public string asm_serial { get; set; }
        public string stop_time { get; set; }
        public OrbitSwVersion orbit_sw_info { get; set; }
        public string test_chamber { get; set; }
        public string OriginalFilename { get; set; }
        public string ASMSN { get; set; }
        public string Frequency { get; set; }
        public DateTime TestDateTime { get; set; }
        public string FileLocation { get; set; }
        public List<AnnieFarFieldSummaryData> data { get; set; }

    }
}
