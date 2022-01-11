using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using Excel = Microsoft.Office.Interop.Excel;
using System.Runtime.InteropServices;

namespace Import_PT_Data
{
    class Program
    {
        const string SPREADSHEET_RUNNER_URI = "http://testresults.kymeta.local/Automated/U8/PT_Spreadsheet_Runner";
        const string RUN_SUMMARY_FILENAME = "RunSummary.json";
        const string SETTINGS_FILENAME = "Settings.json";

        public static void XlsxToCsv(string sourceFilePath, string targetFilePath)
        {
            
                if (File.Exists(sourceFilePath) == false)
                {
                    return;
                }
                Excel.Application xlapp = new Excel.Application();
                Excel.Workbook xlworkbook = null;
                Excel.Worksheet xlsheet = null;
            //xlapp.Visible = true;
                try
                {
                    xlworkbook = xlapp.Workbooks.Open(sourceFilePath);
                    object misValue = System.Reflection.Missing.Value;
                    //xlworkbook = xlapp.ActiveWorkbook;
                    //Excel.Sheets wsSheet = xlworkbook.Worksheets;
                    //Excel.Worksheet CurSheet = (Excel.Worksheet)wsSheet[1];
                    xlsheet = (Excel.Worksheet)xlapp.ActiveSheet;
                
                    xlsheet.SaveAs(targetFilePath, Excel.XlFileFormat.xlCSVWindows);

                //cleanup
                GC.Collect();
                GC.WaitForPendingFinalizers();

                


                }
                catch (Exception obj)
                {
                    Console.WriteLine("bad file: " + sourceFilePath);
                }
                finally
                {
                    //cleanup
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                    if(xlsheet != null)
                        Marshal.ReleaseComObject(xlsheet);
                    if (xlworkbook != null)
                    {
                        xlworkbook.Close(true);
                        Marshal.ReleaseComObject(xlworkbook);
                    }
                    xlapp.Quit();
                    Marshal.ReleaseComObject(xlapp);
                }
        }

        public static bool DownloadFile(Uri uri, string localFilePath)
        {
            try
            {
                var webClient = new System.Net.WebClient();
                webClient.DownloadFile(uri, localFilePath);
                Console.WriteLine("Domnloaded from " + uri);
                return true;
            }
            catch(System.Net.WebException)
            {
                return false;
            }
        }

        public static JToken LoadSetting(string filename, string settingName)
        {
            string filePath = Path.GetFullPath(filename);
            if (File.Exists(filePath)) {
                using (StreamReader file = File.OpenText(filePath))
                {
                    using (JsonTextReader reader = new JsonTextReader(file))
                    {
                        JObject jobjSettings = (JObject)JToken.ReadFrom(reader);
                        reader.Close();
                        file.Close();
                        return jobjSettings.GetValue(settingName);

                    }
                    
                }
                
            }
            return null;
        }

        public static void SaveLastJobId(int jobId)
        {
            var jsonSettings = new JObject(
                new JProperty("JobId", jobId)
            );
            string filePath = Path.GetFullPath(SETTINGS_FILENAME);
            using (StreamWriter file = File.CreateText(filePath))
            using (JsonTextWriter writer = new JsonTextWriter(file))
            {
                jsonSettings.WriteTo(writer);
            }
        }

        static void Main(string[] args)
        {
            JToken tokJobId = LoadSetting(SETTINGS_FILENAME, "JobId");
            int jobId = tokJobId == null ? 140 : tokJobId.Value<int>();
            int successJobId = 0;
            int failCount = 0;

            while(true)
            {
                string baseUri = $"{SPREADSHEET_RUNNER_URI}/{jobId}";
                Uri jobSummaryUri = new Uri($"{baseUri}/{RUN_SUMMARY_FILENAME}");
                bool readSummarySuccess = DownloadFile(jobSummaryUri, RUN_SUMMARY_FILENAME);

                if (readSummarySuccess)
                {
                    JToken tokOutputFilename = LoadSetting(RUN_SUMMARY_FILENAME, "output_filename");

                    if (tokOutputFilename != null)
                    {
                        string studySpreadsheetFilename = "df_" + tokOutputFilename.ToString();

                        Uri studySpreadsheetUri = new Uri($"{baseUri}/{studySpreadsheetFilename}");
                        if (DownloadFile(studySpreadsheetUri, studySpreadsheetFilename))
                        {
                            XlsxToCsv(
                                 Path.GetFullPath(studySpreadsheetFilename),
                                "\\\\Kymeta.local\\Shares\\LabData\\staging\\PointAndTracking\\" + Path.GetFileName(Path.ChangeExtension(studySpreadsheetFilename, ".csv")));

                            Console.WriteLine("excel file: " + Path.GetFullPath(studySpreadsheetFilename));
                            Console.WriteLine("csv file: " + "\\\\Kymeta.local\\Shares\\LabData\\staging\\PointAndTracking\\" + Path.GetFileName(Path.ChangeExtension(studySpreadsheetFilename, ".csv")));

                        }
                    }
                    successJobId = jobId;
                    failCount = 0;
                }
                else
                {
                    failCount++;
                    if (failCount > 15)
                    {
                        break;
                    }
                }
                jobId++;
            }

            if (successJobId > 0)
            {
                SaveLastJobId(successJobId + 1);
            }
        }
    }
}