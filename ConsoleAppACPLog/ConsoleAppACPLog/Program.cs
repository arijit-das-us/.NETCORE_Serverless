using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Runtime.InteropServices;

namespace ConsoleAppACPLog
{
    class Program
    {const string ACP_LOG_URI = "http://testresults.kymeta.local/Automated/U8/PT_15_Pointometer";
        const string SETTINGS_FILENAME = "Settings.json";
        const string RUN_SUMMARY_FILENAME = "RunSummary.json";

        public static bool DownloadFile(Uri uri, string localFilePath, string destinationPath)
        {
            try
            {
                var webClient = new System.Net.WebClient();
                webClient.DownloadFile(uri, localFilePath);
                Console.WriteLine("Domnloaded from " + uri);
                if(destinationPath != "")
                    File.Move(localFilePath, destinationPath);
                return true;
            }
            catch (System.Net.WebException e)
            {
                Console.WriteLine("Exception " + e.Message);
                return false;
            }
        }

        public static JToken LoadSetting(string filename, string settingName)
        {
            string filePath = Path.GetFullPath(filename);
            if (File.Exists(filePath))
            {
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
            Console.WriteLine("Hello World!");
            JToken tokJobId = LoadSetting(SETTINGS_FILENAME, "JobId");
            int jobId = tokJobId == null ? 10000 : tokJobId.Value<int>();
            Console.WriteLine("job ID=" + jobId);
            int successJobId = 0;
            int failCount = 0;
            string kPath = "\\\\Kymeta.local\\Shares\\LabData\\staging\\PT_ACPLOG\\";
            try
            {
                while (true)
                {
                    string baseUri = $"{ACP_LOG_URI}/{jobId}";
                    Uri jobSummaryUri = new Uri($"{baseUri}/{RUN_SUMMARY_FILENAME}");
                    bool readSummarySuccess = DownloadFile(jobSummaryUri, RUN_SUMMARY_FILENAME, "");

                    if (readSummarySuccess)
                    {
                        JToken ASM = LoadSetting(RUN_SUMMARY_FILENAME, "asm_info");

                        if (ASM != null)
                        {

                            string asm_serial_number = ASM.Value<String>("asm-serial-number");

                            string AcpLogFilename = "acp.log";

                            Uri XpolFilenameUri = new Uri($"{baseUri}/{AcpLogFilename}");

                            if (DownloadFile(XpolFilenameUri, jobId.ToString() + "." + asm_serial_number + "." + AcpLogFilename, kPath + jobId.ToString() + "." + asm_serial_number + "." + AcpLogFilename))
                            {

                                Console.WriteLine("json file: " + Path.GetFullPath(AcpLogFilename));
                                Console.WriteLine("json file: " + kPath + jobId.ToString() + "." + asm_serial_number + "." + AcpLogFilename);

                            }
                        }
                        successJobId = jobId;
                        failCount = 0;
                    }
                    else
                    {
                        failCount++;
                        if (failCount > 100)
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
            catch (Exception e)
            {
                Console.WriteLine("Exception " + e.Message);
                SaveLastJobId(successJobId + 1);
            }


        }

    }
}
