using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using System.Linq;
using System.Collections.Generic;

namespace FeedMeasureData
{
    public static class Function1
    {
        [FunctionName("CopyFeedMeasureData")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("BigDataStorage"));
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer rawContainer = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawFeedMeasureContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("FeedMeasureContainer"));

                string Numofdays = req.Query["numberOfDays"];

                string sheaderNOD = req.Headers["numberOfDays"];


                Numofdays = Numofdays ?? sheaderNOD;

                var NumofdaysInt = -1;
                try
                {
                    NumofdaysInt = -1 * int.Parse(Numofdays);
                }
                catch
                {
                    NumofdaysInt = -1;
                }
                log.LogInformation("number of days=" + NumofdaysInt);

                var blobs = await rawContainer.ListBlobsSegmentedAsync("", true, BlobListingDetails.All, null, null, null, null);
                foreach (var blob in blobs.Results)
                {

                    var block = blob as CloudBlockBlob;

                    var content = await block.DownloadTextAsync();

                    // Var filename = blob.Uri.
                    log.LogInformation("Original abs file path=" + blob.Uri.AbsolutePath);

                    var blobPath = blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.IndexOf('/', 1) + 1);
                    var blobpatharray = blobPath.Split("/");
                    var sdatepreviousday = DateTime.Now.AddDays(NumofdaysInt);
                    var sDestination = "";
                    if (blobpatharray.Length == 4)
                    {
                        DateTime oDate = DateTime.ParseExact(blobpatharray[2].Substring(0, 8) + " " + blobpatharray[2].Substring(9), "yyyyMMdd HHmmss", null);
                        log.LogInformation("oDate=" + oDate);
                        log.LogInformation("sdatepreviousday=" + sdatepreviousday);
                        if (blobpatharray[3].Contains("fm_summary") && (oDate.CompareTo(sdatepreviousday) >= 0)) //summary file
                        {
                            sDestination = blobpatharray[2].Substring(0, 8) + "/" + blobpatharray[2].Substring(9) + "/" + "summary" + "/" + blobpatharray[3].Replace(".csv", ".json");
                            var jsonObj = GetFMSummaryData(content, blobpatharray[3]);
                            log.LogInformation("summary file path=" + blobPath);
                            CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                            var js = JsonConvert.SerializeObject(jsonObj);
                            await cloudBlockBlob.UploadTextAsync(js);

                        }
                        else if (blobpatharray[3].Contains("fm_config") && (oDate.CompareTo(sdatepreviousday) >= 0)) //config file
                        {
                            sDestination = blobpatharray[2].Substring(0, 8) + "/" + blobpatharray[2].Substring(9) + "/" + "config" + "/" + blobpatharray[3].Replace(".csv", ".json");
                            var jsonObj = GetFMConfigData(content, blobpatharray[3], oDate.ToString(), blobpatharray[1].Trim());
                            log.LogInformation("config file path=" + blobPath);
                            CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                            var js = JsonConvert.SerializeObject(jsonObj);
                            await cloudBlockBlob.UploadTextAsync(js);
                        }
                        else if (blobpatharray[3].Contains(".s2p") && (oDate.CompareTo(sdatepreviousday) >= 0)) //s2p file
                        {
                            sDestination = blobpatharray[2].Substring(0, 8) + "/" + blobpatharray[2].Substring(9) + "/" + "s2p" + "/" + blobpatharray[3].Replace(".s2p", ".json");
                            var jsonObj = GetS2PCSV(content, blobpatharray[3], oDate.ToString(), blobpatharray[1].Trim());
                            log.LogInformation("s2p file path=" + blobPath);
                            CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                            var js = JsonConvert.SerializeObject(jsonObj);
                            await cloudBlockBlob.UploadTextAsync(js);
                        }
                    }



                }
                return new OkResult();
                //return (ActionResult)new OkObjectResult("{\"Id\":\"123\"}");
            }
            catch(Exception ex)
            {
                return new BadRequestObjectResult(ex);
            }
        }
        private static S2P GetS2PCSV(String content,String filename, String sdate, String serialNumber)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new S2P();

            jsonObj.File = filename.Trim();
            // jsonObj.Date = lines.First(l => l.StartsWith("!Date"));
            // jsonObj.Date = jsonObj.Date.Substring(jsonObj.Date.IndexOf(':') + 1).Trim();
            jsonObj.Date = sdate.Trim();
            jsonObj.Serial = serialNumber;

            jsonObj.data = new List<S2PData>();

            foreach (var item in lines)
            {
                if (!item.StartsWith("!") && !item.StartsWith("#"))
                {
                    var items = item.Split(" ", StringSplitOptions.None);
                    if (items.Length == 9)
                    {
                        try
                        {
                            jsonObj.data.Add(new S2PData
                            {
                                Stimulus = long.Parse(items[0]),
                                RealS11 = double.Parse(items[1]),
                                ImagS11 = double.Parse(items[2]),
                                RealS21 = double.Parse(items[3]),
                                ImagS21 = double.Parse(items[4]),
                                RealS12 = double.Parse(items[5]),
                                ImagS12 = double.Parse(items[6]),
                                RealS22 = double.Parse(items[7]),
                                ImagS22 = double.Parse(items[8])
                            });
                        }
                        catch(Exception e)
                        {
                            Console.WriteLine("exception=" + e.StackTrace);
                            continue;
                        }
                    }
                }

            }

            return jsonObj;
        }

        private static FMConfig GetFMConfigData(String content, String filename, String sdate, String serialNumber)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new FMConfig();

            jsonObj.File = filename.Trim();
            jsonObj.Date = sdate.Trim();
            jsonObj.Serial = serialNumber;

            jsonObj.data = new List<FMConfigData>();

            foreach (var item in lines)
            {
                if (!item.StartsWith("row"))
                {
                    var items = item.Split(",", StringSplitOptions.None);
                    if (items.Length == 12)
                    {
                        try
                        {
                            jsonObj.data.Add(new FMConfigData
                            {
                                Row = long.Parse(items[0]),
                                RxPhi = double.Parse(items[1]),
                                RxPolType = items[2],
                                RxTheta = double.Parse(items[3]),
                                RxFrequency = double.Parse(items[4]),
                                RxPol = double.Parse(items[5]),
                                TxTheta = double.Parse(items[6]),
                                SettlingTime = double.Parse(items[7]),
                                TxFrequency = double.Parse(items[8]),
                                TxPhi = double.Parse(items[9]),
                                TxPol = double.Parse(items[10]),
                                TxPolType = items[11]
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception=" + e.StackTrace);
                            continue;
                        }
                    }
                }

            }

            return jsonObj;
        }

        private static FMSummary GetFMSummaryData(String content, String filename)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new FMSummary();

            jsonObj.File = filename.Trim();
            jsonObj.Date = lines.First(l => l.StartsWith("date")).Substring(lines.First(l => l.StartsWith("date")).IndexOf(',')+1 ).Trim();
            jsonObj.Serial = lines.First(l => l.StartsWith("serial")).Substring(lines.First(l => l.StartsWith("serial")).IndexOf(',')+1).Trim();
            jsonObj.ProductionMode = lines.First(l => l.StartsWith("productionMode")).Substring(lines.First(l => l.StartsWith("productionMode")).IndexOf(',')+1).Trim();
            jsonObj.Ticket = lines.First(l => l.StartsWith("ticket")).Substring(lines.First(l => l.StartsWith("ticket")).IndexOf(',')+1).Trim();
            jsonObj.Operator = lines.First(l => l.StartsWith("operator")).Substring(lines.First(l => l.StartsWith("operator")).IndexOf(',')+1).Trim();
            jsonObj.FirmwareVersion = lines.First(l => l.StartsWith("firmwareVersion")).Substring(lines.First(l => l.StartsWith("firmwareVersion")).IndexOf(',')+1).Trim();
            jsonObj.OriginalFilename = lines.First(l => l.StartsWith("originalFilename")).Substring(lines.First(l => l.StartsWith("originalFilename")).IndexOf(',')+1).Trim();
            jsonObj.Port = lines.First(l => l.StartsWith("port")).Substring(lines.First(l => l.StartsWith("port")).IndexOf(',')+1).Trim();
            jsonObj.ReceiverStatus = lines.First(l => l.StartsWith("recieverStatus")).Substring(lines.First(l => l.StartsWith("recieverStatus")).IndexOf(',')+1).Trim();
            jsonObj.VnaStartGHz = lines.First(l => l.StartsWith("vnaStartGHz")).Substring(lines.First(l => l.StartsWith("vnaStartGHz")).IndexOf(',')+1).Trim();
            jsonObj.VnaStopGHz = lines.First(l => l.StartsWith("vnaStopGHz")).Substring(lines.First(l => l.StartsWith("vnaStopGHz")).IndexOf(',')+1).Trim();
            jsonObj.VnaPoints = lines.First(l => l.StartsWith("vnaPoints")).Substring(lines.First(l => l.StartsWith("vnaPoints")).IndexOf(',')+1).Trim();
            jsonObj.VnaCalFile = lines.First(l => l.StartsWith("vnaCalFile")).Substring(lines.First(l => l.StartsWith("vnaCalFile")).IndexOf(',')+1).Trim();

            jsonObj.data = new List<FMSummaryData>();

            bool start = false;
            foreach (var item in lines)
            {
                if (start)
                {
                    var items = item.Split(",", StringSplitOptions.None);
                    if (items.Length == 8)
                    {
                        try
                        {
                            jsonObj.data.Add(new FMSummaryData
                            {
                                Aperture = items[0],
                                PatternFreqGHz = double.Parse(items[1]),
                                S11FreqGHz = double.Parse(items[2]),
                                S11Gain = double.Parse(items[3]),
                                S21FreqGHz = double.Parse(items[4]),
                                S21Gain = double.Parse(items[5]),
                                S2pFile = items[6],
                                Status = items[7]
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception=" + e.StackTrace);
                            continue;
                        }
                    }
                }
                if (item.StartsWith("aperture"))
                {
                    start = true;
                }

            }

            return jsonObj;
        }
    }

}
