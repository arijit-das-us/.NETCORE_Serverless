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

namespace Test1DataImport
{
    public static class Function1
    {
        [FunctionName("CopyTest1Data")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("BigDataStorage"));
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("W2sakcsStorage"));
                CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawTest1DataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("Test1DataContainer"));

                string datetime = req.Query["yearAndMonth"];

                string sheaderNOM = req.Headers["yearAndMonth"];


                datetime = datetime ?? sheaderNOM ?? DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString();


                log.LogInformation("date time = " + datetime);

                var blobs_crosswalk = await container.ListBlobsSegmentedAsync("Post_OptS11_Test1/ASM_Feed_Crosswalk", true, BlobListingDetails.All, null, null, null, null);
                Dictionary<string, string> SegmentCW = new Dictionary<string, string>();
                foreach (var blob in blobs_crosswalk.Results)
                {

                    var block = blob as CloudBlockBlob;

                    var content = await block.DownloadTextAsync();

                    SegmentCW = GetASMFeedASMCrosswalk(content);

                }

                var list = await rawContainer.ListBlobsSegmentedAsync("ASM_Test1/"+ datetime, true, BlobListingDetails.All, null, null, null, null);
                //Loading summary files (.csv)
                var blobs = list.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv") && !Path.GetDirectoryName(b.Uri.AbsolutePath).ToLower().Contains("archive")));
                log.LogInformation("blob count for summary files=" + blobs.Count());

                foreach (var blob in blobs)
                {

                    var block = blob as CloudBlockBlob;

                    var content = await block.DownloadTextAsync();

                    // Var filename = blob.Uri.                                                                                                                                                                                                                                                                                                                                                                                                                                                
                    //log.LogInformation("Original abs file path=" + blob.Uri.AbsolutePath);

                    var blobPath = blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.IndexOf('/', 1) + 1);
                    var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    var sDestination = "Post_OptS11_Test1/Summary" + "/" + filepath + "/" + filename.Replace(".csv", ".json");

                    var jsonObj = GetTest1SummaryData(content, filename, filepath, SegmentCW);
                    if (jsonObj != null)
                    {
                        CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                        var js = JsonConvert.SerializeObject(jsonObj);
                        await cloudBlockBlob.UploadTextAsync(js);
                    }

                }

                //Loading s2p files (.s2p)
                //var blobsS2p = list.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".s2p") && Path.GetFileName(b.Uri.AbsolutePath).StartsWith(Path.GetDirectoryName(b.Uri.AbsolutePath).Substring(Path.GetDirectoryName(b.Uri.AbsolutePath).LastIndexOf('/')+1))));
                /*
                var blobsS2p = list.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".s2p") && Path.GetFileName(b.Uri.AbsolutePath).StartsWith(Path.GetDirectoryName(b.Uri.AbsolutePath).Substring(Path.GetDirectoryName(b.Uri.AbsolutePath).LastIndexOf(Path.DirectorySeparatorChar) + 1))));
                log.LogInformation("blob count for s2p files=" + blobsS2p.Count());

                foreach (var blob in blobsS2p)
                {

                    var block = blob as CloudBlockBlob;

                    var content = await block.DownloadTextAsync();

                   
                    var feedSN = Path.GetDirectoryName(blob.Uri.AbsolutePath).Substring(Path.GetDirectoryName(blob.Uri.AbsolutePath).LastIndexOf(Path.DirectorySeparatorChar) + 1);
                    

                    var blobPath = blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.IndexOf('/', 1) + 1);
                    var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    var sDestination = "Post_OptS11_Test1/RawData" + "/" + filepath + "/" + filename.Replace(".s2p", ".json");

                    var jsonObj = GetTest1RawData(content, filename, filepath, SegmentCW, feedSN);

                    if (jsonObj != null) {
                        CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                        var js = JsonConvert.SerializeObject(jsonObj);
                        await cloudBlockBlob.UploadTextAsync(js);
                    }

                }*/

                return new OkResult();
                //return (ActionResult)new OkObjectResult("{\"Id\":\"123\"}");
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        private static Test1Summary GetTest1SummaryData(String content, string filename, string filelocation, Dictionary<string, string> dic_cw)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new Test1Summary();
            try
            {
                string FeedSN = lines.First(l => l.StartsWith("Feed SN:")).Substring(lines.First(l => l.StartsWith("Feed SN:")).IndexOf(',') + 1).Trim();
                //string ASMSN = "";
                //dic_cw.TryGetValue(FeedSN, out ASMSN);

                jsonObj.FeedSN = FeedSN;
                //jsonObj.ASMSN = ASMSN;
                jsonObj.OriginalFilename = filename.Trim();
                try
                {
                    jsonObj.DTG = DateTime.ParseExact(lines.First(l => l.StartsWith("DTG:")).Substring(lines.First(l => l.StartsWith("DTG:")).IndexOf(',') + 1).Trim(), "yyyyMMddHHmmss", null);
                }
                catch (Exception e)
                {
                    Console.WriteLine("exception=" + e.StackTrace);
                    Console.WriteLine("filename=" + filename);

                }
                jsonObj.FileLocation = filelocation.Trim();


                jsonObj.data = new List<Test1SummaryData>();

                bool start = false;
                foreach (var item in lines)
                {
                    if (item.StartsWith("Type,"))
                    {
                        start = false;
                    }
                    if (start)
                    {
                        var items = item.Split(",", StringSplitOptions.None);
                        if (items.Length == 9
                            )
                        {
                            try
                            {

                                jsonObj.data.Add(new Test1SummaryData
                                {
                                    SerialNumber = items[0],
                                    Type = items[1],
                                    RxAvgdB = double.Parse(items[2].Equals("") ? "0.0" : items[2]),
                                    TxAvgdB = double.Parse(items[3].Equals("") ? "0.0" : items[3]),
                                    RxFreqPeakGHz = double.Parse(items[4].Equals("") ? "0.0" : items[4]),
                                    TxFreqPeakGHz = double.Parse(items[5].Equals("") ? "0.0" : items[5]),
                                    RxPeakdB = double.Parse(items[6].Equals("") ? "0.0" : items[6]),
                                    TxPeakdB = double.Parse(items[7].Equals("") ? "0.0" : items[7]),
                                    FileName = items[8]
                                });
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("exception=" + e.StackTrace);

                                continue;
                            }
                        }
                    }
                    if (item.StartsWith("Serial Number"))
                    {
                        start = true;
                    }

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("exception=" + e.StackTrace);
                jsonObj = null;
            }
            

            return jsonObj;
        }

        private static Test1Raw GetTest1RawData(String content, string filename, string filelocation, Dictionary<string, string> dic_cw, String feedSN)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new Test1Raw();

            string FeedSN = feedSN.Trim();
            //string ASMSN = "";
            //dic_cw.TryGetValue(FeedSN, out ASMSN);

            jsonObj.FeedSN = FeedSN;
            //jsonObj.ASMSN = ASMSN;
            jsonObj.OriginalFilename = filename.Trim();
            String timestamp = filename.Substring(filename.LastIndexOf("_") + 1, filename.LastIndexOf(".") - filename.LastIndexOf("_") - 1);
            try
            {
                jsonObj.DTG = DateTime.ParseExact(timestamp, "yyyyMMddHHmmss", null);
            }
            catch (Exception e)
            {
                Console.WriteLine("timestamp=" + timestamp + "?");
                Console.WriteLine("exception=" + e.StackTrace);
                Console.WriteLine("filename=" + filename);

            }
            jsonObj.FileLocation = filelocation.Trim();


            jsonObj.data = new List<Test1RawData>();

            bool start = false;
            foreach (var item in lines)
            {
                
                if (start)
                {
                    var items = item.Split(" ", StringSplitOptions.None);
                    if (items.Length == 9
                        )
                    {
                        try
                        {

                            jsonObj.data.Add(new Test1RawData
                            {
                                
                                Frequency = double.Parse(items[0].Equals("") ? "0.0" : items[0]),
                                RealS11 = double.Parse(items[1].Equals("") ? "0.0" : items[1]),
                                ImaginaryS11 = double.Parse(items[2].Equals("") ? "0.0" : items[2])
                                
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception=" + e.StackTrace);

                            continue;
                        }
                    }
                }
                if (item.StartsWith("!freq"))
                {
                    start = true;
                }

            }
            return jsonObj;
        }

        private static Dictionary<string, string> GetASMFeedASMCrosswalk(String content)
        {
            string[] lines = content.Split(Environment.NewLine);

            Dictionary<string, string> d = new Dictionary<string, string>();

            foreach (var item in lines)
            {
                var items = item.Split(",", StringSplitOptions.RemoveEmptyEntries);
                if (items.Length == 7)
                {
                    d.Add(items[0], items[2]);
                }
            }
            return d;
        }
    }
}
