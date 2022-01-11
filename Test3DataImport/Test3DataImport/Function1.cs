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
using System.Globalization;
using System.Collections.Generic;

namespace Test3DataImport
{
    public static class Function1
    {
        [FunctionName("CopyTest3LiteData")]
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
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawTest3DataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("Test3DataContainer"));

                string datetime = req.Query["yearAndMonth"];

                string sheaderNOM = req.Headers["yearAndMonth"];


                datetime = datetime ?? sheaderNOM ?? DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString();


                log.LogInformation("date time = " + datetime);

                

                var list1 = await rawContainer.ListBlobsSegmentedAsync("ASM_Test3_Lite/" + datetime, true, BlobListingDetails.All, null, null, null, null);
                var list2 = await rawContainer.ListBlobsSegmentedAsync("Test3_Lite/" + datetime, true, BlobListingDetails.All, null, null, null, null);
                //Loading csv files (.csv)
                
                var blobs1 = list1.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv")));
                var blobs2 = list2.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv")));

                //var Totalblobs = blobs1.Concat(blobs2);
                var Totalblobs = blobs1;

                log.LogInformation("blob count total for Test3 files=" + Totalblobs.Count());



                foreach (var blob in Totalblobs)
                {

                    var block = blob as CloudBlockBlob;

                    var content = await block.DownloadTextAsync();
                                                                                                                                                                                                                                                                                                                                                                                                                                              
                    

                    var blobPath = block.Name;
                    var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    var sDestination = "Post_OptS11_Test3/Lite/" + block.Name.Replace(".csv", ".json");
                    log.LogInformation("Original abs file path=" + blobPath);
                    log.LogInformation("destination file path=" + sDestination);
                    var jsonObj = GetTest3Data(content, filename, filepath);

                    CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                    var js = JsonConvert.SerializeObject(jsonObj);
                    await cloudBlockBlob.UploadTextAsync(js);


                }


                return new OkResult();
                //return (ActionResult)new OkObjectResult("{\"Id\":\"123\"}");
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        private static Test3Lite GetTest3Data(String content, string filename, string filelocation)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new Test3Lite();

            jsonObj.OriginalFilename = filename.Trim();

            jsonObj.FileLocation = filelocation.Trim();

            try
            {
                jsonObj.SoftwareName = lines.First(l => l.StartsWith("Software Name")).Split(",")[1].Trim();
                jsonObj.SoftwareVersion = lines.First(l => l.StartsWith("Software Version")).Split(",")[1].Trim();
                jsonObj.ASMSN = lines.First(l => l.StartsWith("ASM SN")).Split(",")[1].Trim();
                jsonObj.ASMPN = lines.First(l => l.StartsWith("ASM PN")).Split(",")[1].Trim();
                //jsonObj.ControllerInfo = lines.First(l => l.StartsWith("Controller Information")).Split(",")[1].Trim();


                string strdt = lines.First(l => l.StartsWith("DTG")).Split(",")[1].Trim();
                DateTime dt;
                CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
                DateTimeStyles styles = DateTimeStyles.None;
                //DateTime.TryParse(strdt, culture, styles, out dt);
                DateTime.TryParseExact(strdt, "yyyyMMddHHmmss", culture, styles, out dt);
                jsonObj.DTG = dt;

                jsonObj.data = new List<Test3LiteData>();

                bool start = false;
                foreach (var item in lines)
                {

                    if (start)
                    {
                        var items = item.Split(",", StringSplitOptions.None);
                        if (items.Length == 4
                            )
                        {
                            try
                            {

                                jsonObj.data.Add(new Test3LiteData
                                {
                                    Name = items[0].Trim(),
                                    Value = items[1].Trim(),
                                    PassFail = items[2].Trim(),
                                    Message = items[3]
                                });
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("Filename=" + filename);
                                Console.WriteLine("filelocation=" + filelocation);
                                Console.WriteLine("exception=" + e.StackTrace);

                                continue;
                            }
                        }
                    }
                    if (item.StartsWith("Name"))
                    {
                        start = true;
                    }

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception for the File=" + filename);
                Console.WriteLine("exception=" + e.StackTrace);
            }
            return jsonObj;
        }
    }

    public static class Function2
    {
        [FunctionName("CopyTest3AdamData")]
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
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawTest3DataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("Test3DataContainer"));

                string datetime = req.Query["yearAndMonth"];

                string sheaderNOM = req.Headers["yearAndMonth"];


                datetime = datetime ?? sheaderNOM ?? DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString();


                log.LogInformation("date time = " + datetime);



                var list1 = await rawContainer.ListBlobsSegmentedAsync("ASM_Test3/" + datetime, true, BlobListingDetails.All, null, null, null, null);
               
                var blobs1 = list1.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv")));

                //var Totalblobs = blobs1.Concat(blobs2);
                var Totalblobs = blobs1;

                log.LogInformation("blob count total for Test3 Adam files=" + Totalblobs.Count());



                foreach (var blob in Totalblobs)
                {

                    var block = blob as CloudBlockBlob;

                    var content = await block.DownloadTextAsync();



                    var blobPath = block.Name;
                    var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    var sDestination = "Post_OptS11_Test3/Adam/" + block.Name.Replace(".csv", ".json");
                    //log.LogInformation("Original abs file path=" + blobPath);
                    //log.LogInformation("destination file path=" + sDestination);
                    var jsonObj = GetTest3AdamData(content, filename, filepath);

                    CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                    var js = JsonConvert.SerializeObject(jsonObj);
                    await cloudBlockBlob.UploadTextAsync(js);


                }


                return new OkResult();
                //return (ActionResult)new OkObjectResult("{\"Id\":\"123\"}");
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        private static Test3Adam GetTest3AdamData(String content, string filename, string filelocation)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new Test3Adam();

            jsonObj.OriginalFilename = filename.Trim();

            jsonObj.FileLocation = filelocation.Trim();

            jsonObj.SoftwareName = lines.First(l => l.StartsWith("Software Name")).Split(",")[1].Trim();
            jsonObj.SoftwareVersion = lines.First(l => l.StartsWith("Software Version")).Split(",")[1].Trim();
            jsonObj.ASMSN = lines.First(l => l.StartsWith("ASM SN")).Split(",")[1].Trim();
            jsonObj.ASMPN = lines.First(l => l.StartsWith("ASM PN")).Split(",")[1].Trim();
            jsonObj.ControllerInfo = lines.First(l => l.StartsWith("Controller Information:")).Substring(lines.First(l => l.StartsWith("Controller Information:")).IndexOf(',') + 1).Trim();
            jsonObj.AntennaSN = lines.First(l => l.StartsWith("Antenna SN")).Split(",")[1].Trim();


            string strdt = lines.First(l => l.StartsWith("DTG")).Split(",")[1].Trim();
            DateTime dt;
            CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
            DateTimeStyles styles = DateTimeStyles.None;
            //DateTime.TryParse(strdt, culture, styles, out dt);
            DateTime.TryParseExact(strdt, "yyyyMMddHHmmss", culture, styles, out dt);
            jsonObj.DTG = dt;

            jsonObj.data = new List<Test3AdamData>();

            bool start = false;
            foreach (var item in lines)
            {
                if (item.StartsWith("Scan"))
                {
                    start = false;
                }

                if (start)
                {
                    var items = item.Split(",", StringSplitOptions.None);
                    if (items.Length == 21
                        )
                    {
                        try
                        {

                            jsonObj.data.Add(new Test3AdamData
                            {
                                RxCommandedFreq = double.Parse(items[0].Equals("") ? "0.0" : items[0]),
                                RxActualFreq = double.Parse(items[1].Equals("") ? "0.0" : items[1]),
                                RxActualCommandGain = double.Parse(items[2].Equals("") ? "0.0" : items[2]),
                                TxCommandedFreq = double.Parse(items[3].Equals("") ? "0.0" : items[3]),
                                TxActualFreq = double.Parse(items[4].Equals("") ? "0.0" : items[4]),
                                TxActualCommandGain = double.Parse(items[5].Equals("") ? "0.0" : items[5]),
                                RxMaxGainFreq = double.Parse(items[6].Equals("") ? "0.0" : items[6]),
                                RxMaxGain = double.Parse(items[7].Equals("") ? "0.0" : items[7]),
                                TxMaxGainFreq = double.Parse(items[8].Equals("") ? "0.0" : items[8]),
                                TxMaxGain = double.Parse(items[9].Equals("") ? "0.0" : items[9]),
                                Theta = double.Parse(items[10].Equals("") ? "0.0" : items[10]),
                                Lpa = double.Parse(items[11].Equals("") ? "0.0" : items[11]),
                                Phi = double.Parse(items[12].Equals("") ? "0.0" : items[12]),
                                Current = double.Parse(items[13].Equals("") ? "0.0" : items[13]),
                                Temp = double.Parse(items[14].Equals("") ? "0.0" : items[14]),
                                RH = double.Parse(items[15].Equals("") ? "0.0" : items[15]),
                                GatingNotGated = items[16].Trim(),
                                //DateTime.TryParseExact(items[17].Trim(), "yyyyMMdd HH:mm:ss", culture, styles, out DateTime),
                                DateTime = DateTime.ParseExact(items[17].Trim(), "yyyyMMdd HH:mm:ss", null),
                                s2pFile = items[18].Trim(),
                                RxJsonFileUsed = items[19].Trim(),
                                TxJsonFileUsed = items[20].Trim()
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception=" + e.StackTrace);

                            continue;
                        }
                    }
                }
                if (item.StartsWith("Rx Commanded Freq"))
                {
                    start = true;
                }
                

            }
            return jsonObj;
        }
    }
}
