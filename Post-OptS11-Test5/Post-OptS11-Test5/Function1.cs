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

namespace Post_OptS11_Test5
{
    public static class Function1
    {
        [FunctionName("CopyASMTest5")]
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
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawPostOptS11Test5Container"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("PostOptS11Test5DataContainer"));

                var list = await rawContainer.ListBlobsSegmentedAsync("ASM_Test4_Hertz/ABK", true, BlobListingDetails.All, null, null, null, null);

                var blobs = list.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv") & Path.GetFileName(b.Uri.AbsolutePath).Contains("_ATP_") & !Path.GetDirectoryName(b.Uri.AbsolutePath).ToLower().EndsWith("archive")));
                //log.LogInformation("blob count=" + blobs.Count());
                foreach (var blob in blobs)
                {

                    var block = blob as CloudBlockBlob;

                    var content = await block.DownloadTextAsync();

                    // Var filename = blob.Uri.
                    log.LogInformation("Original abs file path=" + blob.Uri.AbsolutePath);

                    var blobPath = blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.IndexOf('/', 1) + 1);
                    //var blobpatharray = blobPath.Split("/");
                    //log.LogInformation("blob path=" + blobPath);
                    var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    //log.LogInformation("blob filename=" + filename);
                    var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    var sDestination = "Post_OptS11_Test5" + "/" + filepath + "/" + filename.Replace(".csv", ".json");
                    log.LogInformation("blob destination=" + sDestination);
                    var jsonObj = ToJson(content, filename);

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

        private static RootObject ToJson(string content,String Filename)
        {
            string[] lines = content.Split(Environment.NewLine);

            var line = lines.First(l => l.StartsWith("Web API pattern used:"));
            line = line.Substring(line.IndexOf('{'), line.LastIndexOf('}') - line.IndexOf('{') + 1);
           // var dataObj = JsonConvert.DeserializeObject < List<Data>>(line);

        //    Console.WriteLine(dataObj);
            
            //jsonObj.File = lines.First(l => l.StartsWith("! File")).Split(":")[1].Trim();
            //jsonObj.Date = lines.First(l => l.StartsWith("! Date"));
            //jsonObj.Date = jsonObj.Date.Substring(jsonObj.Date.IndexOf(':') + 1).Trim();

            var jsonObj = new RootObject();
            jsonObj.asm_number = lines[0];
            jsonObj.Production_Mode = lines.First(l => l.StartsWith("Production Mode")).Substring(lines.First(l => l.StartsWith("Production Mode")).IndexOf(',') + 1).Trim();
            jsonObj.Operated_by = lines.First(l => l.StartsWith("Operated by")).Substring(lines.First(l => l.StartsWith("Operated by")).IndexOf(',') + 1).Trim();
            jsonObj.start_time = lines.First(l => l.StartsWith("Time Started")).Substring(lines.First(l => l.StartsWith("Time Started")).IndexOf(',') + 1).Trim();
            jsonObj.asm_firmware_version = lines.First(l => l.StartsWith("Firmware version")).Substring(lines.First(l => l.StartsWith("Firmware version")).IndexOf(',') + 1).Trim();
            jsonObj.Calibration_file = lines.First(l => l.StartsWith("Calibration file used")).Substring(lines.First(l => l.StartsWith("Calibration file used")).IndexOf(',') + 1).Trim();
            jsonObj.Receiver_Used = lines.First(l => l.StartsWith("Receiver Used")).Substring(lines.First(l => l.StartsWith("Receiver Used")).IndexOf(',') + 1).Trim();
            jsonObj.Receiver_Status = lines.First(l => l.StartsWith("Receiver Status")).Substring(lines.First(l => l.StartsWith("Receiver Status")).IndexOf(',') + 1).Trim();
            jsonObj.band = lines.First(l => l.StartsWith("Rx Span")).Substring(lines.First(l => l.StartsWith("Rx Span")).IndexOf(':') + 1).Trim();
            jsonObj.FileName = Filename;
            //jsonObj.data = List<Data>(dataObj);
            jsonObj.rows = new List<Rows>();

            bool start = false;
            foreach (var item in lines)
            {
                if (start)
                {
                    var items = item.Split(",", StringSplitOptions.None);
                    if (items.Length == 7)
                    {
                        try
                        {
                            jsonObj.rows.Add(new Rows
                            {
                                AtFrequency = Double.Parse(items[0]),
                                FreqGHz = double.Parse(items[1]),
                                GainDB = double.Parse(items[2]),
                                CurrentA = double.Parse(items[3]),
                                Humidity = double.Parse(items[4]),
                                s2pfile = items[5],
                                status = items[6]
                            });
                        }
                        catch
                        {
                            continue;
                        }
                    }
                }


                if (item.StartsWith("At Frequency"))
                {
                    start = true;
                }
            }

            return jsonObj;
        }
    }
}
