using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ChoETL;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using System;
using System.IO;

namespace TestDurableOrchestration
{
    public static class EnumerableExtensions
    {
    // http://stackoverflow.com/questions/3471899/how-to-convert-linq-results-to-hashset-or-hashedset
    public static HashSet<T> ToHashSet<T>(this IEnumerable<T> source)
    {
        return new HashSet<T>(source);
    }
    }
    public static class Function1
    {
        static String BigDataStorage = "DefaultEndpointsProtocol=https;AccountName=kymetabigdata;AccountKey=HcmXJ6Ks9sV44ef5JhFtIaAEIgL+EhIF8n1wG/an8wyyVuKe/CbPNrSsQuCN7dKy/f6he4cEBi6JGYuGT6//IA==;EndpointSuffix=core.windows.net";

        [FunctionName("RawHertzCustomFunction_DurableFunction")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            


            var outputs = new List<string>();
            (string container, string sourceFolder, string soureFile, string rawzonestaging) fileInfo = context.GetInput<(string, string, string, string)>();




            (string content, string sDestination) obContent = await context.CallActivityAsync<(string, string)>("RawHertzCustomFunction_SearchAzureStorage", fileInfo);
            //log.LogInformation($"content {obContent.content}.");
            log.LogInformation($"sDestination {obContent.sDestination}.");

            string updateContent = await context.CallActivityAsync<string>("RawHertzCustomFunction_EditFile", obContent.content);
            log.LogInformation("updated Content is returned.");

            string status = await context.CallActivityAsync<string>("RawHertzCustomFunction_UploadFile", (fileInfo.rawzonestaging, obContent.sDestination, updateContent));
            log.LogInformation("updated Content uploaded.");

            //log.LogInformation($"updateContent {updateContent}.");
            // returns ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]
            return outputs;
        }

        [FunctionName("RawHertzCustomFunction_SearchAzureStorage")]
        public static async Task<(string, string)> getContent([ActivityTrigger] IDurableActivityContext inputs, ILogger log)
        {
            (string container, string sourceFolder, string soureFile, string rawzonestaging) fileInfo = inputs.GetInput<(string, string, string, string)>();

            log.LogInformation($"container {fileInfo.container}.");
            log.LogInformation($"sourceFolder {fileInfo.sourceFolder}.");
            log.LogInformation($"soureFile {fileInfo.soureFile}.");
            log.LogInformation($"rawzonestaging {fileInfo.rawzonestaging}.");
            CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(BigDataStorage);
            CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(fileInfo.container);

            BlobContinuationToken blobContinuationToken = null;
            var content = "";
            var sDestination = "";
            do
            {
                var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                    prefix: fileInfo.sourceFolder,
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.All,
                    maxResults: null,
                    currentToken: blobContinuationToken,
                    options: null,
                    operationContext: null
                );


                // Get the value of the continuation token returned by the listing call.

                blobContinuationToken = resultSegment.ContinuationToken;

                var blobs = resultSegment.Results.Where(b => (Path.GetFileName(b.Uri.AbsolutePath).EndsWith(fileInfo.soureFile))); //&& (Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("PCDMIS"))); Path.GetExtension(b.Uri.AbsolutePath).Equals(".CSV") && !Path.GetDirectoryName(b.Uri.AbsolutePath).Contains("OLD")
                log.LogInformation("blob count total for RawHertz files=" + blobs.Count());
                foreach (var blob in blobs)
                {
                    var block = blob as CloudBlockBlob;


                    content = await block.DownloadTextAsync();

                    var blobPath = block.Name;
                    var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    sDestination = filepath + "/" + filename;
                    //log.LogInformation("Original abs file path=" + blobPath);
                    //log.LogInformation("destination file path=" + sDestination);
                    blobContinuationToken = null;

                }



            } while (blobContinuationToken != null); // Loop while the continuation token is not null.

            return (content, sDestination);

            //return $"{content}";
        }

        [FunctionName("RawHertzCustomFunction_EditFile")]
        public static string UpdateContent([ActivityTrigger] string content, ILogger log)
        {

            string[] lines = content.Split(Environment.NewLine, options: StringSplitOptions.RemoveEmptyEntries);
            string csvuserinput = lines.First(l => l.Contains("! csv_user_input")).Substring(lines.First(l => l.Contains("! csv_user_input")).IndexOf("{"), lines.First(l => l.Contains("! csv_user_input")).IndexOf("}}") + 2 - lines.First(l => l.Contains("! csv_user_input")).IndexOf("{")).Trim().Replace("u'", "'").Replace("None", "null").Replace("True", "true").Replace("False", "false");
            //Console.WriteLine("user input=" + csvuserinput);
            var jsonObj = JsonConvert.DeserializeObject<CsvUserInput>(csvuserinput, new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All, Formatting = Newtonsoft.Json.Formatting.Indented });

            string csvContent = "";
            string csvHeader = "";
            int lineCount = 0;
            foreach (string s in lines)
            {

                string s1 = s.Trim();
                if (s1.StartsWith("Point"))
                {

                    csvHeader = "Point,Tertiary-Deg,Secondary-Deg,Frequency-Ghz,Primary-Deg,Ch1-dB,Ch1-Deg,jira,mtenna_build_number,start_time\r\n";
                    lineCount = lineCount + 3;
                    break;
                }
                lineCount++;
            }

            string startCsvData = lines[lineCount];
            string csvData = content.Substring(content.IndexOf(startCsvData)).Replace("\r\n", jsonObj.jira + "," + jsonObj.mtenna_build_number + "," + jsonObj.start_time + "\r\n");

            csvContent = csvHeader + csvData;
            return csvContent;

           // return $"{updatedContent}";
        }

        [FunctionName("RawHertzCustomFunction_UploadFile")]
        public static async Task<string> UploadToBlob([ActivityTrigger] IDurableActivityContext inputs, ILogger log)
        {
            (string container, string sDestination, string content) fileInfo = inputs.GetInput<(string, string, string)>();

            log.LogInformation($"container {fileInfo.container}.");
            log.LogInformation($"sDestination {fileInfo.sDestination}.");

            CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(BigDataStorage);
            CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(fileInfo.container);

            CloudBlockBlob cloudBlockBlob = rawContainer.GetBlockBlobReference(fileInfo.sDestination);
            await cloudBlockBlob.UploadTextAsync(fileInfo.content);
            return "OK";

        }

        [FunctionName("RawHertzCustomFunction_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.

            IEnumerable<string> headerValues;
            var sourceFolder = string.Empty;
            var container = string.Empty;
            var soureFile = string.Empty;
            var rawzonestaging = string.Empty;

            if (req.Headers.TryGetValues("SourceFolder", out headerValues))
            {
                sourceFolder = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("SoureFile", out headerValues))
            {
                soureFile = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("rawzonecontainer", out headerValues))
            {
                container = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("rawzonestaging", out headerValues))
            {
                rawzonestaging = headerValues.FirstOrDefault();
            }

            string body = await req.Content.ReadAsStringAsync();

            sourceFolder = sourceFolder.Substring(sourceFolder.IndexOf("/") + 1);
            log.LogInformation("rawzonecontainer=" + container);
            log.LogInformation("SourceFolder=" + sourceFolder);
            log.LogInformation("SoureFile=" + soureFile);
            log.LogInformation("rawzonestaging=" + rawzonestaging);
            log.LogInformation("Body=" + body);

            string instanceId = await starter.StartNewAsync("RawHertzCustomFunction_DurableFunction", Guid.NewGuid().ToString(), (container, sourceFolder, soureFile, rawzonestaging));

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        
    }

    public static class Function2
    {
        static String BigDataStorage = "DefaultEndpointsProtocol=https;AccountName=kymetabigdata;AccountKey=HcmXJ6Ks9sV44ef5JhFtIaAEIgL+EhIF8n1wG/an8wyyVuKe/CbPNrSsQuCN7dKy/f6he4cEBi6JGYuGT6//IA==;EndpointSuffix=core.windows.net";
        [FunctionName("KiruCustomFunction_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.

            IEnumerable<string> headerValues;
            var sourceFolder = string.Empty;
            var container = string.Empty;
            var soureFile = string.Empty;
            var rawzonestaging = string.Empty;

            if (req.Headers.TryGetValues("SourceFolder", out headerValues))
            {
                sourceFolder = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("SoureFile", out headerValues))
            {
                soureFile = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("rawzonecontainer", out headerValues))
            {
                container = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("rawzonestaging", out headerValues))
            {
                rawzonestaging = headerValues.FirstOrDefault();
            }

            string body = await req.Content.ReadAsStringAsync();

            sourceFolder = sourceFolder.Substring(sourceFolder.IndexOf("/") + 1);
            log.LogInformation("rawzonecontainer=" + container);
            log.LogInformation("SourceFolder=" + sourceFolder);
            log.LogInformation("SoureFile=" + soureFile);
            log.LogInformation("rawzonestaging=" + rawzonestaging);
            log.LogInformation("Body=" + body);

            string instanceId = await starter.StartNewAsync("KiruCustomFunction_DurableFunction", Guid.NewGuid().ToString(), (container, sourceFolder, soureFile, rawzonestaging));

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);

        }

        [FunctionName("KiruCustomFunction_DurableFunction")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            


            var outputs = new List<string>();
            (string container, string sourceFolder, string soureFile, string rawzonestaging) fileInfo = context.GetInput<(string, string, string, string)>();




            (string content, string sDestination) obContent = await context.CallActivityAsync<(string, string)>("KiruCustomFunction_SearchAzureStorage", fileInfo);
            //log.LogInformation($"content {obContent.content}.");
            log.LogInformation($"sDestination {obContent.sDestination}.");

            var updateContent = await context.CallActivityAsync<string>("KiruCustomFunction_ToCSVString", obContent.content);
            log.LogInformation("updated Content is returned.");

            string status = await context.CallActivityAsync<string>("KiruCustomFunction_UploadFile", (fileInfo.rawzonestaging, obContent.sDestination, updateContent));
            log.LogInformation("updated Content uploaded.");

            //log.LogInformation($"updateContent {updateContent}.");
            // returns ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]
            return outputs;
        }

        [FunctionName("KiruCustomFunction_SearchAzureStorage")]
        public static async Task<(string, string)> getContent([ActivityTrigger] IDurableActivityContext inputs, ILogger log)
        {
            (string container, string sourceFolder, string soureFile, string rawzonestaging) fileInfo = inputs.GetInput<(string, string, string, string)>();

            log.LogInformation($"container {fileInfo.container}.");
            log.LogInformation($"sourceFolder {fileInfo.sourceFolder}.");
            log.LogInformation($"soureFile {fileInfo.soureFile}.");
            log.LogInformation($"rawzonestaging {fileInfo.rawzonestaging}.");
            CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(BigDataStorage);
            CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(fileInfo.container);

            BlobContinuationToken blobContinuationToken = null;
            var content = "";
            var sDestination = "";
            do
            {
                var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                    prefix: fileInfo.sourceFolder,
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.All,
                    maxResults: null,
                    currentToken: blobContinuationToken,
                    options: null,
                    operationContext: null
                );


                // Get the value of the continuation token returned by the listing call.

                blobContinuationToken = resultSegment.ContinuationToken;

                var blobs = resultSegment.Results.Where(b => (Path.GetFileName(b.Uri.AbsolutePath).EndsWith(fileInfo.soureFile))); //&& (Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("PCDMIS"))); Path.GetExtension(b.Uri.AbsolutePath).Equals(".CSV") && !Path.GetDirectoryName(b.Uri.AbsolutePath).Contains("OLD")
                log.LogInformation("blob count total for Kiru json files=" + blobs.Count());
                foreach (var blob in blobs)
                {
                    var block = blob as CloudBlockBlob;


                    content = await block.DownloadTextAsync();

                    var blobPath = block.Name;
                    var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    sDestination = filepath + "/" + filename;
                    //log.LogInformation("Original abs file path=" + blobPath);
                    //log.LogInformation("destination file path=" + sDestination);
                    blobContinuationToken = null;

                }



            } while (blobContinuationToken != null); // Loop while the continuation token is not null.

            return (content, sDestination);

            //return $"{content}";
        }

        [FunctionName("KiruCustomFunction_ToCSVString")]
        public static string UpdateContent([ActivityTrigger] string content, ILogger log)
        {

            
            var sContent =  QuickConversion(content);
            Console.WriteLine(sContent + "= \n" + sContent);
            return sContent;
            /*
            Dictionary<string, object> dict = new Dictionary<string, object>();
            JToken token = JToken.Parse(content);
            FillDictionaryFromJToken(dict, token, "");
            foreach (var kvp in dict)
            {
                
                Console.WriteLine(kvp.Key + ": " + kvp.Value);
            }
            */
            
           // return $"{updatedContent}";
        }
        /*
        private static void FillDictionaryFromJToken(Dictionary<string, object> dict, JToken token, string prefix)
        {
            switch (token.Type)
            {
                case JTokenType.Object:
                foreach (JProperty prop in token.Children<JProperty>())
                {
                    FillDictionaryFromJToken(dict, prop.Value, Join(prefix, prop.Name));
                }
                break;

                case JTokenType.Array:
                int index = 0;
                foreach (JToken value in token.Children())
                {
                    FillDictionaryFromJToken(dict, value, Join(prefix, index.ToString()));
                    index++;
                }
                break;

                default:
                dict.Add(prefix, ((JValue)token).Value);
                break;
            }
        }

        private static string Join(string prefix, string name)
        {
            return (string.IsNullOrEmpty(prefix) ? name : prefix + "." + name);
        }
*/
        private static string QuickConversion(string content){
            System.Text.StringBuilder stringBuilder = new System.Text.StringBuilder();  
            using (var KiruData = ChoJSONReader<Kiru_Data>.LoadText(content) 
                .UseJsonSerialization() 
               // .Configure(c => c.FlattenNode = true)
               // .Configure(c => c. = true)
               //    .WithJSONPath("$..fruit[*]")  
                                       )  
            {  
                using (var w = new ChoCSVWriter(stringBuilder)  
                    .WithFirstLineHeader() 
                    .Configure(c => c.ThrowAndStopOnMissingField = false)) 
                  {
            w.Write(KiruData.SelectMany(root =>
                root.testsuites
                .SelectMany(testsuites => testsuites.testsuite
                .SelectMany(testsuite => testsuite.testcase
                .Select(testcase => new
                {
                    antenna_serial_number = root.antenna_serial_number,
                    asm_serial_number = root.asm_serial_number,
                    customer = root.customer,
                    serial_number = root.serial_number,
                    testsuitesFailures = testsuites.failures,
                    testsuitesName = testsuites.name,
                    testsuites.start_time,
                    testsuites.stop_time,
                    testsuiteTests = testsuite.tests,
                    testcase.classname,
                    testcaseName=testcase.name,
                    testcaseStatus=testcase.status,
                    testcaseTime=testcase.time,
                    testcaseValues =  testcase.data.IsNull()? "": "[" + String.Join(", ",testcase.data.values) + "]",
                }

                )
                  )
                )
            )
            ); 
        }
            }  

            return stringBuilder.ToString();
        }

/*
        private static void converToCSV(string json){
            var obj = JObject.Parse(json);

        // Collect column titles: all property names whose values are of type JValue, distinct, in order of encountering them.
        var values = obj.DescendantsAndSelf()
            .OfType<JProperty>()
            .Where(p => p.Value is JValue)
            .GroupBy(p => p.Name)
            .ToList();

        var columns = values.Select(g => g.Key).ToArray();

        // Filter JObjects that have child objects that have values.
        var parentsWithChildren = values.SelectMany(g => g).SelectMany(v => v.AncestorsAndSelf().OfType<JObject>().Skip(1)).ToHashSet();

        // Collect all data rows: for every object, go through the column titles and get the value of that property in the closest ancestor or self that has a value of that name.
        var rows = obj
            .DescendantsAndSelf()
            .OfType<JObject>()
            .Where(o => o.PropertyValues().OfType<JValue>().Any())
            .Where(o => o == obj ) // Show a row for the root object + objects that have no children.
            .Select(o => columns.Select(c => o.AncestorsAndSelf()
                .OfType<JObject>()
                .Select(parent => parent[c])
                .Where(v => v is JValue)
                .Select(v => (string)v)
                .FirstOrDefault())
                .Reverse() // Trim trailing nulls
                .SkipWhile(s => s == null)
                .Reverse());

        // Convert to CSV
        var csvRows = new[] { columns }.Concat(rows).Select(r => string.Join(",", r));
        var csv = string.Join("\n", csvRows);

        Console.WriteLine(csv);
        }
*/

        [FunctionName("KiruCustomFunction_UploadFile")]
        public static async Task<string> UploadToBlob([ActivityTrigger] IDurableActivityContext inputs, ILogger log)
        {
            (string container, string sDestination, string content) fileInfo = inputs.GetInput<(string, string, string)>();

            log.LogInformation($"container {fileInfo.container}.");
            log.LogInformation($"sDestination {fileInfo.sDestination}.");


            CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(BigDataStorage);
            CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(fileInfo.container);

            CloudBlockBlob cloudBlockBlob = rawContainer.GetBlockBlobReference(fileInfo.sDestination);

            await cloudBlockBlob.UploadTextAsync(fileInfo.content);

            
            return "OK";

        }
    }



    public static class Function3
    {
        static String BigDataStorage = "DefaultEndpointsProtocol=https;AccountName=kymetabigdata;AccountKey=HcmXJ6Ks9sV44ef5JhFtIaAEIgL+EhIF8n1wG/an8wyyVuKe/CbPNrSsQuCN7dKy/f6he4cEBi6JGYuGT6//IA==;EndpointSuffix=core.windows.net";
        [FunctionName("ACPLogCustomFunction_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.

            IEnumerable<string> headerValues;
            var sourceFolder = string.Empty;
            var container = string.Empty;
            var soureFile = string.Empty;
            var rawzonestaging = string.Empty;

            if (req.Headers.TryGetValues("SourceFolder", out headerValues))
            {
                sourceFolder = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("SoureFile", out headerValues))
            {
                soureFile = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("rawzonecontainer", out headerValues))
            {
                container = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("rawzonestaging", out headerValues))
            {
                rawzonestaging = headerValues.FirstOrDefault();
            }

            string body = await req.Content.ReadAsStringAsync();

            sourceFolder = sourceFolder.Substring(sourceFolder.IndexOf("/") + 1);
            log.LogInformation("rawzonecontainer=" + container);
            log.LogInformation("SourceFolder=" + sourceFolder);
            log.LogInformation("SoureFile=" + soureFile);
            log.LogInformation("rawzonestaging=" + rawzonestaging);
            log.LogInformation("Body=" + body);

            string instanceId = await starter.StartNewAsync("ACPLogCustomFunction_DurableFunction", Guid.NewGuid().ToString(), (container, sourceFolder, soureFile, rawzonestaging));

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);

        }

        [FunctionName("ACPLogCustomFunction_DurableFunction")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            


            var outputs = new List<string>();
            (string container, string sourceFolder, string soureFile, string rawzonestaging) fileInfo = context.GetInput<(string, string, string, string)>();




            (string content, string sDestination) obContent = await context.CallActivityAsync<(string, string)>("ACPLogCustomFunction_SearchAzureStorage", fileInfo);
            //log.LogInformation($"content {obContent.content}.");
            log.LogInformation($"sDestination {obContent.sDestination}.");

            var updateContent = await context.CallActivityAsync<string>("ACPLogCustomFunction_ToCSVString", obContent.content);
            log.LogInformation("updated Content is returned.");

            string status = await context.CallActivityAsync<string>("ACPLogCustomFunction_UploadFile", (fileInfo.rawzonestaging, obContent.sDestination, updateContent));
            log.LogInformation("updated Content uploaded.");

            //log.LogInformation($"updateContent {updateContent}.");
            // returns ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]
            return outputs;
        }

        [FunctionName("ACPLogCustomFunction_SearchAzureStorage")]
        public static async Task<(string, string)> getContent([ActivityTrigger] IDurableActivityContext inputs, ILogger log)
        {
            (string container, string sourceFolder, string soureFile, string rawzonestaging) fileInfo = inputs.GetInput<(string, string, string, string)>();

            log.LogInformation($"container {fileInfo.container}.");
            log.LogInformation($"sourceFolder {fileInfo.sourceFolder}.");
            log.LogInformation($"soureFile {fileInfo.soureFile}.");
            log.LogInformation($"rawzonestaging {fileInfo.rawzonestaging}.");
            CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(BigDataStorage);
            CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(fileInfo.container);

            BlobContinuationToken blobContinuationToken = null;
            var content = "";
            var sDestination = "";
            do
            {
                var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                    prefix: fileInfo.sourceFolder,
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.All,
                    maxResults: null,
                    currentToken: blobContinuationToken,
                    options: null,
                    operationContext: null
                );


                // Get the value of the continuation token returned by the listing call.

                blobContinuationToken = resultSegment.ContinuationToken;

                var blobs = resultSegment.Results.Where(b => (Path.GetFileName(b.Uri.AbsolutePath).EndsWith(fileInfo.soureFile))); //&& (Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("PCDMIS"))); Path.GetExtension(b.Uri.AbsolutePath).Equals(".CSV") && !Path.GetDirectoryName(b.Uri.AbsolutePath).Contains("OLD")
                log.LogInformation("blob count total for ACPLog json files=" + blobs.Count());
                foreach (var blob in blobs)
                {
                    var block = blob as CloudBlockBlob;


                    content = await block.DownloadTextAsync();

                    var blobPath = block.Name;
                    var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    sDestination = filepath + "/" + filename;
                    log.LogInformation("Original abs file path=" + blobPath);
                    log.LogInformation("destination file path=" + sDestination);
                    blobContinuationToken = null;

                }



            } while (blobContinuationToken != null); // Loop while the continuation token is not null.

            return (content, sDestination);

            //return $"{content}";
        }

        [FunctionName("ACPLogCustomFunction_ToCSVString")]
        public static string UpdateContent([ActivityTrigger] string content, ILogger log)
        {

            
            var sContent =  QuickConversionACPLog(content);
            Console.WriteLine(sContent + "= \n" + sContent);
            return sContent;
        }
        private static string QuickConversionACPLog(string content){
            System.Text.StringBuilder csv = new System.Text.StringBuilder();
            using (var logData = ChoJSONReader<log_data>.LoadText(content)
            
            )
            {
            using (var w = new ChoCSVWriter(csv)
            .WithDelimiter(",")
            .WithFirstLineHeader()
            .Configure(c => c.IgnoreDictionaryFieldPrefix = true)
            )
            {
            w.Write(logData.SelectMany(root =>
                root.log   
                .Select(log => new
                {
                    state=log.state,
                    searchCount=log.searchCount,
                    plsyncStick=log.plsyncStick,
                    mispointDetected=log.mispointDetected,
                    patternCommandRx_lpa = log.patternCommandRx.lpa,
                    patternCommandRx_phi = log.patternCommandRx.phi,
                    patternCommandRx_theta = log.patternCommandRx.theta,
                    patternCommandRx_timestamps_pointing = log.patternCommandRx.timestamps.pointing,
                    patternCommandTx_lpa = log.patternCommandTx.lpa,
                    patternCommandTx_phi = log.patternCommandTx.phi,
                    patternCommandTx_theta = log.patternCommandTx.theta,
                    patternCommandTx_timestamps_pointing = log.patternCommandTx.timestamps.pointing,                   
                    sensors =  log.sensors.IsNull()? "": "[" + String.Join(", ",log.sensors) + "]",
                    ekfData =  log.ekfData.IsNull()? "": "[" + String.Join(", ",log.ekfData) + "]",
                    primaryMetric = log.primaryMetric,
                    secondaryMetric = log.secondaryMetric,
                    plEsno = log.plEsno,
                    plSync = log.plSync,
                    carrierFrequencyOffsetHz = log.carrierFrequencyOffsetHz,
                    pointingAccurate = log.pointingAccurate,
                    rxLock = log.rxLock,
                    wrongSatellite = log.wrongSatellite,
                    gpsTime = log.gpsTime,
                    gpsLat = log.gpsLat,
                    gpsLon = log.gpsLon,
                    gpsAlt = log.gpsAlt,
                    gpsQuality = log.gpsQuality,
                    gpsFix = log.gpsFix,
                    gpsSpeed = log.gpsSpeed,
                    gpsDirection = log.gpsDirection,
                    dwell = log.dwell,
                    trkState = log.trkState,
                    gyroBias1 = log.gyroBias1,
                    gyroBias2 = log.gyroBias2,
                    gyroBias3 = log.gyroBias3,
                    PSq1 = log.PSq1,
                    PSq2 = log.PSq2,
                    PSq3 = log.PSq3,
                    PSq4 = log.PSq4,
                    Version=root.version,
                }

                )
                  
            )
            ); 
            }
            }
            //Console.WriteLine(csv.ToString());
            return csv.ToString();
        }

        



        [FunctionName("ACPLogCustomFunction_UploadFile")]
        public static async Task<string> UploadToBlob([ActivityTrigger] IDurableActivityContext inputs, ILogger log)
        {
            (string container, string sDestination, string content) fileInfo = inputs.GetInput<(string, string, string)>();

            log.LogInformation($"container {fileInfo.container}.");
            log.LogInformation($"sDestination {fileInfo.sDestination}.");


            CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(BigDataStorage);
            CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(fileInfo.container);

            CloudBlockBlob cloudBlockBlob = rawContainer.GetBlockBlobReference(fileInfo.sDestination);

            await cloudBlockBlob.UploadTextAsync(fileInfo.content);

            
            return "OK";

        }
    }


    public static class Function4
    {
        static String BigDataStorage = "DefaultEndpointsProtocol=https;AccountName=kymetabigdata;AccountKey=HcmXJ6Ks9sV44ef5JhFtIaAEIgL+EhIF8n1wG/an8wyyVuKe/CbPNrSsQuCN7dKy/f6he4cEBi6JGYuGT6//IA==;EndpointSuffix=core.windows.net";
        [FunctionName("PT_ACPLogCustomFunction_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.

            IEnumerable<string> headerValues;
            var sourceFolder = string.Empty;
            var container = string.Empty;
            var soureFile = string.Empty;
            var rawzonestaging = string.Empty;

            if (req.Headers.TryGetValues("SourceFolder", out headerValues))
            {
                sourceFolder = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("SoureFile", out headerValues))
            {
                soureFile = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("rawzonecontainer", out headerValues))
            {
                container = headerValues.FirstOrDefault();
            }
            if (req.Headers.TryGetValues("rawzonestaging", out headerValues))
            {
                rawzonestaging = headerValues.FirstOrDefault();
            }

            string body = await req.Content.ReadAsStringAsync();

            sourceFolder = sourceFolder.Substring(sourceFolder.IndexOf("/") + 1);
            log.LogInformation("rawzonecontainer=" + container);
            log.LogInformation("SourceFolder=" + sourceFolder);
            log.LogInformation("SoureFile=" + soureFile);
            log.LogInformation("rawzonestaging=" + rawzonestaging);
            log.LogInformation("Body=" + body);

            string instanceId = await starter.StartNewAsync("PT_ACPLogCustomFunction_DurableFunction", Guid.NewGuid().ToString(), (container, sourceFolder, soureFile, rawzonestaging));

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);

        }

        [FunctionName("PT_ACPLogCustomFunction_DurableFunction")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context, ILogger log)
        {
            


            var outputs = new List<string>();
            (string container, string sourceFolder, string soureFile, string rawzonestaging) fileInfo = context.GetInput<(string, string, string, string)>();




            (string content, string sDestination) obContent = await context.CallActivityAsync<(string, string)>("PT_ACPLogCustomFunction_SearchAzureStorage", fileInfo);
            //log.LogInformation($"content {obContent.content}.");
            log.LogInformation($"sDestination {obContent.sDestination}.");

            var updateContent = await context.CallActivityAsync<string>("PT_ACPLogCustomFunction_ToCSVString", obContent.content);
            log.LogInformation("updated Content is returned.");

            string status = await context.CallActivityAsync<string>("PT_ACPLogCustomFunction_UploadFile", (fileInfo.rawzonestaging, obContent.sDestination, updateContent));
            log.LogInformation("updated Content uploaded.");

            //log.LogInformation($"updateContent {updateContent}.");
            // returns ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]
            return outputs;
        }

        [FunctionName("PT_ACPLogCustomFunction_SearchAzureStorage")]
        public static async Task<(string, string)> getContent([ActivityTrigger] IDurableActivityContext inputs, ILogger log)
        {
            (string container, string sourceFolder, string soureFile, string rawzonestaging) fileInfo = inputs.GetInput<(string, string, string, string)>();

            log.LogInformation($"container {fileInfo.container}.");
            log.LogInformation($"sourceFolder {fileInfo.sourceFolder}.");
            log.LogInformation($"soureFile {fileInfo.soureFile}.");
            log.LogInformation($"rawzonestaging {fileInfo.rawzonestaging}.");
            CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(BigDataStorage);
            CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(fileInfo.container);

            BlobContinuationToken blobContinuationToken = null;
            var content = "";
            var sDestination = "";
            do
            {
                var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                    prefix: fileInfo.sourceFolder,
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.All,
                    maxResults: null,
                    currentToken: blobContinuationToken,
                    options: null,
                    operationContext: null
                );


                // Get the value of the continuation token returned by the listing call.

                blobContinuationToken = resultSegment.ContinuationToken;

                var blobs = resultSegment.Results.Where(b => (Path.GetFileName(b.Uri.AbsolutePath).EndsWith(fileInfo.soureFile))); //&& (Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("PCDMIS"))); Path.GetExtension(b.Uri.AbsolutePath).Equals(".CSV") && !Path.GetDirectoryName(b.Uri.AbsolutePath).Contains("OLD")
                log.LogInformation("blob count total for PT ACPLog json files=" + blobs.Count());
                foreach (var blob in blobs)
                {
                    var block = blob as CloudBlockBlob;


                    content = await block.DownloadTextAsync();

                    var blobPath = block.Name;
                    var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    sDestination = filepath + "/" + filename;
                    log.LogInformation("Original abs file path=" + blobPath);
                    log.LogInformation("destination file path=" + sDestination);
                    blobContinuationToken = null;

                }



            } while (blobContinuationToken != null); // Loop while the continuation token is not null.

            return (content, sDestination);

            //return $"{content}";
        }

        [FunctionName("PT_ACPLogCustomFunction_ToCSVString")]
        public static string UpdateContent([ActivityTrigger] string content, ILogger log)
        {

            content = content.Replace("\r\n",",");
            content = "{\"log\":[" + content;
            content = content.Substring(0,content.Length-1) + "],\"version\":\"2\"}";
            //Console.WriteLine("content" + "= \n" + content);
            var sContent =  QuickConversionACPLog(content);
            //Console.WriteLine("sContent" + "= \n" + sContent);

            return sContent;
        }
        private static string QuickConversionACPLog(string content){
            System.Text.StringBuilder csv = new System.Text.StringBuilder();
            using (var logData = ChoJSONReader<log_data>.LoadText(content)
            
            )
            {
            using (var w = new ChoCSVWriter(csv)
            .WithDelimiter(",")
            .WithFirstLineHeader()
            .Configure(c => c.IgnoreDictionaryFieldPrefix = true)
            )
            {
            w.Write(logData.SelectMany(root =>
                root.log   
                .Select(log => new
                {
                                      
                    state=log.state,
                    searchCount=log.searchCount,
                    plsyncStick=log.plsyncStick,
                    mispointDetected=log.mispointDetected,
                    slsReacqThresh = log.slsReacqThresh,
                    sdDetected = log.sdDetected,
                    carrierLockState = log.openamip.IsNull()? 0:log.openamip.carrierLockState,
                    dataCnr = log.openamip.IsNull()? 0:log.openamip.dataCnr,
                    headerCnr = log.openamip.IsNull()? 0:log.openamip.headerCnr,
                    compPower = log.openamip.IsNull()? 0:log.openamip.compPower,
                    patternCommandRx_lpa = log.patternCommandRx.IsNull()? 0:log.patternCommandRx.lpa,
                    patternCommandRx_phi = log.patternCommandRx.IsNull()? 0:log.patternCommandRx.phi,
                    patternCommandRx_theta = log.patternCommandRx.IsNull()? 0:log.patternCommandRx.theta,
                    patternCommandRx_timestamps_pointing = log.patternCommandRx.IsNull()? 0:log.patternCommandRx.timestamps.pointing,
                    patternCommandTx_lpa = log.patternCommandTx.IsNull()? 0:log.patternCommandTx.lpa,
                    patternCommandTx_phi = log.patternCommandTx.IsNull()? 0:log.patternCommandTx.phi,
                    patternCommandTx_theta = log.patternCommandTx.IsNull()? 0:log.patternCommandTx.theta,
                    patternCommandTx_timestamps_pointing = log.patternCommandTx.IsNull()? 0:log.patternCommandTx.timestamps.pointing,                   
                    sensors =  log.sensors.IsNull()? "": "[" + String.Join(", ",log.sensors) + "]",
                    ekfData =  log.ekfData.IsNull()? "": "[" + String.Join(", ",log.ekfData) + "]",
                    primaryMetric = log.primaryMetric,
                    secondaryMetric = log.secondaryMetric,
                    plEsno = log.plEsno,
                    plSync = log.plSync,
                    carrierFrequencyOffsetHz = log.carrierFrequencyOffsetHz,
                    pointingAccurate = log.pointingAccurate,
                    rxLock = log.rxLock,
                    wrongSatellite = log.wrongSatellite,
                    gpsTime = log.gpsTime,
                    gpsLat = log.gpsLat,
                    gpsLon = log.gpsLon,
                    gpsAlt = log.gpsAlt,
                    gpsQuality = log.gpsQuality,
                    gpsFix = log.gpsFix,
                    gpsSpeed = log.gpsSpeed,
                    gpsDirection = log.gpsDirection,
                    dwell = log.dwell,
                    trkState = log.trkState,
                    gyroBias1 = log.gyroBias1,
                    gyroBias2 = log.gyroBias2,
                    gyroBias3 = log.gyroBias3,
                    PSq1 = log.PSq1,
                    PSq2 = log.PSq2,
                    PSq3 = log.PSq3,
                    PSq4 = log.PSq4,
                    Version=root.version,
                }

                )
                  
            )
            ); 
            }
            }
            //Console.WriteLine(csv.ToString());
            return csv.ToString();
        }

        



        [FunctionName("PT_ACPLogCustomFunction_UploadFile")]
        public static async Task<string> UploadToBlob([ActivityTrigger] IDurableActivityContext inputs, ILogger log)
        {
            (string container, string sDestination, string content) fileInfo = inputs.GetInput<(string, string, string)>();

            log.LogInformation($"container {fileInfo.container}.");
            log.LogInformation($"sDestination {fileInfo.sDestination}.");


            CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(BigDataStorage);
            CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(fileInfo.container);

            CloudBlockBlob cloudBlockBlob = rawContainer.GetBlockBlobReference(fileInfo.sDestination);

            await cloudBlockBlob.UploadTextAsync(fileInfo.content);

            
            return "OK";

        }
    }



}