using System;
using System.IO;
using System.Net.Http;
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
using StackExchange.Redis;

namespace KITE_PROD_API
{
    public static class Function1
    {
        [FunctionName("MonitorStreamingJobs")]
        public static async Task RunAsync([TimerTrigger("0 0/5 * * * *")]TimerInfo myTimer, ILogger log)
        {
            try
            {
                HttpClient client = new HttpClient();
                var runDict = new Dictionary<int, Boolean>();
                var resultActiveJobList = await GetActiveJobList(client);
                if (!resultActiveJobList.IsSuccessStatusCode)
                {
                    // Remotes Service Error
                    Console.WriteLine("Get active job list failed.");
                }
                if (resultActiveJobList.DeserializedContent == null)
                {
                    // Status Code 204, Empty Payload, Remote Id Not Found
                    Console.WriteLine("Status Code 204, Empty Payload, job list Not Found");
                }
                else if (resultActiveJobList.DeserializedContent.runs != null)
                {
                    foreach (var run in resultActiveJobList.DeserializedContent.runs)
                    {
                        runDict.Add(run.job_id, run.state.life_cycle_state.Equals("RUNNING"));
                    }
                }

                client.Dispose();


                IConnectionMultiplexer redisObj = ConnectionMultiplexer.Connect(Environment.GetEnvironmentVariable("RedisConnectionString"));
                IDatabase db = redisObj.GetDatabase(db: 0);

                var hashObj = await db.HashGetAllAsync("Databrick:ListOfJobs");

                var updates = new List<HashEntry>();

                if (hashObj == null || hashObj.Length == 0)
                {
                    Console.WriteLine("Databrick:ListOfJobs Not Found in redis");
                    await populateRedisWithListOfJobs(db);
                }

                if (hashObj != null && hashObj.Length > 0)
                {
                    foreach (var entry in hashObj)
                    {
                        var updateState = new JobQueryData();
                        var Key = entry.Name;
                        var desriedStateFlag = false;
                        var currentStateFlag = true;
                        var message = "";
                        var result = JsonConvert.DeserializeObject<List<JobQueryData>>(entry.Value.ToString());
                        foreach (var r in result)
                        {

                            if (r.DesiredState.Equals("ON") && (!runDict.ContainsKey(r.Id) || !runDict[r.Id]))
                            {
                                currentStateFlag = currentStateFlag && false;
                                message = message + Key + "-" + r.Name + " pipeline is down;";
                            }


                            if (r.DesiredState.Equals("ON"))
                                desriedStateFlag = desriedStateFlag || true;
                        }
                        updateState.Name = entry.Name;
                        updateState.Id = (int)Enum.Parse(typeof(Pipelines), entry.Name);
                        updateState.DesiredState = (desriedStateFlag) ? "ON" : "OFF";
                        updateState.CurrentState = (currentStateFlag && desriedStateFlag) ? "ON" : "OFF";
                        updateState.Message = (!currentStateFlag) ? message : null;
                        updates.Add(new HashEntry(Key, JsonConvert.SerializeObject(updateState)));
                        Console.WriteLine(entry.Name + ": Added to the Hash entry");
                    }
                    if (updates.Any())
                    {
                        await db.HashSetAsync("Databrick:CurrentPipelineState", updates.ToArray());
                        Console.WriteLine("Updated pipeline states into Redis");
                    }
                }

                // return new OkResult();
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                // return new BadRequestObjectResult(ex);
            }

        }

        public static async Task<Response<DatabrickJobListModel>> GetActiveJobList(HttpClient client)
        {
            client.BaseAddress = new Uri(Environment.GetEnvironmentVariable("DatabrickAPI"));
            client.DefaultRequestHeaders.Add("Authorization", "Bearer " + Environment.GetEnvironmentVariable("DatabrickToken"));
            client.Timeout = TimeSpan.FromMinutes(30);
            client.DefaultRequestHeaders.Add("Connection", "keep-alive");
            client.DefaultRequestHeaders.Add("Keep-Alive", "1800");
            var activeflag = "true";
            var offset = "0";
            var limit = "0";
            var response = await client.GetAsync($"2.0/jobs/runs/list?active_only={activeflag}&offset={offset}&limit={limit}");
            return new Response<DatabrickJobListModel>(response);
        }
        public static async Task populateRedisWithListOfJobs(IDatabase db)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("BigDataStorage"));
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("DataContainer"));

            var list = await container.ListBlobsSegmentedAsync("listofjobs", true, BlobListingDetails.All, null, null, null, null);
            //Loading list of job json files
            var blobs = list.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".json")));
            var inserts = new List<HashEntry>();
            foreach (var blob in blobs)
            {

                var block = blob as CloudBlockBlob;

                var content = await block.DownloadTextAsync();

                var result = JsonConvert.DeserializeObject<List<JobListJson>>(content.ToString());
                if (result != null && result.Count > 0)
                {
                    Console.WriteLine("JSON de-serialized");
                    foreach (var entry in result)
                    {
                        //var updateState = new JobQueryData();
                        var Key = entry.Key;
                        var jobList = entry.Data;
                        inserts.Add(new HashEntry(Key, JsonConvert.SerializeObject(jobList)));
                    }
                    if (inserts.Any())
                    {
                        await db.HashSetAsync("Databrick:ListOfJobs", inserts.ToArray());
                        Console.WriteLine("Updated pipeline list of jobs into Redis");
                    }

                }
            }
        }
    }
}
