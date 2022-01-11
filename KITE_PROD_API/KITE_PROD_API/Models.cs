using System;
using System.Collections.Generic;
using System.Text;

namespace KITE_PROD_API
{
    enum RunStates
    {
        PENDING,
        RUNNING,
        TERMINATED
    }
    enum Pipelines
    {
        GOLD = 1,
        ASM,
        EVO,
        PEPLINK,
        CUBIC,
        HUBSTATS,
        HUBUSAGE,
        HUBSTATUS,
        INTELSAT
    }
    class JobListJson
    {
        public string Key { get; set; }
        public List<JobQueryData> Data { get; set; }
    }
    class JobQueryData
    {
        public string Name { get; set; }
        public int Id { get; set; }
        public string DesiredState { get; set; }
        public string CurrentState { get; set; }
        public string Message { get; set; }
    }

    public class RunState
    {
        public string life_cycle_state { get; set; }
        public string state_message { get; set; }
    }
    public class DatabrickRunListModel
    {
        public int job_id { get; set; }
        public int run_id { get; set; }
        public string run_name { get; set; }
        public long start_time { get; set; }
        public RunState state { get; set; }

    }
    public class DatabrickJobListModel
    {

        public List<DatabrickRunListModel> runs { get; set; }
        public Boolean has_more { get; set; }

    }

}
