using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ST_Project
{
    class Quote_Unit
    {
        public string Quote_Time { get; set; }
        public string open { get; set; }
        public string high { get; set; }
        public string low { get; set; }
        public string close { get; set; }
        public string volumn { get; set; }
    }

    class Test_2_Unite
    {
        [JsonProperty("Meta Data")]
        public string MetaData { get; set; }

        [JsonProperty("Time Series(1min)")]
        public string TimeSeries { get; set; }
    }
}
