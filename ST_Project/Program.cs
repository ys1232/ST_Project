using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Threading;
//using System.Web.Script.Serialization;

namespace ST_Project
{
    class Program
    {
        static void Main(string[] args)
        {
            #region Parsing_json_array_test
            /**
            //using (SqlConnection connection = new SqlConnection("Data Source = HUICHEN;Integrated Security=true;Initial Catalog=Hui"))
            //{
            //    connection.Open();
            //    SqlCommand cmd = new SqlCommand("select count(1) from dbo.student", connection);
            //    object CNT = cmd.ExecuteScalar();
            //    Console.WriteLine("The count is:{0}", CNT.ToString());
            //    Console.ReadKey();
            //}

            //string Json_Str = Test_Http_Connection(URL_Config("TIME_SERIES_INTRADAY", "ZBH", "15min", "full")).Result;
            //Console.WriteLine(Json_Str);
            //Console.ReadLine();


            string json_sample =
               @"{
                    ""Meta Data"": {
                        ""1. Information"": ""Intraday (1min) prices and volumes"",
                        ""2. Symbol"": ""NFLX"",
                        ""3. Last Refreshed"": ""2017-07-14 16:00:00"",
                        ""4. Interval"": ""1min"",
                        ""5. Output Size"": ""Full size"",
                        ""6. Time Zone"": ""US/Eastern""
                    },
                    ""Time Series (1min)"": {
                        ""2017-07-14 16:00:00"": {
                            ""1. open"": ""161.1800"",
                            ""2. high"": ""161.2600"",
                            ""3. low"": ""161.1000"",
                            ""4. close"": ""161.1200"",
                            ""5. volume"": ""276809""
                        },
                        ""2017-07-14 15:59:00"": {
                            ""1. open"": ""161.2800"",
                            ""2. high"": ""161.3000"",
                            ""3. low"": ""161.1800"",
                            ""4. close"": ""161.2000"",
                            ""5. volume"": ""61322""
                        },
                        ""2017-07-14 15:58:00"": {
                            ""1. open"": ""161.2500"",
                            ""2. high"": ""161.3500"",
                            ""3. low"": ""161.2400"",
                            ""4. close"": ""161.2900"",
                            ""5. volume"": ""55874""
                        }
	                }
                }";

            Test_2_Unite user = JsonConvert.DeserializeObject<Test_2_Unite>(json_sample);
            Console.WriteLine(user.MetaData);

            //JObject rss = JObject.Parse(json_sample);


            //string Json_2 = "[{Name:'John Simith',Age:35},{Name:'Pablo Perez',Age:34}]"; //,{Name:'Pablo Perez',Age:34}}
            //List<Person> user = JsonConvert.DeserializeObject<List<Person>>(Json_2);
            ////JavaScriptSerializer js = new JavaScriptSerializer();
            ////Person[] persons = js.DeserializeObject<Person[]>(Json_2);

            //foreach(Person p in user)
            //Console.WriteLine(p.Name);
            //Console.ReadLine();

            **/
            #endregion

            #region write to csv file
            //string Json_Str = Test_Http_Connection(URL_Config("TIME_SERIES_INTRADAY", "ZBH", "15min", "full")).Result;

            //string FileName = DateTime.Now.ToString("yyyyMMdd_HHmmss.ffffff") + ".csv";

            //using (StreamWriter writer = new StreamWriter(Path.Combine(Config.DataFolder, FileName)))
            //{
            //    writer.Write(Json_Str);
            //}
            #endregion 

    
            Helper.Logging("program started");

            string select_query = "select Ticker_Symbol from SP500_List";
            string check_query = "select Ticker_Symbol from dbo.Intraday_log group by Ticker_Symbol having max(new_timestamp) < (select max(new_timestamp) from dbo.Intraday_log)";

            Helper.Logging("initial_load...");
            LoadData(select_query);
            Helper.Logging("1st time check missing records");
            LoadData(check_query);
            Helper.Logging("2nd time check missing records");
            LoadData(check_query);

            Console.ReadLine();

            
        }

        public static void LoadData(string Query)
        {

            using (SqlConnection connection = new SqlConnection(Config.ConnStr))
            {
                connection.Open();
                SqlCommand cmd = new SqlCommand("select Ticker_Symbol from SP500_List", connection);
                /* check ticker_symbols with less data
                //SqlCommand cmd = new SqlCommand("select Ticker_Symbol from dbo.Intraday_log group by Ticker_Symbol having max(new_timestamp) < (select max(new_timestamp) from dbo.Intraday_log)", connection);
                */
                SqlDataReader Symbol_Reader = cmd.ExecuteReader();

                List<string> Ticker_Symbol_List = new List<string>();

                while (Symbol_Reader.Read())
                {
                    Ticker_Symbol_List.Add(Symbol_Reader["Ticker_Symbol"].ToString());

                }

                Parallel.ForEach(Ticker_Symbol_List, new ParallelOptions { MaxDegreeOfParallelism = Config.MaxThreads }, Ticker_Symbol =>
                {
                    Helper.Logging("Loading Stock " + Ticker_Symbol + "...");
                    string FileName = Ticker_Symbol + "_" + DateTime.Now.ToString("yyyyMMdd_HHmmss.ffffff") + ".csv";
                    string FileDirectory = Path.Combine(Config.DataFolder, FileName);
                    string Json_Str = Helper.Test_Http_Connection(Helper.URL_Config("TIME_SERIES_INTRADAY", Ticker_Symbol, "1min", "full")).Result;
                    using (StreamWriter writer = new StreamWriter(FileDirectory))
                    {
                        writer.Write(Json_Str);
                    }

                    Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + ": " + Ticker_Symbol);

                    SqlCommand cmd_2 = new SqlCommand("select max(new_timestamp) from dbo.Intraday_log where Ticker_Symbol = '" + Ticker_Symbol + "'", connection);
                    string MaxDateTime = cmd_2.ExecuteScalar().ToString();

                    DataTable csvData = Helper.GetDataTabletFromCSVFile(FileDirectory, Ticker_Symbol, MaxDateTime);
                    if (csvData == null)
                        Helper.Logging(Ticker_Symbol + ": csvData is empty");
                    else
                        Helper.InsertDataIntoSQLServerUsingSQLBulkCopy(csvData, Ticker_Symbol);
                });

                cmd = new SqlCommand("update  [dbo].[Intraday_log] set Loaded_DTM = '" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + "' where Loaded_DTM is null", connection);
                int updated_cnt = cmd.ExecuteNonQuery();

                Helper.Logging("Done," + updated_cnt.ToString() + " records have been inserted");
                Console.WriteLine("Done,{0} records have been inserted", updated_cnt);
            }
        }
    }
}
