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


            using (SqlConnection connection = new SqlConnection(Config.ConnStr))
            {
                connection.Open();
                SqlCommand cmd = new SqlCommand("select Ticker_Symbol from SP500_List", connection);
                SqlDataReader Symbol_Reader = cmd.ExecuteReader();

                while (Symbol_Reader.Read())
                {
                    string Ticker_Symbol = Symbol_Reader["Ticker_Symbol"].ToString();
                    string FileName = Ticker_Symbol + "_" +DateTime.Now.ToString("yyyyMMdd_HHmmss.ffffff") + ".csv";
                    string FileDirectory = Path.Combine(Config.DataFolder, FileName);                  
                    string Json_Str = Test_Http_Connection(URL_Config("TIME_SERIES_INTRADAY", Ticker_Symbol, "1min", "full")).Result;
                    using (StreamWriter writer = new StreamWriter(FileDirectory))
                    {
                        writer.Write(Json_Str);
                    }

                    Console.WriteLine(DateTime.Now.ToString("yyyyMMdd_HHmmss.ffffff") + ": " +Ticker_Symbol);
                    DataTable csvData = Helper.GetDataTabletFromCSVFile(FileDirectory, Ticker_Symbol);
                    Helper.InsertDataIntoSQLServerUsingSQLBulkCopy(csvData);
                }

            }

            Console.ReadLine();

            //string FilePath = @"C:\Users\Hui\Documents\Visual Studio 2015\Projects\ST_Project\ST_Project\ST_Project\data\new.txt";
            //DataTable csvData = Helper.GetDataTabletFromCSVFile(FilePath);

            //Helper.InsertDataIntoSQLServerUsingSQLBulkCopy(csvData);

        }

        static async Task<string> Test_Http_Connection(string URL)
        {
            HttpClient client = new HttpClient();
            var result = await client.GetAsync(URL);
            MemoryStream memoryStream = new MemoryStream();
            result.Content.CopyToAsync(memoryStream).Wait();
            memoryStream.Position = 0;
            var sr = new StreamReader(memoryStream);
            var myStr = sr.ReadToEnd();

            return myStr;
        }

        static string URL_Config(string function, string symbol, string interval, string outputsize)
        {
            string apikey = Config.apikey;
            string DataType = Config.dataType;
            string URL = string.Format("https://www.alphavantage.co/query?function={0}&outputsize={1}&symbol={2}&interval={3}&apikey={4}&datatype={5}", function, outputsize, symbol, interval, apikey, DataType);
            return URL;
        }
    }
}
