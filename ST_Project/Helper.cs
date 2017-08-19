using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;
using System.Net.Http;

namespace ST_Project
{
    class Helper
    {

        //public static MultiThread_Processing()
        //{


        //}
        public static DataTable GetDataTabletFromCSVFile(string csv_file_path, string Ticker_Symbol, string MaxDateTime)
        {
            DataTable csvData = new DataTable();

            try
            {
                using (StreamReader sr = new StreamReader(csv_file_path))
                {
                    string Line;
                    DataColumn datecolumn;
                    string[] colFields = sr.ReadLine().Split(',');  // the column name is in the first line
                    foreach (string column in colFields)
                    {
                        datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvData.Columns.Add(datecolumn);
                    }

                    datecolumn = new DataColumn("Ticker_Symbol"); // add ID column
                    datecolumn.AllowDBNull = true;
                    csvData.Columns.Add(datecolumn);

                    while ((Line = sr.ReadLine()) != null)
                    {
                        string[] fieldData = Line.Split(',');
                        int Field_CNT = fieldData.Length;

                        if (Convert.ToDateTime(fieldData[0]) > Convert.ToDateTime(MaxDateTime)) // only add new data into datatable
                        {
                            string[] fieldData_new = new string[Field_CNT + 1]; // we use Field_CNT + 1 since we need to add Ticker_Symbol to datatable also 
                            for (int runs = 0; runs < Field_CNT + 1; runs++)
                            {
                                if (runs != Field_CNT)
                                    fieldData_new[runs] = fieldData[runs];
                                else
                                    fieldData_new[runs] = Ticker_Symbol;
                            }

                            csvData.Rows.Add(fieldData_new);
                        }          
                    }
                }
            }
            catch (Exception ex)
            {
                return null;
            }
            return csvData;
        }


        public static void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable csvFileData, string _Ticker_Symbol)
        {
            if (csvFileData!= null)
            {
                using (SqlConnection dbConnection = new SqlConnection(Config.ConnStr))
                {
                    dbConnection.Open();
                    using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
                    {
                        s.DestinationTableName = "Intraday_log";
                        foreach (var column in csvFileData.Columns)
                            s.ColumnMappings.Add(column.ToString(), Constant_Parameters.Col_Name_Lkup[column.ToString()]);
                        s.WriteToServer(csvFileData);
                    }
                }
                Helper.Logging("Loaded " + csvFileData.Rows.Count.ToString() + " records into table for "+ _Ticker_Symbol);

            }
            else
                Helper.Logging("Loaded 0" + " record into table");

        }


        public static async Task<string> Test_Http_Connection(string URL)
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

        public static string URL_Config(string function, string symbol, string interval, string outputsize)
        {
            string apikey = Config.apikey;
            string DataType = Config.dataType;
            string URL = string.Format("https://www.alphavantage.co/query?function={0}&outputsize={1}&symbol={2}&interval={3}&apikey={4}&datatype={5}", function, outputsize, symbol, interval, apikey, DataType);
            return URL;
        }

        public static void Logging(string Msg)
        {
            lock (Config.LogFolder)
            {
                string path = Path.Combine(Config.LogFolder, Config.RunTime + ".log");
                if (!File.Exists(path))
                {
                    // Create a file to write to.
                    using (StreamWriter sw = File.CreateText(path))
                    {
                        sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + ": " + Msg);
                    }

                }
                else
                {
                    using (StreamWriter sw = File.AppendText(path))
                    {
                        sw.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff") + ": " + Msg);

                    }
                }
            }
            

        }

    }
}

