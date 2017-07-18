using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;

namespace ST_Project
{
    class Helper
    {
        public static DataTable GetDataTabletFromCSVFile(string csv_file_path, string Ticker_Symbol)
        {
            DataTable csvData = new DataTable();

            try
            {
                using (StreamReader sr = new StreamReader(csv_file_path))
                {
                    string Line;
                    DataColumn datecolumn;
                    string[] colFields = sr.ReadLine().Split(',');
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

                        string[] fieldData_new = new string[Field_CNT + 1];
                        for (int runs = 0; runs < Field_CNT+ 1; runs++)
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
            catch (Exception ex)
            {
                return null;
            }
            return csvData;
        }


        public static void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable csvFileData)
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
        }


    }
}

