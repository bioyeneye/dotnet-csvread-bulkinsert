using System;
using System.Data;
using System.Data.SqlClient;

namespace ReadCSVDbBulkInsert
{
    class Program
    {
        static void Main(string[] args)
        {
            var data = CsvData.LoadCsvData("data.csv");
            
            string connectionString = @"Data Source = MyServerName/Instance; Integrated Security=true; Initial Catalog=YourDatabase";
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                {
                    foreach (DataColumn c in data.Columns)
                        bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);
 
                    bulkCopy.DestinationTableName = data.TableName;
                    try
                    {
                        bulkCopy.WriteToServer(data.CreateDataReader());
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            }

            var dataRows =  data.Rows;
            Console.WriteLine();
            Console.WriteLine("Hello World!");
        }
    }
}