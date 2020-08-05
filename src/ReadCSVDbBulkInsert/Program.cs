using System;

namespace ReadCSVDbBulkInsert
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = @"Data Source = localhost; Initial Catalog=csvdb; User ID=sa;Password=Password1@";
            var data = CsvHelper.LoadCsvData("data.csv");

            CsvHelper.SqlBulkInsertWithAutoTableCreate(data, connectionString, "Endpoints");
            Console.WriteLine();
            Console.WriteLine("Hello World!");
        }

        
    }
}