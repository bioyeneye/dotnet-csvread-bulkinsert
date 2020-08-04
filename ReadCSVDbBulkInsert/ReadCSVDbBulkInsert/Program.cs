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
            
            
            Console.WriteLine();
            Console.WriteLine("Hello World!");
        }
    }
}