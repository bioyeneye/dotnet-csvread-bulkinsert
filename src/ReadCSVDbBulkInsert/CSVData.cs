using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;

namespace ReadCSVDbBulkInsert
{
    public class CsvData
    {
        private string _fileSavePath;
        public CsvData(string fileSavePath)
        {
            _fileSavePath = fileSavePath;
        }
        
        // public static IEnumerable<DataRow> GetData(string fileSavePath)
        // {
        //     var csvTable = new DataTable();
        //     var stream = File.Open(fileSavePath, FileMode.Open, FileAccess.Read);
        //     using (var csvReader = new CsvReader(new StreamReader(stream), true, ',', '"', '"', '#', LumenWorks.Framework.IO.Csv.ValueTrimmingOptions.All))
        //     {
        //         csvReader.MissingFieldAction = MissingFieldAction.ParseError;
        //         csvReader.DefaultParseErrorAction = ParseErrorAction.RaiseEvent;
        //         csvReader.SkipEmptyLines = true;
        //         
        //         csvTable.Load(csvReader);
        //         var rows = from DataRow a in csvTable.Rows select a;
        //         return rows;
        //     }
        // }
        
        public static DataTable LoadCsvData(string refPath)
        {
            var result = new DataTable();
            using (var sr = new StreamReader(refPath, Encoding.UTF8, false, 16384 * 2))
            {
                using (var csv = new CsvReader(sr, CultureInfo.InvariantCulture))
                {
                    csv.Configuration.Delimiter = ",";
                    csv.Configuration.HasHeaderRecord = true;
                    csv.Configuration.IgnoreQuotes = true;
                    using (var dataRdr = new CsvDataReader(csv))
                    {
                        result.Load(dataRdr);
                    }
                }
            }
            return result;
        }
    }
}

/*
 * using (var csv = new CsvReader(sr, CultureInfo.InvariantCulture))
                {
                    csv.Configuration.Delimiter = ",";
                    csv.Configuration.HasHeaderRecord = true;
                    csv.Configuration.IgnoreQuotes = true;
                    result.Load(csv.);
                }
 */