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