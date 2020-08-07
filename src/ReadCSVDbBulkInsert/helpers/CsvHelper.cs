using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;

namespace ReadCSVDbBulkInsert
{
    public class CsvHelper
    {
        public static DataTable LoadCsvDataNoLibrary(string refPath)
        {
            DataTable csvData = new DataTable();
            try
            {
                string[] seps = {"\",", ",\""};
                char[] quotes = {'\"', ' '};
                string[] colFields = null;
                foreach (var line in File.ReadLines(refPath))
                {
                    var fields = line
                        .Split(seps, StringSplitOptions.None)
                        .Select(s => s.Trim(quotes).Replace("\\\"", "\""))
                        .ToArray();

                    if (colFields == null)
                    {
                        colFields = fields;
                        foreach (string column in colFields)
                        {
                            DataColumn datacolumn = new DataColumn(column);
                            datacolumn.AllowDBNull = true;
                            csvData.Columns.Add(datacolumn);
                        }
                    }
                    else
                    {
                        for (int i = 0; i < fields.Length; i++)
                        {
                            if (fields[i] == "")
                            {
                                fields[i] = null;
                            }
                        }

                        csvData.Rows.Add(fields);
                    }
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
            return csvData;
        }

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

                        if (csv.Configuration.HasHeaderRecord)
                        {
                            csv.ReadHeader();

                            foreach (var header in csv.Context.HeaderRecord)
                            {
                                var headerWithoutQuotes = header.Trim('"');
                                result.Columns.Add(headerWithoutQuotes, typeof(string));
                            }
                        }

                        while (csv.Read())
                        {
                            var row = result.NewRow();
                            foreach (DataColumn column in result.Columns)
                            {
                                var data = csv.GetField(column.DataType, $"\"{column.ColumnName}\"");
                                row[column.ColumnName] = data.ToString()?.Trim('"');
                            }

                            result.Rows.Add(row);
                        }
                    }
                }
            }

            return result;
        }

            public static void SqlBulkInsertWithAutoTableCreate(DataTable dataTable, string connectionString, string tableName)
            {
                using (var sqlConnection = new SqlConnection(connectionString))
                {
                    sqlConnection.Open();
                    var transaction = sqlConnection.BeginTransaction();

                    try
                    {
                        dataTable.TableName = tableName;

                        // checking whether the table selected from the dataset exists in the database or not
                        var checkTableIfExistsCommand = new SqlCommand("IF EXISTS (SELECT 1 FROM sysobjects WHERE name =  '" + dataTable.TableName + "') SELECT 1 ELSE SELECT 0", sqlConnection, transaction);
                        var exists = checkTableIfExistsCommand.ExecuteScalar().ToString().Equals("1");

                        // if does not exist
                        if (!exists)
                        {
                            var createTableBuilder = new StringBuilder("CREATE TABLE [" + dataTable.TableName + "]");
                            createTableBuilder.AppendLine("(");

                            createTableBuilder.AppendLine($"{tableName}Id bigint IDENTITY(1,1),");

                            // selecting each column of the datatable to create a table in the database
                            foreach (DataColumn dc in dataTable.Columns)
                            {
                                string dataType;
                                switch (dc.DataType.Name)
                                {
                                    case "String":
                                        dataType = " nvarchar(MAX) ";
                                        break;
                                    case "DateTime":
                                        dataType = " nvarchar(MAX) ";
                                        break;
                                    case "Boolean":
                                        dataType = " bit ";
                                        break;
                                    case "Int32":
                                        dataType = " int ";
                                        break;
                                    case "Byte[]":
                                        dataType = " varbinary(8000) ";
                                        break;
                                    default:
                                        dataType = " nvarchar(MAX) ";
                                        break;
                                }

                                createTableBuilder.AppendLine($"  [{dc.ColumnName}] {dataType},");
                            }

                            createTableBuilder.Remove(createTableBuilder.Length - 1, 1);
                            createTableBuilder.AppendLine(")");

                            var createTableCommand = new SqlCommand(createTableBuilder.ToString(), sqlConnection, transaction);
                            createTableCommand.ExecuteNonQuery();
                        }

                        // if table exists, just copy the data to the destination table in the database
                        // copying the data from datatable to database table
                        using (var bulkCopy = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, transaction))
                        {
                            bulkCopy.DestinationTableName = dataTable.TableName;
                            bulkCopy.BatchSize = 100;
                            bulkCopy.WriteToServer(dataTable.CreateDataReader());
                        }

                        transaction.Commit();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        transaction.Rollback();
                    }
                    finally
                    {
                        sqlConnection.Close();
                    }
                }
            }
        }
    }
