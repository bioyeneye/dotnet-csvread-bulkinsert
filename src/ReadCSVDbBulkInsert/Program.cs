using System;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace ReadCSVDbBulkInsert
{
    class Program
    {
        static void Main(string[] args)
        {
            var data = CsvData.LoadCsvData("data.csv");

            string connectionString = @"Data Source = localhost; Initial Catalog=csvdb; User ID=sa;Password=Password1@";

            AutoSqlBulkCopy(data, connectionString);
            Console.WriteLine();
            Console.WriteLine("Hello World!");
        }

        private static void AutoSqlBulkCopy(DataTable dataTable, string connectionString)
        {
            using (var sqlConnection = new SqlConnection(connectionString))
            {
                sqlConnection.Open();
                var transaction = sqlConnection.BeginTransaction();

                try
                {
                    dataTable.TableName = "Endpoint";

                    // checking whether the table selected from the dataset exists in the database or not
                    var checkTableIfExistsCommand = new SqlCommand("IF EXISTS (SELECT 1 FROM sysobjects WHERE name =  '" + dataTable.TableName + "') SELECT 1 ELSE SELECT 0", sqlConnection, transaction);
                    var exists = checkTableIfExistsCommand.ExecuteScalar().ToString().Equals("1");

                    // if does not exist
                    if (!exists)
                    {
                        var createTableBuilder = new StringBuilder("CREATE TABLE [" + dataTable.TableName + "]");
                        createTableBuilder.AppendLine("(");

                        string dataType = string.Empty;
                        // selecting each column of the datatable to create a table in the database
                        foreach (DataColumn dc in dataTable.Columns)
                        {
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