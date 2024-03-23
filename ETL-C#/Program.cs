using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime.Internal.Transform;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace ETL_C_
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            OracleConnector oracleConnector = new OracleConnector();
            OracleConnection conn = oracleConnector.Connect();

            // Establish connection to DynamoDB table
            DynamoConnector dynamoConnector = new DynamoConnector();
            Table table = dynamoConnector.GetTable("PM_Review_M");

            // Read data from DynamoDB table
            List<Document> data = dynamoConnector.ReadItemTable("PM_Review_M");

            // Check if the Oracle connection is established
            if (conn != null)
            {
                foreach (var d in data)
                {
                    //CreateTime(d["Date"]);
                    Console.WriteLine(Convert.ToInt32(d["customerID"]));
                    Console.WriteLine(d["Date"]);
                    ETL_TO_Oracle(d["Date"], Convert.ToInt32(d["customerID"]),Convert.ToDecimal(d["score"]));
                }

                // Close the Oracle connection
                conn.Close();
            }

            Console.ReadKey();
        }
        static void CreateTime(string TimeID)
        {
            string connectionString = "User Id=AdvDBS04;Password=CSKMITL_MAS;Data Source=161.246.35.106:1521/orcldb";

            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    // เตรียมคำสั่ง SQL เพื่อตรวจสอบข้อมูลซ้ำ
                    string checkDuplicateSql = "SELECT COUNT(*) FROM DW04_TIME WHERE timeID = :val1";

                    // เตรียมคำสั่ง SQL เพื่อเพิ่มข้อมูล
                    string insertSql = "INSERT INTO DW04_TIME(timeID, week, month,quarter,year,holiday) VALUES (:val1, :val2, :val3,:val4, :val5, :val6)";

                    DateTime date_time = DateTime.Parse(TimeID);
                    CultureInfo ci = CultureInfo.CurrentCulture;
                    int weekNum = ((date_time.Day - 1) / 7) + 1;
                    int month = date_time.Month;
                    int quarter = (month - 1) / 3 + 1;
                    int year = date_time.Year;

                    // ตรวจสอบว่ามีข้อมูลที่ซ้ำหรือไม่
                    using (OracleCommand checkDuplicateCommand = new OracleCommand(checkDuplicateSql, connection))
                    {
                        checkDuplicateCommand.Parameters.Add(":val1", OracleDbType.Date).Value = date_time;
                        int duplicateCount = Convert.ToInt32(checkDuplicateCommand.ExecuteScalar());

                        // ถ้าไม่มีข้อมูลที่ซ้ำ ให้เพิ่มข้อมูล
                        if (duplicateCount == 0)
                        {
                            // สร้าง OracleCommand
                            using (OracleCommand insertCommand = new OracleCommand(insertSql, connection))
                            {
                                // เพิ่มพารามิเตอร์
                                insertCommand.Parameters.Add(":val1", OracleDbType.Date).Value = date_time;
                                insertCommand.Parameters.Add(":val2", OracleDbType.Int32).Value = weekNum;
                                insertCommand.Parameters.Add(":val3", OracleDbType.Int32).Value = month;
                                insertCommand.Parameters.Add(":val4", OracleDbType.Int32).Value = quarter;
                                insertCommand.Parameters.Add(":val5", OracleDbType.Int32).Value = year;
                                insertCommand.Parameters.Add(":val6", OracleDbType.Int32).Value = 0;

                                // ประมวลผลคำสั่ง
                                int rowsInserted = insertCommand.ExecuteNonQuery();
                                Console.WriteLine($"{rowsInserted} row(s) inserted.");
                            }
                        }
                        else
                        {
                            Console.WriteLine("Duplicate data found. Skipping insertion.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }
            }
        }
        static void ETL_TO_Oracle(string TimeID, int CustomerID, Decimal Score)
        {
            string connectionString = "User Id=AdvDBS04;Password=CSKMITL_MAS;Data Source=161.246.35.106:1521/orcldb";

            using (OracleConnection connection = new OracleConnection(connectionString))
            {
                try
                {
                    connection.Open();

                    // Prepare SQL statement with WHERE NOT EXISTS clause
                    string sql = "INSERT INTO DW04_FT_RATING (timeID, customerID, Score) " +
                                 "SELECT :val1, :val2, :val3 FROM dual " +
                                 "WHERE NOT EXISTS (SELECT 1 FROM DW04_FT_RATING WHERE timeID = :val1 AND customerID = :val2)";

                    // Create OracleCommand
                    using (OracleCommand command = new OracleCommand(sql, connection))
                    {
                        // Add parameters
                        command.Parameters.Add(":val1", OracleDbType.Date).Value = DateTime.Parse(TimeID);
                        command.Parameters.Add(":val2", OracleDbType.Int32).Value = CustomerID;
                        command.Parameters.Add(":val3", OracleDbType.Decimal).Value = Score;

                        // Execute command
                        int rowsInserted = command.ExecuteNonQuery();
                        Console.WriteLine($"{rowsInserted} row(s) inserted.");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }
            }
        }

    }
}

