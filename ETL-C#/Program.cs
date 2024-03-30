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
using System.Net.Http;
using Newtonsoft.Json;

namespace ETL_C_
{
    internal class Program
    {
         


        static async Task ETL_Review()
        {
            DynamoConnector dynamoConnector = new DynamoConnector();
            OracleConnector oracleConnector = new OracleConnector();

            var connection = oracleConnector.Connect();

            string tableName = "PM_Review_M";

            var table = dynamoConnector.GetTable(tableName);
            if (table != null)
            {
                // เชื่อมต่อสำเร็จ
                Console.WriteLine("Connected to table: " + tableName);

                // อ่านข้อมูลทั้งตาราง
                var items = dynamoConnector.ReadItemTable(tableName);
                if (items != null)
                {
                    if (connection != null)
                    {
                        foreach (var item in items)
                        {
                            int timestampFromDynamoDB = (int)Convert.ToInt64(item["Date"]);
                            DateTime dateTime = DateTimeOffset.FromUnixTimeSeconds(timestampFromDynamoDB).UtcDateTime;


                            // สร้างคำสั่ง SQL INSERT
                            string insertQuery = @"INSERT INTO DW04_DIM_TIME (timeID, week, month, quarter, year)
                                VALUES (:timeID, :week, :month, :quarter, :year)";

                            DateTime date_time = dateTime;
                            int weekNum = ((date_time.Day - 1) / 7) + 1;
                            int month = date_time.Month;
                            int quarter = (month - 1) / 3 + 1;
                            int year = date_time.Year;

                            // สร้าง OracleParameter สำหรับแทรกข้อมูล
                            OracleParameter[] parameters = new OracleParameter[]
                            {
                            new OracleParameter(":timeID", OracleDbType.TimeStamp) { Value = dateTime },
                            new OracleParameter(":week", OracleDbType.Int32) { Value = weekNum },
                            new OracleParameter(":month", OracleDbType.Int32) { Value = month },
                            new OracleParameter(":quarter", OracleDbType.Int32) { Value = quarter },
                            new OracleParameter(":year", OracleDbType.Int32) { Value = year }
                            };

                            // เรียกใช้เมทอด InsertData ในคลาส OracleConnector
                            oracleConnector.InsertData(connection, insertQuery, parameters);

                        }
                        connection.Close();
                    }
                }
                else
                {
                    Console.WriteLine("Failed to read data from table.");
                }
            }
            else
            {
                // เชื่อมต่อไม่สำเร็จ
                Console.WriteLine("Failed to connect to table: " + tableName);
            }
        }

        static async Task CALLApi()
        {
            OracleConnector oracleConnector = new OracleConnector();

            var connection = oracleConnector.Connect();

            CallAPI api = new CallAPI();
            //dynamic data1 = await api.GetDataaJson("https://raw.githubusercontent.com/kongvut/thai-province-data/master/api_province.json");
            //dynamic data2 = await api.GetDataaJson("https://raw.githubusercontent.com/kongvut/thai-province-data/master/api_amphure.json");
            dynamic data3 = await api.GetDataaJson("https://raw.githubusercontent.com/kongvut/thai-province-data/master/api_tambon.json");

            if (connection != null)
            {
                /*
                // วนลูปเพื่อเข้าถึงข้อมูลในแต่ละ key
                foreach (var key in data1)
                {

                    string insertQuery = @"INSERT INTO TPS04_PM_Provinces (provincesID, Name)
                                VALUES (:provincesID, :Name)";
                    OracleParameter[] parameters = new OracleParameter[]
                    {
                            new OracleParameter(":timeID", OracleDbType.Int32) { Value = Convert.ToInt32(key["id"]) },
                            new OracleParameter(":week", OracleDbType.Varchar2) { Value = key["name_en"] },

                       };
                    // เรียกใช้เมทอด InsertData ในคลาส OracleConnector
                    oracleConnector.InsertData(connection, insertQuery, parameters);
                }
                */
                /*
                // วนลูปเพื่อเข้าถึงข้อมูลในแต่ละ key
                foreach (var key in data2)
                {

                    string insertQuery = @"INSERT INTO TPS04_PM_Amphures (amphuresID, Name,provincesID)
                                VALUES (:provincesID, :Name,:provincesID)";
                    OracleParameter[] parameters = new OracleParameter[]
                    {
                            new OracleParameter(":timeID", OracleDbType.Int32) { Value = Convert.ToInt32(key["id"]) },
                            new OracleParameter(":Name", OracleDbType.Varchar2) { Value = key["name_en"] },
                            new OracleParameter(":provincesID", OracleDbType.Varchar2) { Value = key["province_id"] },

                       };
                    // เรียกใช้เมทอด InsertData ในคลาส OracleConnector
                    oracleConnector.InsertData(connection, insertQuery, parameters);
                }
                */
                // วนลูปเพื่อเข้าถึงข้อมูลในแต่ละ key
                foreach (var key in data3)
                {
                    /*
                    
                    string insertQuery = @"INSERT INTO TPS04_PM_Tombons (tombonsID, Name,amphuresID)
                                VALUES (:tombonsID, :Name, :amphuresID)";
                    OracleParameter[] parameters = new OracleParameter[]
                    {
                            new OracleParameter(":tombonsID", OracleDbType.Int32) { Value = Convert.ToInt32(key["id"]) },
                            new OracleParameter(":Name", OracleDbType.Varchar2) { Value = key["name_en"] },
                            new OracleParameter(":amphuresID", OracleDbType.Varchar2) { Value = key["amphure_id"] },

                       };
                    // เรียกใช้เมทอด InsertData ในคลาส OracleConnector
                    oracleConnector.InsertData(connection, insertQuery, parameters);*/
                    
                    string insertQuery2 = @"INSERT INTO TPS04_PM_POSTALCODE_M(posID, posName,tombonsID)
                                VALUES (:posID, :posName,:tombonsID)";
                    OracleParameter[] parameters2 = new OracleParameter[]
                    {
                            new OracleParameter(":posID", OracleDbType.Int32) { Value = Convert.ToInt32(key["zip_code"]) },
                            new OracleParameter(":posName", OracleDbType.Varchar2) { Value = key["name_en"] },
                            new OracleParameter(":tombonsID", OracleDbType.Int32) { Value = Convert.ToInt32(key["id"]) },

                       };
                    // เรียกใช้เมทอด InsertData ในคลาส OracleConnector
                    oracleConnector.InsertData(connection, insertQuery2, parameters2);
                    
                }
                
                connection.Close();
            }


        }
        static async Task Main(string[] args)
        {
            CALLApi();


            /*
            dynamic data2 = await api.GetDataaJson("https://raw.githubusercontent.com/kongvut/thai-province-data/master/api_amphure.json");

            
            foreach (var key in data2)
            {
                Console.WriteLine(key["id"]);
                Console.WriteLine(key["name_en"]);
                Console.WriteLine(key["province_id"]);
            }
            dynamic data3 = await api.GetDataaJson("https://raw.githubusercontent.com/kongvut/thai-province-data/master/api_tambon.json");

            
            foreach (var key in data3)
            {
                Console.WriteLine(key["id"]);
                Console.WriteLine(key["name_en"]);
                Console.WriteLine(key["zip_code"]);
            }
            */

            
            Console.ReadKey();
        }

    }
}

