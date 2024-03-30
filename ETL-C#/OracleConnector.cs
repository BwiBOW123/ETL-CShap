using System;
using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace ETL_C_
{

    public class OracleConnector
    {
        private readonly string _username;
        private readonly string _password;
        private readonly string _dsn;

        public OracleConnector()
        {
            _username = "AdvDBS04";
            _password = "CSKMITL_MAS";
            _dsn = "161.246.35.106:1521/orcldb";
        }

        public OracleConnection Connect()
        {
            try
            {
                OracleConnection connection = new OracleConnection($"User Id={_username};Password={_password};Data Source={_dsn}");
                connection.Open();
                Console.WriteLine("Connection established successfully!");
                return connection;
            }
            catch (OracleException ex)
            {
                Console.WriteLine("Error occurred: " + ex.Message);
                return null;
            }
        }

        public DataTable ExecuteQuery(OracleConnection connection, string query)
        {
            DataTable dataTable = new DataTable();
            try
            {
                using (OracleCommand command = new OracleCommand(query, connection))
                {
                    using (OracleDataAdapter adapter = new OracleDataAdapter(command))
                    {
                        adapter.Fill(dataTable);
                    }
                }
                return dataTable;
            }
            catch (OracleException ex)
            {
                Console.WriteLine("Error occurred while executing query: " + ex.Message);
                return null;
            }
        }

        public void InsertData(OracleConnection connection, string query, OracleParameter[] parameters)
        {
            try
            {
                using (OracleCommand command = new OracleCommand(query, connection))
                {
                    command.Parameters.AddRange(parameters);
                    command.ExecuteNonQuery();
                }
                Console.WriteLine("Data inserted successfully!");
            }
            catch (OracleException ex)
            {
                Console.WriteLine("Error occurred while inserting data: " + ex.Message);
                Console.WriteLine("Oracle error code: " + ex.Number); // Output the Oracle error code for further troubleshooting
                Console.WriteLine("Oracle error source: " + ex.Source); // Output the Oracle error source for further troubleshooting
            }
            finally
            {
                
            }
        }

        public void UpdateData(OracleConnection connection, string query)
        {
            try
            {
                using (OracleCommand command = new OracleCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
                Console.WriteLine("Data updated successfully!");
            }
            catch (OracleException ex)
            {
                Console.WriteLine("Error occurred while updating data: " + ex.Message);
            }
        }

        public void DeleteData(OracleConnection connection, string query)
        {
            try
            {
                using (OracleCommand command = new OracleCommand(query, connection))
                {
                    command.ExecuteNonQuery();
                }
                Console.WriteLine("Data deleted successfully!");
            }
            catch (OracleException ex)
            {
                Console.WriteLine("Error occurred while deleting data: " + ex.Message);
            }
        }
 
    }

}