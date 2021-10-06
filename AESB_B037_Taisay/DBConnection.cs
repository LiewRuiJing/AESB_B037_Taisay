using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;

namespace AESB
{
    public class DBConnection
    {
        private string _ConnectionString;
        public string ConnectionString { get { return _ConnectionString; } }
        private string _Password;
        private string _UserID;
        private string _Database;
        public string DatabaseName { get { return _Database; } }
        private string _Server;
        public string ServerName { get { return _Server; } }
        private int _Port;
        private SqlConnection _SQLConn;
        private SqlTransaction _SQLTrans;
        public ConnectionState ConnectionState { get { return _SQLConn.State; } }
        public int TimeOut = 60;
        public event EventHandler<SqlInfoMessageEventArgs> InfoMessage;

        public DBConnection()
        {
            _ConnectionString = "";
            _SQLTrans = null;
        }

        public DBConnection(string ServerName, string Database, string UserID, string Password)
        {
            _Server = ServerName;
            _Database = Database;
            _UserID = UserID;
            _Password = Password;

            _ConnectionString = "Data Source='" + _Server + "'; Initial Catalog= '" + _Database + "'; User ID= '" + _UserID + "'; Password='" + _Password + "';";

            _SQLConn = new SqlConnection(_ConnectionString);
            _SQLConn.InfoMessage += _SQLConn_InfoMessage;
            _SQLTrans = null;
        }

        public DBConnection(string ServerName, string Database)
        {
            _Server = ServerName;
            _Database = Database;

            _ConnectionString = "Data Source=" + ServerName + ";Integrated Security=true; Initial Catalog= " + _Database + ";";
            _SQLConn = new SqlConnection(_ConnectionString);
            _SQLConn.InfoMessage += _SQLConn_InfoMessage;
            _SQLTrans = null;
        }

        public DBConnection(string ServerName, int Port, string Database, string UserID, string Password)
        {
            _Server = ServerName;
            _Database = Database;
            _UserID = UserID;
            _Password = Password;
            _Port = Port;

            _ConnectionString = "Data Source=" + _Server + "," + _Port.ToString() + "; Initial Catalog= " + _Database + "; User ID= " + _UserID + "; Password=" + _Password + ";";
            _SQLConn = new SqlConnection(_ConnectionString);
            _SQLConn.InfoMessage += _SQLConn_InfoMessage;
            _SQLTrans = null;
        }

        void _SQLConn_InfoMessage(object sender, SqlInfoMessageEventArgs e)
        {
            if (InfoMessage != null)
            {
                InfoMessage(this, e);
            }
        }

        public DBConnection Copy()
        {
            DBConnection tmpDB = new DBConnection(_Server, _Database, _UserID, _Password);
            return tmpDB;
        }

        public DBConnection StartTransaction()
        {
            if (_SQLConn.State != System.Data.ConnectionState.Open)
                _SQLConn.Open();

            _SQLTrans = _SQLConn.BeginTransaction();
            return this;
        }

        public void Commit()
        {
            try
            {
                if (_SQLTrans != null)
                    _SQLTrans.Commit();

                _SQLTrans.Dispose();
                _SQLTrans = null;
                _SQLConn.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Rollback()
        {
            try
            {
                if (_SQLTrans != null)
                {
                    _SQLTrans.Rollback();
                    _SQLTrans.Dispose();
                    _SQLTrans = null;
                }
                _SQLConn.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public ConnectionState IsServerConnected()
        {
            ConnectionState status = ConnectionState.Closed;

            try
            {
                SqlConnection tmpSql = new SqlConnection(ConnectionString);
                tmpSql.Open();

                status = tmpSql.State;

                tmpSql.Close();
            }
            catch (Exception)
            {
                status = ConnectionState.Broken;
            }
            return status;
        }

        public Boolean IsServerConnected(int TimeoutInSeconds)
        {
            try
            {
                SqlConnection tmpSql = new SqlConnection(ConnectionString + " Timeout = " + TimeoutInSeconds + ";");

                tmpSql.Open();
                tmpSql.Close();
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public DataSet GetDataSet(string Query)
        {
            try
            {
                SqlDataAdapter da = new SqlDataAdapter();
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = Query;
                da.SelectCommand = cmd;
                cmd.CommandTimeout = TimeOut;
                da.UpdateBatchSize = TimeOut;

                DataSet ds = new DataSet();
                _SQLConn.Open();

                da.FillSchema(ds, SchemaType.Mapped);
                da.Fill(ds);

                _SQLConn.Close();
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable GetDataTable(string Query, SqlParameter[] sqlParameter)
        {
            try
            {
                SqlDataAdapter da = new SqlDataAdapter();
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = Query;
                cmd.CommandTimeout = TimeOut;
                da.UpdateBatchSize = TimeOut;

                if (sqlParameter != null)
                    for (int i = 0; i < sqlParameter.Length; i++)
                        cmd.Parameters.AddWithValue(sqlParameter[i].ParameterName, sqlParameter[i].Value);

                if (_SQLTrans != null)
                    cmd.Transaction = _SQLTrans;

                da.SelectCommand = cmd;

                DataTable dtData = new DataTable();

                if (_SQLConn.State != System.Data.ConnectionState.Open)
                    _SQLConn.Open();

                da.FillSchema(dtData, SchemaType.Mapped);
                da.Fill(dtData);

                if (_SQLTrans == null)
                    _SQLConn.Close();

                return dtData;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable GetDataTable(string Query, SqlParameter[] sqlParameter, Boolean TableSchema)
        {
            try
            {
                SqlDataAdapter da = new SqlDataAdapter();
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = Query;
                cmd.CommandTimeout = TimeOut;
                da.UpdateBatchSize = TimeOut;

                if (sqlParameter != null)
                    for (int i = 0; i < sqlParameter.Length; i++)
                        cmd.Parameters.AddWithValue(sqlParameter[i].ParameterName, sqlParameter[i].Value);

                if (_SQLTrans != null)
                    cmd.Transaction = _SQLTrans;

                da.SelectCommand = cmd;

                DataTable dtData = new DataTable();

                if (_SQLConn.State != System.Data.ConnectionState.Open)
                    _SQLConn.Open();

                if (TableSchema)
                    da.FillSchema(dtData, SchemaType.Mapped);

                da.Fill(dtData);

                if (_SQLTrans == null)
                    _SQLConn.Close();

                return dtData;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable GetDataTable(string Query)
        {
            try
            {
                SqlDataAdapter da = new SqlDataAdapter();
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = Query;
                cmd.CommandTimeout = TimeOut;
                da.UpdateBatchSize = TimeOut;

                if (_SQLTrans != null)
                    cmd.Transaction = _SQLTrans;

                da.SelectCommand = cmd;

                DataTable dtData = new DataTable();

                if (_SQLConn.State != System.Data.ConnectionState.Open)
                    _SQLConn.Open();

                da.Fill(dtData);

                if (_SQLTrans == null)
                    _SQLConn.Close();

                return dtData;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public DataTable GetDataTable(string Query, Boolean TableSchema)
        {
            try
            {
                SqlDataAdapter da = new SqlDataAdapter();
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = Query;
                cmd.CommandTimeout = TimeOut;
                da.UpdateBatchSize = TimeOut;

                if (_SQLTrans != null)
                    cmd.Transaction = _SQLTrans;

                da.SelectCommand = cmd;

                DataTable dtData = new DataTable();

                if (_SQLConn.State != System.Data.ConnectionState.Open)
                    _SQLConn.Open();

                if (TableSchema)
                    da.FillSchema(dtData, SchemaType.Mapped);

                da.Fill(dtData);

                if (_SQLTrans == null)
                    _SQLConn.Close();

                return dtData;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public int ExecuteNonQuery(string Query, SqlParameter[] sqlParameter)
        {
            int returnRow = 0;
            try
            {
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = Query;
                cmd.Connection = _SQLConn;
                cmd.CommandTimeout = TimeOut;

                if (_SQLTrans != null)
                    cmd.Transaction = _SQLTrans;

                if (sqlParameter != null)
                    for (int i = 0; i < sqlParameter.Length; i++)
                        cmd.Parameters.AddWithValue(sqlParameter[i].ParameterName, sqlParameter[i].Value);

                if (_SQLConn.State != System.Data.ConnectionState.Open)
                    _SQLConn.Open();

                returnRow = cmd.ExecuteNonQuery();

                if (_SQLTrans == null)
                    _SQLConn.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return returnRow;
        }

        public int SaveDataTable(string Query, DataTable Table, SqlParameter[] sqlParameter)
        {
            int returnRow = 0;

            try
            {
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = Query;
                cmd.Connection = _SQLConn;
                cmd.CommandTimeout = TimeOut;

                if (_SQLTrans != null)
                    cmd.Transaction = _SQLTrans;

                if (sqlParameter != null)
                    for (int i = 0; i < sqlParameter.Length; i++)
                        cmd.Parameters.AddWithValue(sqlParameter[i].ParameterName, sqlParameter[i].Value);

                SqlDataAdapter Adapter = new SqlDataAdapter(cmd);
                SqlCommandBuilder sqlCB = new SqlCommandBuilder(Adapter);
                Adapter.DeleteCommand = sqlCB.GetDeleteCommand();
                Adapter.DeleteCommand.CommandTimeout = TimeOut;
                Adapter.UpdateCommand = sqlCB.GetUpdateCommand();
                Adapter.UpdateCommand.CommandTimeout = TimeOut;
                Adapter.InsertCommand = sqlCB.GetInsertCommand();
                Adapter.InsertCommand.CommandTimeout = TimeOut;
                Adapter.UpdateBatchSize = TimeOut;

                returnRow = Adapter.Update(Table);
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return returnRow;
        }

        public int SaveDataTable(string Query, DataTable Table)
        {
            int returnRow = 0;

            try
            {
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = Query;
                cmd.Connection = _SQLConn;
                cmd.CommandTimeout = TimeOut;

                if (_SQLTrans != null)
                    cmd.Transaction = _SQLTrans;

                SqlDataAdapter Adapter = new SqlDataAdapter(cmd);
                SqlCommandBuilder sqlCB = new SqlCommandBuilder(Adapter);
                Adapter.DeleteCommand = sqlCB.GetDeleteCommand();
                Adapter.DeleteCommand.CommandTimeout = TimeOut;
                Adapter.UpdateCommand = sqlCB.GetUpdateCommand();
                Adapter.UpdateCommand.CommandTimeout = TimeOut;
                Adapter.InsertCommand = sqlCB.GetInsertCommand();
                Adapter.InsertCommand.CommandTimeout = TimeOut;
                Adapter.UpdateBatchSize = TimeOut;

                returnRow = Adapter.Update(Table);
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return returnRow;
        }

        public object ExecuteScalar(string Query, SqlParameter[] sqlParameter)
        {
            object obj = null;

            try
            {
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = Query;
                cmd.CommandTimeout = TimeOut;

                if (_SQLTrans != null)
                    cmd.Transaction = _SQLTrans;

                if (sqlParameter != null)
                    for (int i = 0; i < sqlParameter.Length; i++)
                        cmd.Parameters.AddWithValue(sqlParameter[i].ParameterName, sqlParameter[i].Value);

                if (_SQLConn.State != System.Data.ConnectionState.Open)
                    _SQLConn.Open();

                obj = cmd.ExecuteScalar();

                if (_SQLTrans == null)
                    _SQLConn.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return obj;
        }

        public DateTime GetServerDateTime()
        {
            DateTime obj = DateTime.MinValue;

            try
            {
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = "SELECT SYSDATETIME() AS DateTime";
                cmd.CommandTimeout = TimeOut;

                if (_SQLConn.State != System.Data.ConnectionState.Open)
                    _SQLConn.Open();

                obj = Convert.ToDateTime(cmd.ExecuteScalar());

                if (_SQLTrans == null)
                    _SQLConn.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return obj;
        }

        public DataRow GetFirstDataRow(string Query, SqlParameter[] sqlParameter)
        {
            DataRow dr = null;

            try
            {
                SqlDataAdapter da = new SqlDataAdapter();
                SqlCommand cmd = _SQLConn.CreateCommand();
                cmd.CommandText = Query;
                cmd.CommandTimeout = TimeOut;
                da.UpdateBatchSize = TimeOut;

                if (sqlParameter != null)
                    for (int i = 0; i < sqlParameter.Length; i++)
                        cmd.Parameters.AddWithValue(sqlParameter[i].ParameterName, sqlParameter[i].Value);

                if (_SQLTrans != null)
                    cmd.Transaction = _SQLTrans;

                da.SelectCommand = cmd;

                DataTable dtData = new DataTable();

                if (_SQLConn.State != System.Data.ConnectionState.Open)
                    _SQLConn.Open();

                da.Fill(dtData);

                if (_SQLTrans == null)
                    _SQLConn.Close();

                if (dtData.Rows.Count > 0)
                    dr = dtData.Rows[0];
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return dr;
        }
    }
}
