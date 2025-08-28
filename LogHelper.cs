using Microsoft.Data.SqlClient;

public class LogHelper
{
    public static void WriteConsoleMessage(string msg, string sqlConnectionString)
    {
        WriteConsoleMessage(msg);

        using (SqlConnection emlConnection = new SqlConnection(sqlConnectionString))
        {
            emlConnection.Open();

            // Use parameterized query to safely handle single quotes and avoid SQL injection
            using (SqlCommand cmdWriteLog = new SqlCommand(
                "INSERT INTO tbl_MKTECommOrderInfoLog(LogData) VALUES (@msg)",
                emlConnection))
            {
                cmdWriteLog.Parameters.AddWithValue("@msg", msg);
                cmdWriteLog.ExecuteNonQuery();
            }
        }

        /*
        using (SqlConnection emlConnection = new SqlConnection(SqlconnectionString))
        {
            emlConnection.Open();
            // logic below corrects it so that msg values that contain single quotes still work
            using (SqlCommand cmdWriteLog = new SqlCommand("INSERT INTO tbl_MKTECommOrderInfoLog(LogData) VALUES (@msg)", emlConnection))
            {
                cmdWriteLog.Parameters.AddWithValue("@msg", msg);
                cmdWriteLog.ExecuteNonQuery();
            }
            // SqlCommand cmdWriteLog = new SqlCommand("INSERT INTO tbl_MKTECommOrderInfoLog(LogData) VALUES ('" + msg + "')", emlConnection);
            // cmdWriteLog.ExecuteNonQuery();
        }
        */
    }

    public static void WriteConsoleMessage(string msg)
    {
        Console.WriteLine(string.Concat(DateTime.Now, ": ", msg));
    }

    public static void WriteConsoleMessage(Exception ex)
    {
        Console.WriteLine(string.Concat(DateTime.Now, ": ", ex.Message));
    }
}