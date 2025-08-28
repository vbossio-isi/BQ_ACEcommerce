using Google.Cloud.BigQuery.V2;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Google.Apis.Auth.OAuth2;
using BQ_ACECommerce;


public class BigQueryHelper
{
    private readonly IConfiguration _config;
    private readonly string _sqlConnString;
    private readonly ILogger? _logger;

    private readonly GoogleCredential _credential;

    public BigQueryHelper(IConfiguration config, string sqlConnString /*, ILoggerFactory loggerFactory */)
    {
        _config = config;
        _sqlConnString = sqlConnString;
        //_logger = loggerFactory.CreateLogger<BigQueryHelper>();

        // var credPath = _config["GoogleCredentialFile"];
        // if (!string.IsNullOrEmpty(credPath))
        // {
        //     credPath = credPath.Replace("%HOME%", Environment.GetEnvironmentVariable("HOME") ?? Environment.CurrentDirectory);
        //     Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", credPath);
        // }

        var saJson = _config["Values:GcpCredentialsJson"];
        if (string.IsNullOrWhiteSpace(saJson))
            throw new InvalidOperationException("App setting 'GcpCredentialsJson' is required and must contain the service-account JSON.");

        string[] scopes = { "https://www.googleapis.com/auth/bigquery" };

        // Build a scoped GoogleCredential directly from JSON (no env vars, no files).
        _credential = GoogleCredential
            .FromJson(saJson)
            .CreateScoped(scopes);

    }

    public async Task<BigQueryResults> FetchTableAsync(string GcpProjectId, string SQLToRun)
    {
        try
        {
            // using added by Vince
            using (var client = await BigQueryClient.CreateAsync(GcpProjectId, _credential))
            {
                //_logger.LogInformation($"Executing BigQuery SQL: {SQLToRun}");

                var job = await client.CreateQueryJobAsync(
                SQLToRun,
                parameters: null,
                options: new QueryOptions
                {
                    UseLegacySql = false,
                    UseQueryCache = false
                });

                BigQueryResults results = await job.GetQueryResultsAsync();
                return results;
            }

        }
        catch (Exception ex)
        {
            LogHelper.WriteConsoleMessage($"FetchTableAsync returned error: {ex.Message} {ex.InnerException?.Message}", _sqlConnString);
            return null;
        }

    }

    public static Type MapBigQueryTypeToDotNet(string fieldType)
    {
        return fieldType.ToUpper() switch
        {
            "STRING" => typeof(string),
            "INT64" => typeof(long),
            "FLOAT64" => typeof(double),
            "BOOL" => typeof(bool),
            "BYTES" => typeof(byte[]),
            "TIMESTAMP" => typeof(DateTime),
            "DATE" => typeof(DateTime),
            "TIME" => typeof(TimeSpan),
            "DATETIME" => typeof(DateTime),
            "NUMERIC" => typeof(decimal),
            "GEOGRAPHY" => typeof(string),
            _ => typeof(string)
        };
    }
    
    public static object ConvertBigQueryValue(object value, string fieldType)
    {
        if (value == null) return DBNull.Value;

        return fieldType.ToUpper() switch
        {
            "INT64" => Convert.ToInt64(value),
            "FLOAT64" => Convert.ToDouble(value),
            "BOOL" => Convert.ToBoolean(value),
            "NUMERIC" => ((BigQueryNumeric)value).ToDecimal(LossOfPrecisionHandling.Throw), // <-- fix here
            "BIGNUMERIC" => ((BigQueryNumeric)value).ToDecimal(LossOfPrecisionHandling.Throw), // if you use BigNumeric
            "TIMESTAMP" => ((DateTime)value).ToLocalTime(),
            "DATE" => ((DateTime)value).Date,
            "TIME" => value, // TimeSpan
            "DATETIME" => ((DateTime)value),
            _ => value
        };
    }


}
