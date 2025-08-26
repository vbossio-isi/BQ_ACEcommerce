using System.Data;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
//using Sql.Data;
using Oracle.ManagedDataAccess.Client;


namespace BQ_ACECommerce
{

    // Notes from the ActiveCampaign website about dealing with processing problems
    //
    //     Code	Description
    // 402	The request could not be processed due to account payment issues.
    // 403	The request could not be authenticated or the authenticated user is not authorized to access the requested resource.
    // 404	The requested resource does not exist.
    // 422	The request could not be processed, usually due to a missing or invalid parameter.
    // 

    // API has a rate limit of 5 per second

    // If an error occurs, please pause or "sleep" execution for at least 1,000 milliseconds before resubmitting your API request.

    public class FieldConverter : JsonConverter<Field>
    {
        public override Field Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            throw new NotImplementedException("Deserialization is not supported.");
        }

        public override void Write(Utf8JsonWriter writer, Field value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WriteString("id", value.Id);


            switch (value.Value)
            {
                case int i:
                    writer.WriteNumber("value", i);
                    break;
                case decimal d:
                    writer.WriteNumber("value", d);
                    break;
                case long l:
                    writer.WriteNumber("value", l);
                    break;
                case float f:
                    writer.WriteNumber("value", f);
                    break;
                case double db:
                    writer.WriteNumber("value", db);
                    break;
                case string s:
                    writer.WriteString("value", s);
                    break;
                case null:
                    writer.WriteNull("value");
                    break;
                default:
                    writer.WriteString("value", value.Value.ToString());
                    break;
            }
            writer.WriteEndObject();
        }
    }

    public class EcomCustomerWrapper
    {
        [JsonPropertyName("ecomCustomer")]
        public EcomCustomer Customer { get; set; }
    }

    public class EcomCustomer
    {
        [JsonPropertyName("connectionid")]
        public string ConnectionId { get; set; }

        [JsonPropertyName("externalid")]
        public string ExternalId { get; set; }

        [JsonPropertyName("email")]
        public string Email { get; set; }

        [JsonPropertyName("acceptsMarketing")]
        public string AcceptsMarketing { get; set; } = "1";  // hardcoded
    }

    public class ApiResponse
    {
        public bool IsSuccess { get; set; }
        public HttpStatusCode StatusCode { get; set; }
        public string Content { get; set; }
        public string ErrorMessage { get; set; }
    }

    public class EcomOrderWrapper
    {
        [JsonPropertyName("ecomOrder")]
        public EcomOrder EcomOrder { get; set; }
    }

    public class EcomOrder
    {
        [JsonPropertyName("externalid")]
        public string ExternalId { get; set; }

        [JsonPropertyName("source")]
        public string? Source { get; set; }

        [JsonPropertyName("email")]
        public string Email { get; set; }

        [JsonPropertyName("orderProducts")]
        public List<OrderProduct> OrderProducts { get; set; }

        [JsonPropertyName("orderDiscounts")]
        public List<OrderDiscount> OrderDiscounts { get; set; }

        [JsonPropertyName("orderUrl")]
        public string OrderUrl { get; set; }

        [JsonPropertyName("externalCreatedDate")]
        public string? ExternalCreatedDate { get; set; }

        [JsonPropertyName("externalUpdatedDate")]
        public string ExternalUpdatedDate { get; set; }

        [JsonPropertyName("shippingMethod")]
        public string ShippingMethod { get; set; }

        [JsonPropertyName("totalPrice")]
        public int TotalPrice { get; set; }

        [JsonPropertyName("shippingAmount")]
        public int ShippingAmount { get; set; }

        [JsonPropertyName("taxAmount")]
        public int TaxAmount { get; set; }

        [JsonPropertyName("discountAmount")]
        public int DiscountAmount { get; set; }

        [JsonPropertyName("currency")]
        public string Currency { get; set; }

        [JsonPropertyName("orderNumber")]
        public string OrderNumber { get; set; }

        [JsonPropertyName("connectionid")]
        public string? ConnectionId { get; set; }

        [JsonPropertyName("customerid")]
        public string? CustomerId { get; set; }
    }

    public class OrderProduct
    {
        [JsonPropertyName("externalid")]
        public string ExternalId { get; set; }

        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("price")]
        public int Price { get; set; }

        [JsonPropertyName("quantity")]
        public int Quantity { get; set; }

        [JsonPropertyName("category")]
        public string Category { get; set; }

        [JsonPropertyName("sku")]
        public string Sku { get; set; }

        [JsonPropertyName("description")]
        public string Description { get; set; }

        [JsonPropertyName("imageUrl")]
        public string ImageUrl { get; set; }

        [JsonPropertyName("productUrl")]
        public string ProductUrl { get; set; }
    }

    public class OrderDiscount
    {
        [JsonPropertyName("name")]
        public string Name { get; set; }

        [JsonPropertyName("type")]
        public string Type { get; set; }

        [JsonPropertyName("discountAmount")]
        public int DiscountAmount { get; set; }
    }

    public class APIClient
    {
        private readonly HttpClient _client;
        public ActiveCampaignAPISettings _acApiSettings;
        public TimeSpan _retryDelay = TimeSpan.Zero;
        public int _numRetries;
        private string _dbConnectionString;

        public APIClient(ActiveCampaignAPISettings acApiSettings, string dbConnectionString)
        {
            _acApiSettings = acApiSettings;
            _dbConnectionString = dbConnectionString;
            _client = new HttpClient();
            _client.DefaultRequestHeaders.Add("User-Agent", "mt-activecampaign-agent/7.39.0");
            _client.DefaultRequestHeaders.Add("Accept", "application/json");
            _client.DefaultRequestHeaders.Add("Connection", "keep-alive");
            _client.DefaultRequestHeaders.Add("Api-Token", _acApiSettings.ApiKey);
            _client.DefaultRequestHeaders.Add("Host", "medievaltimes.api-us1.com");

            _numRetries = 1;  // always try at least once
            if (_acApiSettings.UseRetry == "1")
            {
                _numRetries = Convert.ToInt32(_acApiSettings.MaxRetries);
                TimeSpan.TryParse(_acApiSettings.RetryWaitTime ?? String.Empty, out _retryDelay);
                // _acApiSettings.RetryWaitTime = "00:00:3" = 3 seconds
            }
        }

        public async Task<ApiResponse> CallApiWithRetry(string action, HttpMethod method, string url, HttpContent? content = null)
        {
            var apiResponse = new ApiResponse();

            for (int i = 0; i < _numRetries; i++)
            {
                using (var requestMessage = new HttpRequestMessage(method, url))
                {
                    requestMessage.Headers.Add("MT-Request-Token", Guid.NewGuid().ToString());
                    if (content != null)
                    {
                        requestMessage.Content = content;
                    }

                    var prefix = $"{action} Attempt {i + 1} of {_numRetries}";
                    HttpResponseMessage response = await _client.SendAsync(requestMessage);
                    apiResponse = new ApiResponse
                    {
                        StatusCode = response.StatusCode,
                        IsSuccess = response.IsSuccessStatusCode
                    };

                    if (response.IsSuccessStatusCode)
                    {
                        apiResponse.Content = await response.Content.ReadAsStringAsync();
                        break; // no need to retry 
                    }
                    else
                    {
                        apiResponse.ErrorMessage = await response.Content.ReadAsStringAsync();
                        Program.WriteConsoleMessage($"{prefix} returned unsuccesful response {apiResponse.StatusCode} {apiResponse.ErrorMessage}", _dbConnectionString);
                        Task.Delay(_retryDelay).Wait(); // Wait before the next try
                    }
                }
            }

            return apiResponse;
        }

        public async Task<ApiResponse> PostRecordAsync(string action, string URL, string json, string path)
        {
            var url = $"https://{URL}/{path}";
            var jsonContent = new StringContent(json, Encoding.UTF8, "application/json");

            try
            {
                var apiResponse = await CallApiWithRetry(action, HttpMethod.Post, url, jsonContent);

                return apiResponse;

            }
            catch (HttpRequestException e)
            {

                //Console.WriteLine($"Error: {e.Message}");
                Program.WriteConsoleMessage($"Error: {e.Message} posting to {URL}", _dbConnectionString);
                return null;
            }
        }
        public async Task<ApiResponse> GetContactAsync(string URL, string emailAddress)
        {
            string url = $"https://{URL}/contacts?filters[email]={Uri.EscapeDataString(emailAddress)}&include=contactLists";
            string action = "Get Contact";

            try
            {
                var apiResponse = await CallApiWithRetry(action, HttpMethod.Get, url);

                return apiResponse;

            }
            catch (HttpRequestException e)
            {
                //Console.WriteLine($"Error: {e.Message}");
                Program.WriteConsoleMessage($"Error: {e.Message} getting contact with email {emailAddress}", _dbConnectionString);
                return null;
            }
        }


        public async Task<ApiResponse> GetCustomerAsync(string URL, string connectionId, string emailAddress)
        {
            string url = $"https://{URL}/ecomCustomers?filters[connectionid]={connectionId}&filters[email]={Uri.EscapeDataString(emailAddress)}";
            string action = "Get EcomCustomer";

            try
            {
                var apiResponse = await CallApiWithRetry(action, HttpMethod.Get, url);

                return apiResponse;

            }
            catch (HttpRequestException e)
            {
                //Console.WriteLine($"Error: {e.Message}");
                Program.WriteConsoleMessage($"Error: {e.Message} {action} with email {emailAddress}", _dbConnectionString);
                return null;
            }
        }

        public async Task<ApiResponse> GetOrderAsync(string URL, string connectionId, string externalId)
        {
            string url = $"https://{URL}/ecomOrders?filters[connectionid]={connectionId}&filters[externalid]={Uri.EscapeDataString(externalId)}";
            string action = "Get EComOrder";

            try
            {
                var apiResponse = await CallApiWithRetry(action, HttpMethod.Get, url);

                return apiResponse;

            }
            catch (HttpRequestException e)
            {
                //Console.WriteLine($"Error: {e.Message}");
                Program.WriteConsoleMessage($"Error: {e.Message} getting Order with externalId {externalId}", _dbConnectionString);
                return null;
            }
        }

        public async Task<ApiResponse> PostCustomerOrOrdersAsync(string action, string URL, string json, string path)
        {
            var url = $"https://{URL}/{path}";
            var jsonContent = new StringContent(json, Encoding.UTF8, "application/json");
            try
            {
                var apiResponse = await CallApiWithRetry(action, HttpMethod.Post, url, jsonContent);

                return apiResponse;

            }
            catch (HttpRequestException e)
            {

                //Console.WriteLine($"Error: {e.Message}");
                Program.WriteConsoleMessage($"Error: {e.Message} posting to {URL}", _dbConnectionString);
                return null;
            }
        }

        public async Task<ApiResponse> UpdateOrdersAsync(string URL, string json, string path, string existingOrderId)
        {
            var url = $"https://{URL}/{path}/{existingOrderId}";
            string action = "Update EComOrder";
            var jsonContent = new StringContent(json, Encoding.UTF8, "application/json");
            try
            {
                var apiResponse = await CallApiWithRetry(action, HttpMethod.Put, url, jsonContent);

                return apiResponse;

            }
            catch (HttpRequestException e)
            {

                //Console.WriteLine($"Error: {e.Message}");
                Program.WriteConsoleMessage($"Error: {e.Message} updating order {existingOrderId}", _dbConnectionString);
                return null;
            }
        }
    }


    public class EmailResponse
    {
        public string email { get; set; }
        public string status { get; set; }
        public string _id { get; set; }
        public string? reject_reason { get; set; }
    }
    public class ActiveCampaignAPISettings
    {
        public string ApiKey { get; set; }
        public string URL { get; set; }
        public string ConnectionId { get; set; }
        public string UseRetry { get; set; }
        public string MaxRetries { get; set; }
        public string RetryWaitTime { get; set; }

    }
    public class DatabaseSettings
    {
        public string PVConnectionString { get; set; }
        public string MT_EMLConnectionString { get; set; }
    }

    public class TransientFaultHandlingOptions
    {
        public bool Enabled { get; set; }
        public TimeSpan AutoRetryDelay { get; set; }
    }

    public class BuyerTypeTextOptions
    {
        public bool Enabled { get; set; }
        public string AllowedReportGroupIDs { get; set; }
    }



    public class LoggingSettings
    {
        public string LogLevel { get; set; }
        public bool Debug { get; set; }
        public bool UseDebugEmail { get; set; }
        public string DebugEmail { get; set; }
        public bool DryRun { get; set; }
        public bool OverrideTranDate { get; set; }
        public DateTime OverrideTranDateValue { get; set; }
        public string ConfirmationTrimInterval { get; set; }
        public bool ProcessOrdersTableOnly { get; set; }
        public bool ProcessTicketsTableOnly { get; set; }
        public bool TransactionIdOverride { get; set; }
        public string TransactionList { get; set; }
    }
    public class RequestContent
    {
        public string key { get; set; }
        public string template_name { get; set; }
        public string template_content { get { return null; } }
        public EmailMessage message { get; set; }

    }

    public class EmailMessage
    {
        // public string from_email { get; set; }
        // public string subject { get; set; }
        // public string text { get; set; }
        public List<EmailAddress> to { get; set; }

        public string headers { get { return string.Empty; } }

        public string bcc_address { get { return null; } }
        public List<ContentVariable> global_merge_vars { get; set; }

    }

    public class ContentVariable
    {
        public string content { get; set; }
        public string name { get; set; }
    }

    public class EmailAddress
    {
        public string email { get; set; }
        public string type { get; set; }
        public string name { get; set; }
    }

    class Program
    {
        private static bool TimeToRun()
        {
            if (true)
            {
                return true;
            }

            return false;
        }

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

        private static void WriteConsoleMessage(string msg)
        {
            Console.WriteLine(string.Concat(DateTime.Now, ": ", msg));
        }

        private static void WriteConsoleMessage(Exception ex)
        {
            Console.WriteLine(string.Concat(DateTime.Now, ": ", ex.Message));
        }

        public static async Task Main(string[] args)
        {
//           IConfigurationBuilder builder = new ConfigurationBuilder().AddJsonFile("appsettings.json", false, true);    

            string projectRoot = Path.Combine(AppContext.BaseDirectory, @"..\..\.."); // navigate up 3 folders
            projectRoot = Path.GetFullPath(projectRoot); // normalize path

            var builder = new ConfigurationBuilder()
                .SetBasePath(projectRoot)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            IConfigurationRoot root = builder.Build();

            LoggingSettings loggingSettings = new();
            root.GetSection(nameof(LoggingSettings))
                .Bind(loggingSettings);

            TransientFaultHandlingOptions transientFaultSettings = new();
            root.GetSection(nameof(TransientFaultHandlingOptions))
                .Bind(transientFaultSettings);

            DatabaseSettings databaseSettings = new();
            root.GetSection(nameof(DatabaseSettings))
                .Bind(databaseSettings);

            ActiveCampaignAPISettings acAPISettings = new();
            root.GetSection(nameof(ActiveCampaignAPISettings))
                .Bind(acAPISettings);


            string connString = root["SqlConnectionString"];

            /* sql server test code
                        // Use SqlClient
                        using (SqlConnection conn = new SqlConnection(connString))
                        {
                            conn.Open();
                            Console.WriteLine(conn == null ? "conn is NULL!" : "conn is OK");
                            Console.WriteLine("Connected to SQL Server!");

                            string sql = "SELECT TOP 5 name, create_date FROM sys.databases";

                            Console.WriteLine(sql == null ? "sql is NULL!" : $"SQL = {sql}");

                            var assembly = typeof(SqlCommand).Assembly.Location;
                            Console.WriteLine($"SqlCommand from: {assembly}");


                            using (SqlCommand cmd = new SqlCommand(sql, conn))
                            {
                                // cmd.CommandText must not be null
                                Console.WriteLine($"SQL: {cmd.CommandText}");
                                using (SqlDataReader reader = cmd.ExecuteReader())  // <- this is where yours fails
                                {
                                    while (reader.Read())
                                    {
                                        Console.WriteLine($"{reader["name"]} - {reader["create_date"]}");
                                    }
                                }
                            }

                        }
            */
            /* BigQuery test code 
            var bq = new BigQueryHelper(root);
            var sql = "select * from `tdc-replication.medieval_times.service_charge` limit 3"; 
            var bqResults = await bq.FetchTableAsync("tdc-replication", sql);
            // now extract to DataTable/DataSet
            DataTable table = new DataTable("BigQueryData");

            // create columns dynamically from schema with type mapping
            foreach (var field in bqResults.Schema.Fields)
            {
                Type columnType = BigQueryHelper.MapBigQueryTypeToDotNet(field.Type);
                table.Columns.Add(field.Name, columnType);
            }
            // add data
            foreach (var row in bqResults)
            {
                var dataRow = table.NewRow();
                foreach (var field in bqResults.Schema.Fields)
                {
                    dataRow[field.Name] = BigQueryHelper.ConvertBigQueryValue(row[field.Name], field.Type);
                }
                table.Rows.Add(dataRow);
            }
            */

            WriteConsoleMessage("Beginning Service", connString);

            if (loggingSettings.Debug)
            {
                WriteConsoleMessage($"TransientFaultHandlingOptions.Enabled={transientFaultSettings.Enabled}", connString);
                WriteConsoleMessage($"TransientFaultHandlingOptions.AutoRetryDelay={transientFaultSettings.AutoRetryDelay}", connString);
                WriteConsoleMessage($"DatabaseSettings.ConnectionString={databaseSettings.PVConnectionString}", connString);
                WriteConsoleMessage($"ActiveCampaignAPISettings.ApiKey={acAPISettings.ApiKey}", connString);
                WriteConsoleMessage($"ActiveCampaignAPISettings.PostURL={acAPISettings.URL}", connString);
                WriteConsoleMessage($"ActiveCampaignAPISettings.UseRety={acAPISettings.UseRetry}", connString);
                WriteConsoleMessage($"ActiveCampaignAPISettings.MaxRetries={acAPISettings.MaxRetries}", connString);
                WriteConsoleMessage($"ActiveCampaignAPISettings.RetryWaitTime={acAPISettings.RetryWaitTime}", connString);
                WriteConsoleMessage($"LogLevel={loggingSettings.LogLevel}", connString);
                WriteConsoleMessage($"Debug={loggingSettings.Debug}", connString);
                WriteConsoleMessage($"UseDebugEmail={loggingSettings.UseDebugEmail}", connString);
                WriteConsoleMessage($"DebugEmail={loggingSettings.DebugEmail}", connString);
                WriteConsoleMessage($"DryRun={loggingSettings.DryRun}", connString);
                WriteConsoleMessage($"OverrideTranDate={loggingSettings.OverrideTranDate}", connString);
                WriteConsoleMessage($"OverrideTranDateValue={loggingSettings.OverrideTranDateValue}", connString);
                WriteConsoleMessage($"ConfirmationTrimInterval={loggingSettings.ConfirmationTrimInterval}", connString);
                WriteConsoleMessage($"ProcessOracleTableOnly={loggingSettings.ProcessOrdersTableOnly}", connString);
                WriteConsoleMessage($"ProcessTicketsTableOnly={loggingSettings.ProcessTicketsTableOnly}", connString);
                WriteConsoleMessage($"TransactionIdOverride={loggingSettings.TransactionIdOverride}", connString);
                WriteConsoleMessage($"TransactionList={loggingSettings.TransactionList}", connString);
            }

            if (TimeToRun())
            {

                string queryTicketsSelectString = @"SELECT              e.EVENT_ID, 
                                                            EVENT_CODE, 
                                                            EVENT_DATE AS EVENT_DATE_TIME, 
                                                            e.Supplier_Id, 
                                                            o.FINANCIAL_PATRON_ACCOUNT_ID AS PATRON_ACCOUNT_ID,  
                                                            t.ORDER_ID, 
                                                            p.PRICE_SCALE_CODE AS PRICE_SCALE, 
                                                            b.BUYER_TYPE_CODE, 
                                                            b.DESCRIPTION AS BUYER_TYPE_DESC, 
                                                            b.BUYER_TYPE_GROUP_ID, 
                                                            b.REPORT_BUYER_TYPE_GROUP_ID, 
                                                            b.DISPLAY_INDICATOR, 
                                                            b.TAX_EXEMPT, 
                                                            t.TICKET_ID, 
                                                            t.TRANSACTION_ID, 
                                                            MAX(t.PAYMENT_STATUS_CODE) AS PAYMENT_STATUS_CODE, 
                                                            MAX(PRICE) AS TICKET_PRICE, 
                                                            SUM(CASE WHEN s.DESCRIPTION LIKE '%Processing Fee%' THEN si.ACTUAL_AMOUNT ELSE 0 END) AS CONV_FEE, 
                                                            SUM(CASE WHEN (s.IS_TAX = 1 OR s.SERVICE_CHARGE_CODE = 'CONVSNTAX') AND (s.INCLUDE_IN_TICKET_PRICE = 0) THEN si.ACTUAL_AMOUNT ELSE 0 END) AS SALES_TAX, 
                                                            SUM(CASE WHEN (s.IS_TAX = 1 OR s.SERVICE_CHARGE_CODE = 'CONVSNTAX') AND (s.INCLUDE_IN_TICKET_PRICE = 1) THEN si.ACTUAL_AMOUNT ELSE 0 END) AS INC_SALES_TAX, 
                                                            SUM(CASE WHEN s.SERVICE_CHARGE_CODE LIKE '%TIP%' AND s.INCLUDE_IN_TICKET_PRICE = 0 THEN si.ACTUAL_AMOUNT ELSE 0 END) AS GRATUITY, 
                                                            SUM(CASE WHEN s.INCLUDE_IN_TICKET_PRICE = 1 THEN si.ACTUAL_AMOUNT ELSE 0 END) AS ALLOCATION, 
                                                            1 AS TICKET_COUNT 
                                        FROM                      TICKET t 
                                        INNER JOIN          (SELECT * FROM ORDER_LINE_ITEM WHERE ORDER_ID IN (<Order_Id_List>)) ol 
                                        ON                                             t.ORDER_LINE_ITEM_ID = ol.ORDER_LINE_ITEM_ID 
                                        INNER JOIN          PRICE_SCALE p 
                                        ON                                             p.PRICE_SCALE_ID = t.PRICE_SCALE_ID 
                                        INNER JOIN          BUYER_TYPE b 
                                        ON                                             t.BUYER_TYPE_ID = b.BUYER_TYPE_ID 
                                        INNER JOIN          EVENT e  
                                        ON                                             e.EVENT_ID = ol.EVENT_ID 
                                        AND                                          e.EVENT_CLASS_CODE = 'P' 
                                        INNER JOIN          EVENT_CATEGORY ec 
                                        ON                                             e.EVENT_CATEGORY_ID = ec.EVENT_CATEGORY_ID 
                                        INNER JOIN          PATRON_ORDER o 
                                        ON                                             o.ORDER_ID = ol.ORDER_ID 
                                        LEFT JOIN              Service_Charge_Item si 
                                        ON                                             si.TICKET_ID = t.TICKET_ID  
                                        AND                                          si.NEGATING_SERVICE_CHRG_ITEM_ID IS NULL 
                                        LEFT JOIN              Service_Charge s 
                                        ON                                             s.SERVICE_CHARGE_ID = si.SERVICE_CHARGE_ID 
                                        WHERE                   t.REMOVE_ORDER_LINE_ITEM_ID IS NULL  
                                        GROUP BY            e.Supplier_Id, 
                                                    e.EVENT_ID, 
                                                    EVENT_CODE, 
                                                    EVENT_DATE, 
                                                    o.FINANCIAL_PATRON_ACCOUNT_ID,  
                                                    t.ORDER_ID, 
                                                    p.PRICE_SCALE_CODE, 
                                                    b.BUYER_TYPE_CODE, 
                                                    b.BUYER_TYPE_GROUP_ID, 
                                                    b.REPORT_BUYER_TYPE_GROUP_ID, 
                                                    b.DESCRIPTION, 
                                                    b.DISPLAY_INDICATOR, 
                                                    b.TAX_EXEMPT, 
                                                    t.TRANSACTION_ID, 
                                                    t.TICKET_ID";

                string queryTicketsArchive = @"INSERT INTO tbl_MKTECommTicketDetailArchive (ARCHIVED_DATE_TIME, EVENT_ID, EVENT_CODE, EVENT_DATE_TIME, SUPPLIER_ID, PATRON_ACCOUNT_ID, ORDER_ID, PRICE_SCALE, BUYER_TYPE_CODE, BUYER_TYPE_DESC, BUYER_TYPE_GROUP_ID, REPORT_BUYER_TYPE_GROUP_ID, DISPLAY_INDICATOR, TAX_EXEMPT, TICKET_ID, TRANSACTION_ID, PAYMENT_STATUS_CODE, TICKET_PRICE, CONV_FEE, SALES_TAX, GRATUITY, ALLOCATION, INC_SALES_TAX, TICKET_COUNT, Ticket_Update_Status, Ticket_Updated_Dtm, Response_Object, Insert_Dtm)
                                        SELECT NOW(), EVENT_ID, EVENT_CODE, EVENT_DATE_TIME, SUPPLIER_ID, PATRON_ACCOUNT_ID, ORDER_ID, PRICE_SCALE, BUYER_TYPE_CODE, BUYER_TYPE_DESC, BUYER_TYPE_GROUP_ID, REPORT_BUYER_TYPE_GROUP_ID, DISPLAY_INDICATOR, TAX_EXEMPT, TICKET_ID, TRANSACTION_ID, PAYMENT_STATUS_CODE, TICKET_PRICE, CONV_FEE, SALES_TAX, GRATUITY, ALLOCATION, INC_SALES_TAX, TICKET_COUNT, Ticket_Update_Status, Ticket_Updated_Dtm, Response_Object, Insert_Dtm FROM tbl_MKTECommTicketDetail
                                        ;
                                        TRUNCATE TABLE tbl_MKTECommTicketDetail";

                // first query to get orders header info
                // IMPORTANT: Use order table for order_id and transactionid, otherwise transactions get broken out differently
                // only get orders that have tickets, max() is used on order columns because there should only be one order per grouping
                string queryTicketOrders = @"select o.order_id, o.order_email, o.Attending_Patron_Account_id, o.coupon, 
                        max(o.transactionid) as transactionid,
                        sum(t.ticket_price) as TicketDetailTotalPrice,
                        max(o.Tickets_Value) as TicketsValue, max(o.UpSells_Value) as UpSellsValue, sum(t.SALES_TAX) as SalesTax, max(o.Order_Date) as OrderDate, max(o.Insert_Dtm) as InsertDtm, max(o.last_updated_dtm) as LastUpdatedDtm
                        from tbl_MKTECommTicketDetail t
                        inner join tbl_MKTECommOrderInfo o on t.order_id = o.order_id
                        where o.order_update_status = 'P'
                        group by o.order_id, o.order_email, o.Attending_Patron_Account_id, o.coupon
                        order by o.order_id, o.order_email, o.Attending_Patron_Account_id, o.coupon";
                //having t.order_id in (42761910,42761811,)";

                // second query runs for each order to get product list
                string queryTicketDetails = @"select t.buyer_type_code, t.buyer_type_desc, t.buyer_type_group_id, t.ticket_price,
                        SUM(t.ticket_count) as TicketCount, MAX(t.ticket_price) as TicketPrice
                        from tbl_MKTECommTicketDetail t
                        where Order_Id = <Order_Id>
                        group by t.buyer_type_code, t.buyer_type_desc, t.buyer_type_group_id, t.ticket_price";

                string queryOrdersSelectPart1 = @"WITH FirstEmailPerOrder AS (
                                                    SELECT ORDER_ID, EMAIL, ATTENDING_PATRON_ACCOUNT_ID, DELIVERY_METHOD_CODE
                                                    FROM (
                                                        SELECT
                                                            t.ORDER_ID,
                                                            d.EMAIL,
                                                            d.ATTENDING_PATRON_ACCOUNT_ID,
                                                            dm.DELIVERY_METHOD_CODE,
                                                            ROW_NUMBER() OVER (
                                                                PARTITION BY t.ORDER_ID
                                                                ORDER BY d.CREATED_DATE ASC, d.DELIVERY_ID ASC
                                                            ) AS rn
                                                        FROM `tdc-replication.medieval_times.ticket` t
                                                        JOIN `tdc-replication.medieval_times.ticket_delivery` td
                                                            ON td.TICKET_ID = t.TICKET_ID
                                                        AND td.REMOVE_TRANSACTION_ID IS NULL
                                                        JOIN `tdc-replication.medieval_times.delivery` d
                                                            ON d.DELIVERY_ID = td.DELIVERY_ID
                                                        AND d.DELIVERY_STATUS_CODE <> 'C'
                                                        JOIN `tdc-replication.medieval_times.delivery_method` dm
                                                            ON dm.DELIVERY_METHOD_ID = d.DELIVERY_METHOD_ID
                                                        WHERE dm.DELIVERY_TYPE_CODE = 'ETH'
                                                    )
                                                    WHERE rn = 1
                                                ), cte_filtered_events AS (
                                                    SELECT EVENT_ID, EVENT_DATE, EVENT_CATEGORY_ID
                                                    FROM `tdc-replication.medieval_times.event`
                                                    WHERE EVENT_CODE NOT LIKE '%GIFTCERT%'
                                                    AND EVENT_CODE NOT LIKE '%BULK%'
                                                )
                                                SELECT MAX(a.TRANSACTION_ID) as MAX_TRANSACTION_ID,  MAX(a.LAST_UPDATED_DATE) as LAST_UPDATED_DATE, b.ORDER_ID, fe.EMAIL, 
                                                            fe.ATTENDING_PATRON_ACCOUNT_ID,  
                                                            fe.DELIVERY_METHOD_CODE, o.SUPPLIER_ID, 'N' as CELEBRATING, 
                                                            MAX(CASE WHEN p.PRICE_SCALE_CODE <> 'NONADM' THEN CONCAT(p.PUBLIC_DESCRIPTION, CASE WHEN p.PUBLIC_DESCRIPTION <> 'General Admission' THEN ' Upgrade' ELSE '' END)  ELSE NULL END) AS PACKAGE_TYPE,
                                                            SUM(CASE WHEN (bt.DESCRIPTION NOT LIKE ('%Child%') AND p.PRICE_SCALE_CODE <> 'NONADM') THEN 1 ELSE 0 END) AS ADULT_TICKETS,
                                                            SUM(CASE WHEN (bt.DESCRIPTION LIKE ('%Child%') AND p.PRICE_SCALE_CODE <> 'NONADM')THEN 1 ELSE 0 END) AS CHILD_TICKETS,
                                                            SUM(CASE WHEN (bt.DISPLAY_INDICATOR = 'A' AND p.PRICE_SCALE_CODE IN ('GA','ROYAL','CELEB','KINGS','QUEENS')) THEN PRICE ELSE 0.00 END) + 
                                                                SUM(CASE WHEN (bt.DISPLAY_INDICATOR = 'C' AND p.PRICE_SCALE_CODE IN ('GA','ROYAL','CELEB','KINGS','QUEENS')) THEN PRICE ELSE 0.00 END) AS TICKET_VALUE,
                                                            SUM(
                                                                CASE    WHEN p.PRICE_SCALE_CODE = 'ROYAL' AND bt.DESCRIPTION NOT LIKE '%Military%' THEN 15.00
                                                                        WHEN p.PRICE_SCALE_CODE = 'CELEB' AND bt.DESCRIPTION NOT LIKE '%Military%' THEN 22.00
                                                                        WHEN p.PRICE_SCALE_CODE IN ('QUEENS','KINGS') AND bt.DESCRIPTION NOT LIKE '%Military%' THEN 27.00
                                                                ELSE 0 END) AS PACKAGE_VALUE,
                                                            IFNULL(MAX(COUPON_CODE),'') as COUPON,
                                                            SUM(CASE WHEN p.PRICE_SCALE_CODE = 'NONADM' AND bt.DISPLAY_INDICATOR <> 'T' THEN PRICE ELSE 0.00 END) AS UPSELLS_VALUE, 
                                                            '' as UPSELLS_DATA,
                                                            o.ORDER_DATE,
                                                            e.EVENT_DATE,
                                                            CASE WHEN ac.ACCOUNT_TYPE_CODE = 'IND' THEN 'IND' ELSE act.PATRON_ACCOUNT_TYPE_CODE END AS GUEST_TYPE,
                                                            ec.EVENT_CATEGORY_CODE as EVENT_TYPE,
                                                            ag.DESCRIPTION as Agency
                                                FROM	
                                                (SELECT ORDER_ID, order_line_item_id, TRANSACTION_ID, EVENT_ID FROM `tdc-replication.medieval_times.order_line_item` WHERE ORDER_ID IN  ";
                string queryOrdersSelectPart2All = @"   (SELECT ORDER_ID FROM `tdc-replication.medieval_times.order_line_item` WHERE Transaction_Id IN 
                                                            (SELECT TRANSACTION_ID FROM `tdc-replication.medieval_times.transaction`  WHERE 
                                                            bq_last_updated_date > DATETIME_SUB(DATETIME('<Last_Updated_Dtm>'), INTERVAL 5 MINUTE)) )
                                                        ) b
                                                    ";
                string queryOrdersSelectPart2Override = @"   (SELECT ORDER_ID FROM `tdc-replication.medieval_times.order_line_item` WHERE Transaction_Id IN (<TRANSACTIONLIST>)
                                                        )
                                                    ) b
                                                    ";

                string queryOrdersSelectPart3 = @" INNER JOIN  `tdc-replication.medieval_times.transaction` a
                                                    ON b.TRANSACTION_ID = a.TRANSACTION_ID
                                                    INNER JOIN  cte_filtered_events e 
                                                    ON          b.EVENT_ID = e.EVENT_ID 
                                                    INNER JOIN  `tdc-replication.medieval_times.event_category` ec
                                                    ON          e.EVENT_CATEGORY_ID = ec.EVENT_CATEGORY_ID
                                                    INNER JOIN  `tdc-replication.medieval_times.patron_order` o 
                                                    ON          b.ORDER_ID = o.ORDER_ID 
                                                    INNER JOIN  `tdc-replication.medieval_times.agency` ag
                                                    ON          o.CREATED_BY_AGENCY_ID = ag.AGENCY_ID
                                                    INNER JOIN  `tdc-replication.medieval_times.patron_account` act 
                                                    ON          o.FINANCIAL_PATRON_ACCOUNT_ID = act.PATRON_ACCOUNT_ID 
                                                    INNER JOIN	`tdc-replication.medieval_times.patron_account_type` ac
                                                    ON				act.PATRON_ACCOUNT_TYPE_CODE = ac.PATRON_ACCOUNT_TYPE_CODE  
                                                    INNER JOIN  `tdc-replication.medieval_times.ticket` t   
                                                    ON          b.order_line_item_id = t.order_line_item_id AND   
                                                                t.remove_order_line_item_id IS NULL 
                                                    LEFT JOIN   `tdc-replication.medieval_times.coupon` cp
                                                    ON          t.COUPON_ID = cp.COUPON_ID
                                                    INNER JOIN	`tdc-replication.medieval_times.price_scale` p
                                                    ON			p.PRICE_SCALE_ID = t.PRICE_SCALE_ID
                                                    INNER JOIN	`tdc-replication.medieval_times.buyer_type` bt
                                                    ON			t.BUYER_TYPE_ID = bt.BUYER_TYPE_ID
                                                    /* Note: we no longer join DELIVERY/TD/DM here; we use the preselected values */
                                                    INNER JOIN FirstEmailPerOrder fe
                                                    ON          fe.ORDER_ID = b.ORDER_ID 
                                                    GROUP BY    b.ORDER_ID, o.SUPPLIER_ID, fe.EMAIL, fe.ATTENDING_PATRON_ACCOUNT_ID, fe.DELIVERY_METHOD_CODE, -- p.PRICE_SCALE_CODE, 
                                                                CASE WHEN ac.ACCOUNT_TYPE_CODE = 'IND' THEN 'IND' ELSE act.PATRON_ACCOUNT_TYPE_CODE END, 
                                                                o.ORDER_DATE,
                                                                e.EVENT_DATE,
                                                                ec.EVENT_CATEGORY_CODE,
                                                                ag.DESCRIPTION";

                string queryOrdersSelectString = queryOrdersSelectPart1 + queryOrdersSelectPart2All + queryOrdersSelectPart3;

                string queryOrdersSelectWithOverrideString = queryOrdersSelectPart1 + queryOrdersSelectPart2Override + queryOrdersSelectPart3;

                string queryOrdersVerifyString = "SELECT COUNT(*) as PreviousOrderCount FROM tbl_MKTECommOrderInfo WHERE TransactionId = <VALUE>  " +
                                                " AND Last_Updated_Dtm BETWEEN dateadd(second,-1,'<Last_Updated_Dtm>') AND " +
                                                " dateadd(second,1,'<Last_Updated_Dtm>')";

                string queryAccountsToUpdateString = @"SELECT TransactionId, Order_Id, AC_ID, LocationDesc as Castle, Celebrating, Adult_Tickets + Child_Tickets as Number_Of_Tickets, Adult_Tickets, Child_Tickets, Tickets_Value, Coupon, Package_Type, Package_Value, 
                                                    Package_Type, Upsells_Value, Upsells_Data, Order_Date, Event_Date,  Guest_Type, Event_Type, Agency
                                                    FROM  tbl_MKTECommOrderInfo a
                                                    INNER JOIN fusion.dbo.tbl_location b ON a.SUPPLIER_ID = b.SupplierId
                                                    WHERE Order_Update_Status = 'P' AND AC_Exists = 'Y' AND TRIM(IFNULL(AC_Active_List,'')) <> ''";

                string queryOrderIdsListString = @"SELECT DISTINCT Order_Id 
                                                    FROM  tbl_MKTECommOrderInfo
                                                    WHERE Order_Update_Status = 'P' AND AC_Exists = 'Y' AND TRIM(IFNULL(AC_Active_List,'')) <> ''";

                string queryOrdersInsertString = "INSERT INTO tbl_MKTECommOrderInfo(TransactionId,Last_Updated_Dtm,Order_Id,Order_Email,Delivery_Type,SUPPLIER_ID,Celebrating,Adult_Tickets,Child_Tickets,Tickets_Value,Coupon,Package_Type,Package_Value,Upsells_Value,Upsells_Data,Order_Date,Event_Date,Guest_Type,Event_Type,Agency,Attending_Patron_Account_Id) SELECT <VALUES>";

                string queryTicketsInsertString = "INSERT INTO tbl_MKTECommTicketDetail(TRANSACTION_ID, EVENT_ID, EVENT_CODE, EVENT_DATE_TIME, SUPPLIER_ID, PATRON_ACCOUNT_ID, ORDER_ID, PRICE_SCALE, BUYER_TYPE_CODE, BUYER_TYPE_DESC, BUYER_TYPE_GROUP_ID, REPORT_BUYER_TYPE_GROUP_ID, DISPLAY_INDICATOR, TAX_EXEMPT, TICKET_ID, PAYMENT_STATUS_CODE, TICKET_PRICE, CONV_FEE, SALES_TAX, GRATUITY, ALLOCATION, INC_SALES_TAX, TICKET_COUNT) SELECT <VALUES>";


                string queryUpdateOrdersToSkip = "UPDATE tbl_MKTECommOrderInfo SET Order_Update_Status = 'X' WHERE Order_Update_Status = 'P'; " +
                                                 "UPDATE tbl_MKTECommOrderInfo SET Order_Update_Status = 'S' WHERE Order_Update_Status = 'N' AND SUPPLIER_ID NOT IN " +
                                                    "(SELECT SupplierId FROM fusion.dbo.tbl_location WHERE EmailActive = 'Y'); " +
                                                    "UPDATE tbl_MKTECommOrderInfo SET Order_Update_Status = 'P' WHERE Order_Update_Status = 'N' AND SUPPLIER_ID IN " +
                                                    "(SELECT SupplierId FROM fusion.dbo.tbl_location WHERE EmailActive = 'Y')";

                string queryOrdersToProcessString = @"SELECT TransactionId,Last_Updated_Dtm,Order_Id,Order_Email,Delivery_Type,Celebrating,Adult_Tickets,Child_Tickets,Tickets_Value,Coupon,Package_Type,Package_Value,Upsells_Value,Upsells_Data,Order_Date,Event_Date,Guest_Type,Event_Type,Agency, LocationDesc as Castle
                                                    FROM (SELECT * FROM tbl_MKTECommOrderInfo WHERE Order_Update_Status = 'P') a 
                                                    INNER JOIN fusion.dbo.tbl_location b ON a.SUPPLIER_ID = b.SupplierId;";

                string queryOrderUpdateString = "UPDATE tbl_MKTECommOrderInfo SET Order_Update_Status = '<Order_Update_Status>', Order_Updated_Dtm = CURRENT_TIMESTAMP, Response_Object = '<Response_Object>' WHERE TransactionId = <MaxTransactionId>";

                string queryTicketUpdateString = "UPDATE tbl_MKTECommTicketDetail SET Ticket_Update_Status = '<Ticket_Update_Status>', Ticket_Updated_Dtm = CURRENT_TIMESTAMP, Response_Object = '<Response_Object>' WHERE Ticket_ID = <Ticket_Id>";


                string queryOrderUpdatePostTypeString = "UPDATE tbl_MKTECommOrderInfo SET Order_Post_Type = '<Order_Post_Type>', Order_Updated_Dtm = CURRENT_TIMESTAMP, Response_Object = '<Response_Object>' WHERE TransactionId = <MaxTransactionId>";

                string queryOrderACDataString = "UPDATE tbl_MKTECommOrderInfo SET AC_Exists ='<AC_Exists>', AC_ID = <AC_ID>, AC_Active_List = '<AC_Active_List>' WHERE TransactionId = <MaxTransactionId>";

                string queryRecordsMaintenanceString = "DELETE FROM tbl_MKTECommTicketDetailArchive WHERE Archived_Date_Time < dateadd(day,-<ConfirmationTrimInterval>,getdate()); " +

                                                        "DELETE FROM tbl_MKTECommOrderInfo WHERE Last_Updated_Dtm < dateadd(day,-<ConfirmationTrimInterval>,getdate()); " +

                                                        "DELETE FROM tbl_MKTECommOrderInfoLog WHERE LogDate < dateadd(day,-<ConfirmationTrimInterval>,getdate()); ";


                using (SqlConnection emlConnection = new SqlConnection(connString))
                {
                    try
                    {
                        if (loggingSettings.Debug) WriteConsoleMessage("Opening connection to EML", connString);
                        emlConnection.Open();

                        SqlCommand cmdRecordsMaintenance = new SqlCommand(queryRecordsMaintenanceString.Replace("<ConfirmationTrimInterval>", loggingSettings.ConfirmationTrimInterval), emlConnection);
                        cmdRecordsMaintenance.ExecuteNonQuery();

                        string maxLast_Updated_DtmString = string.Empty;

                        if (!loggingSettings.OverrideTranDate)
                        {
                            SqlDataAdapter daMaxTransactionId = new SqlDataAdapter("SELECT MAX(Last_Updated_Dtm) as MaxLast_Updated_Dtm FROM tbl_MKTECommOrderInfo", emlConnection);
                            DataSet dsMaxTransactionId = new DataSet();
                            daMaxTransactionId.Fill(dsMaxTransactionId);


                            foreach (DataRow r in dsMaxTransactionId.Tables[0].Rows)
                            {
                                maxLast_Updated_DtmString = r["MaxLast_Updated_Dtm"].ToString();
                            }
                        }
                        else
                        {
                            maxLast_Updated_DtmString = loggingSettings.OverrideTranDateValue.ToString("yyyy-MM-dd HH:mm:ss");
                        }

                        if (!loggingSettings.ProcessOrdersTableOnly)
                        {
                            var bq = new BigQueryHelper(root);
                            try
                            {
                                int rowcount = 0;
                                int totalRowcount = 0;
                                string ordersString = string.Empty;

                                if (loggingSettings.Debug) WriteConsoleMessage("Executing orders query in PV for ActiveCampaign updates", connString);


                                var ordersSql =
                                    loggingSettings.TransactionIdOverride ? queryOrdersSelectWithOverrideString.Replace("<TRANSACTIONLIST>", loggingSettings.TransactionList) :
                                    queryOrdersSelectString.Replace("<Last_Updated_Dtm>", maxLast_Updated_DtmString);

                                var bqResults = await bq.FetchTableAsync("tdc-replication", ordersSql);
                                // now extract to DataTable/DataSet
                                DataTable dtOrders = new DataTable("BigQueryData");

                                // create columns dynamically from schema with type mapping
                                foreach (var field in bqResults.Schema.Fields)
                                {
                                    Type columnType = BigQueryHelper.MapBigQueryTypeToDotNet(field.Type);
                                    dtOrders.Columns.Add(field.Name, columnType);
                                }
                                // add data
                                foreach (var row in bqResults)
                                {
                                    var dataRow = dtOrders.NewRow();
                                    foreach (var field in bqResults.Schema.Fields)
                                    {
                                        dataRow[field.Name] = BigQueryHelper.ConvertBigQueryValue(row[field.Name], field.Type);
                                    }
                                    dtOrders.Rows.Add(dataRow);
                                }

                                SqlCommand cmdInsertOrder = new SqlCommand();
                                cmdInsertOrder.Connection = emlConnection;
                                string orderValuesString = string.Empty;

                                if (loggingSettings.Debug) WriteConsoleMessage("Inserting orders into tbl_MKTECommOrderInfo", connString);
                                foreach (DataRow r in dtOrders.Rows)
                                {
                                    // allow order to be re-processed if overriding transaction id list
                                    var bProcessOrder = true;
                                    if (!loggingSettings.TransactionIdOverride)
                                    {
                                        bProcessOrder = false;
                                        SqlDataAdapter daVerifyOrder = new SqlDataAdapter(queryOrdersVerifyString.Replace("<VALUE>", r["MAX_TRANSACTION_ID"].ToString()).Replace("<Last_Updated_Dtm>", r["LAST_UPDATED_DATE"].ToString()), emlConnection);
                                        DataSet dsVerifyOrder = new DataSet();
                                        daVerifyOrder.Fill(dsVerifyOrder);
                                        DataRow r2 = dsVerifyOrder.Tables[0].Rows[0];
                                        if (0 == int.Parse(r2["PreviousOrderCount"].ToString()))
                                        {
                                            bProcessOrder = true;
                                        }
                                    }

                                    if (bProcessOrder)
                                    {
                                        if (loggingSettings.Debug) WriteConsoleMessage("Inserting TransactionId " + r["MAX_TRANSACTION_ID"].ToString(), connString);

                                        orderValuesString =
                                            r["MAX_TRANSACTION_ID"] + ", " +
                                            "'" + (r["LAST_UPDATED_DATE"]) + "', " +
                                            r["ORDER_ID"] + ", " +
                                            "'" + r["EMAIL"].ToString().Replace("'", "''") + "', " +
                                            "'" + r["DELIVERY_METHOD_CODE"] + "', " +
                                            r["SUPPLIER_ID"] + ", " +
                                            "'" + r["CELEBRATING"] + "', " +
                                            r["ADULT_TICKETS"] + ", " +
                                            r["CHILD_TICKETS"] + ", " +
                                            r["TICKET_VALUE"] + ", " +
                                            "'" + r["COUPON"] + "', " +
                                            "'" + r["PACKAGE_TYPE"].ToString().Replace("'", "''") + "', " +
                                            r["PACKAGE_VALUE"] + ", " +
                                            r["UPSELLS_VALUE"] + ", " +
                                            "'" + r["UPSELLS_DATA"].ToString().Replace("'", "''") + "', " +
                                            "'" + (r["ORDER_DATE"]) + "', " +
                                            "'" + (r["EVENT_DATE"]) + "', " +
                                            "'" + r["GUEST_TYPE"] + "', " +
                                            "'" + r["EVENT_TYPE"] + "', " +
                                            "'" + r["Agency"] + "'," +
                                            r["ATTENDING_PATRON_ACCOUNT_ID"];

                                        cmdInsertOrder.CommandText = queryOrdersInsertString.Replace("<VALUES>", orderValuesString);
                                        cmdInsertOrder.ExecuteNonQuery();
                                    }
                                    else
                                    {
                                        if (loggingSettings.Debug) WriteConsoleMessage($"Skipping TransactionId {r["MAX_TRANSACTION_ID"].ToString()} because it was already processed", connString);
                                    }
                                }
                                if (loggingSettings.Debug) WriteConsoleMessage("Finished inserting orders into tbl_MKTECommOrderInfo", connString);

                                if (loggingSettings.Debug) WriteConsoleMessage("Update orders to skip if location not active", connString);
                                SqlCommand cmdUpdateOrdersToSkip = new SqlCommand(queryUpdateOrdersToSkip, emlConnection);
                                cmdUpdateOrdersToSkip.ExecuteNonQuery();
                            }
                            catch (Exception ex)
                            {
                                WriteConsoleMessage(ex.Message, connString);
                            }
                            

                            try
                            {
                                if (loggingSettings.Debug) WriteConsoleMessage("Getting orders to process", connString);

                                SqlDataAdapter daOrdersToProcess = new SqlDataAdapter(queryOrdersToProcessString, emlConnection);
                                DataSet dsOrdersToProcess = new DataSet();
                                daOrdersToProcess.Fill(dsOrdersToProcess);

                                foreach (DataRow r in dsOrdersToProcess.Tables[0].Rows)
                                {

                                    string currentTransactionId = r["TransactionId"].ToString();
                                    bool isActive = false;

                                    try
                                    {
                                        if (loggingSettings.Debug) WriteConsoleMessage($"Checking existence and status of {r["Order_Email"].ToString()} in ActiveCampaign", connString);

                                        var apiClient = new APIClient(acAPISettings, connString);
                                        string url = acAPISettings.URL;
                                        string emailAddress = r["Order_Email"].ToString();

                                        try
                                        {
                                            ApiResponse response = await apiClient.GetContactAsync(url, emailAddress);

                                            if (response.IsSuccess)
                                            {
                                                ACContactWithLists contactWithLists = JsonSerializer.Deserialize<ACContactWithLists>(response.Content, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

                                                if (contactWithLists != null && contactWithLists.Contacts.Count > 0)
                                                {
                                                    string listsWithStatusOne = string.Empty;

                                                    listsWithStatusOne = string.Join(",",
                                                                                    contactWithLists.ContactLists
                                                                                    .Where(cl => cl.Status == "1")
                                                                                    .Select(cl => cl.List)
                                                                                    );

                                                    string ac_id = string.Empty;

                                                    if (listsWithStatusOne != null && listsWithStatusOne != string.Empty)
                                                    {
                                                        ac_id = contactWithLists.ContactLists.FirstOrDefault(cl => cl.Status == "1")?.Contact;
                                                        if (loggingSettings.Debug) WriteConsoleMessage($"Found {r["Order_Email"].ToString()} in ActiveCampaign with Id = {ac_id} active on at least one list", connString);
                                                        SqlCommand daACData = new SqlCommand(queryOrderACDataString.Replace("<AC_Exists>", "Y").Replace("<AC_ID>", ac_id).Replace("<AC_Active_List>", listsWithStatusOne).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                        daACData.ExecuteNonQuery();
                                                        isActive = true;
                                                    }
                                                    else
                                                    {
                                                        ac_id = contactWithLists.Contacts.FirstOrDefault()?.Id;
                                                        if (loggingSettings.Debug) WriteConsoleMessage($"Found {r["Order_Email"].ToString()} in ActiveCampaign with Id = {ac_id} but no active lists", connString);
                                                        SqlCommand daACData = new SqlCommand(queryOrderACDataString.Replace("<AC_Exists>", "Y").Replace("<AC_ID>", ac_id).Replace("<Order_Update_Status>", "S").Replace("<AC_Active_List>", "").Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                        daACData.ExecuteNonQuery();
                                                        isActive = false;
                                                    }
                                                }
                                                else
                                                {
                                                    string jsonResponse = JsonSerializer.Serialize(contactWithLists);

                                                    SqlCommand daACData = new SqlCommand(queryOrderACDataString.Replace("<AC_Exists>", "N").Replace("<AC_ID>", "0").Replace("<AC_Active_List>", string.Empty).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                    daACData.ExecuteNonQuery();

                                                    SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "S").Replace("<Response_Object>", jsonResponse).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                    daOrderUpdateData.ExecuteNonQuery();

                                                    isActive = false;
                                                }
                                            }
                                            else
                                            {
                                                var responseContent = $"API error status: {response.StatusCode} {response.Content} {response.ErrorMessage}";
                                                WriteConsoleMessage($"Could not get contact for transactionid {currentTransactionId}", connString);
                                                SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", responseContent).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                daOrderUpdateData.ExecuteNonQuery();
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            try
                                            {
                                                if (int.TryParse(currentTransactionId, out int id))
                                                {
                                                    SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", ex.Message).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                    daOrderUpdateData.ExecuteNonQuery();
                                                }
                                            }
                                            catch (Exception ex2) { WriteConsoleMessage($"We ignored an error within a catch block.  {ex2.Message}", connString); }

                                            if (loggingSettings.Debug) WriteConsoleMessage($"An error occured while processing transaction ID {currentTransactionId}.  {ex.Message}", connString);
                                            WriteConsoleMessage(ex.Message, connString);
                                            isActive = false;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        if (loggingSettings.Debug) WriteConsoleMessage("An error occured while constructing outgoing request for AC_ID.  See next record if an exception message is available.", connString);
                                        WriteConsoleMessage(ex.Message, connString);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                WriteConsoleMessage(ex.Message, connString);
                            }

                        }
                        else
                        {
                            if (loggingSettings.Debug) WriteConsoleMessage("Skipped checking PV for orders due to runtime settings.  Check appSettings.json if this is unintended.", connString);
                        }

                        // at this point all order statuses have been applied:
                        // Order_Update_Status
                        // N = Not touched yet
                        // P = Need to be processed
                        // Y = Processed successfully
                        // Z = Order create returned success, but could not get order info from response
                        // X = was a P but didn't get processed
                        // S = Skipped
                        // E = error 


                        if (!loggingSettings.ProcessTicketsTableOnly)
                        {
                            // archive the current Sql ticket detail table and truncate
                            SqlCommand cmdArchive = new SqlCommand(queryTicketsArchive, emlConnection);
                            cmdArchive.ExecuteNonQuery();

                            // get order id list
                            SqlDataAdapter daOrderIdList = new SqlDataAdapter(queryOrderIdsListString, emlConnection);
                            DataSet dsOrderIdList = new DataSet();
                            daOrderIdList.Fill(dsOrderIdList);

                            // turn dataset above into comma-separated list of order id's with single quotes
                            string orderIdListString = string.Join(",",
                                dsOrderIdList.Tables[0].AsEnumerable()
                                .Select(row => $"'{row[0].ToString()}'")
                            );

                            if (!String.IsNullOrEmpty(orderIdListString))
                            {
                                queryTicketsSelectString = queryTicketsSelectString.Replace("<Order_Id_List>", orderIdListString);

                                using (OracleConnection pvConnection = new OracleConnection(databaseSettings.PVConnectionString))
                                {
                                    try
                                    {

                                        if (loggingSettings.Debug) WriteConsoleMessage("Executing tickets query in PV for ActiveCampaign updates", connString);

                                        OracleDataAdapter daTickets = new OracleDataAdapter(
                                            //loggingSettings.TransactionIdOverride ? queryOrdersSelectWithOverrideString.Replace("<TRANSACTIONLIST>", loggingSettings.TransactionList) :
                                            queryTicketsSelectString.Replace("<Order_Id_List>", orderIdListString),
                                                pvConnection);

                                        DataSet dsTickets = new DataSet();
                                        daTickets.SelectCommand.CommandTimeout = 900;
                                        daTickets.Fill(dsTickets);

                                        SqlCommand cmdInsertTicket = new SqlCommand();
                                        cmdInsertTicket.Connection = emlConnection;
                                        string ticketValuesString = string.Empty;

                                        if (loggingSettings.Debug) WriteConsoleMessage("Inserting tickets into tbl_MKTECommTicketDetail", connString);
                                        foreach (DataRow r in dsTickets.Tables[0].Rows)
                                        {

                                            if (loggingSettings.Debug) WriteConsoleMessage("Inserting TransactionId to TicketDetails " + r["TRANSACTION_ID"].ToString(), connString);

                                            ticketValuesString =
                                                r["TRANSACTION_ID"] + ", " +
                                                r["EVENT_ID"] + ", " +
                                                "'" + r["EVENT_CODE"] + "', " +
                                                "STR_TO_DATE('" + (r["EVENT_DATE_TIME"]) + "', '%m/%d/%Y %h:%i:%s %p'), " +
                                                r["Supplier_Id"] + ", " +
                                                r["PATRON_ACCOUNT_ID"] + ", " +
                                                r["ORDER_ID"] + ", " +
                                                "'" + r["PRICE_SCALE"] + "', " +
                                                "'" + r["BUYER_TYPE_CODE"] + "', " +
                                                "'" + r["BUYER_TYPE_DESC"].ToString().Replace("'", "''") + "', " +
                                                r["BUYER_TYPE_GROUP_ID"] + ", " +
                                                (Convert.IsDBNull(r["REPORT_BUYER_TYPE_GROUP_ID"]) ? "0" : r["REPORT_BUYER_TYPE_GROUP_ID"].ToString()) + "," +
                                                "'" + r["DISPLAY_INDICATOR"] + "', " +
                                                r["TAX_EXEMPT"] + ", " +
                                                r["TICKET_ID"] + ", " +
                                                "'" + r["PAYMENT_STATUS_CODE"] + "', " +
                                                r["TICKET_PRICE"] + ", " +
                                                r["CONV_FEE"] + ", " +
                                                r["SALES_TAX"] + ", " +
                                                r["INC_SALES_TAX"] + ", " +
                                                r["GRATUITY"] + ", " +
                                                r["ALLOCATION"] + ", " +
                                                r["TICKET_COUNT"];

                                            cmdInsertTicket.CommandText = queryTicketsInsertString.Replace("<VALUES>", ticketValuesString);
                                            // temp log every query to find other null problem
                                            // if (loggingSettings.Debug) WriteConsoleMessage(cmdInsertTicket.CommandText, connString);

                                            cmdInsertTicket.ExecuteNonQuery();

                                        }
                                        if (loggingSettings.Debug) WriteConsoleMessage("Finished inserting tickets into tbl_MKTECommTicketDetail", connString);

                                    }
                                    catch (Exception ex)
                                    {
                                        WriteConsoleMessage($"Error inserting ticket details {ex.Message}", connString);
                                    }
                                }
                            }
                            else
                            {
                                WriteConsoleMessage($"No order ids found with tickets", connString);
                            }
                        }
                        else
                        {
                            if (loggingSettings.Debug) WriteConsoleMessage("Skipped getting Tickets for Orders due to runtime settings.  Check appSettings.json if this is unintended.", connString);
                        }


                        // build customer and order info for EComm API
                        // Order info has either 0 or 1 coupons - keep this together with header info
                        SqlDataAdapter daApiOrderList = new SqlDataAdapter(queryTicketOrders, emlConnection);
                        DataSet dsApiOrderList = new DataSet();
                        daApiOrderList.Fill(dsApiOrderList);

                        foreach (DataRow r in dsApiOrderList.Tables[0].Rows)
                        {
                            try
                            {
                                var apiClient = new APIClient(acAPISettings, connString);
                                string url = acAPISettings.URL;

                                if (loggingSettings.Debug) WriteConsoleMessage($"Checking existence of {r["Order_Email"].ToString()} as an Ecomm Customer", connString);

                                string currentTransactionId = r["transactionid"].ToString();
                                string orderId = r["order_id"].ToString();
                                string emailAddress = r["order_Email"].ToString();
                                string externalId = r["Attending_Patron_Account_id"].ToString();
                                string createdDate = r["OrderDate"] == DBNull.Value ? null : ((DateTime)r["OrderDate"]).ToString("yyyy-MM-ddTHH:mm:ss");
                                string updatedDate = r["LastUpdatedDtm"] == DBNull.Value ? null : ((DateTime)r["LastUpdatedDtm"]).ToString("yyyy-MM-ddTHH:mm:ss");

                                string connectionId = acAPISettings.ConnectionId;
                                string customerId = "";
                                bool customerExists = false;

                                try
                                {
                                    ApiResponse response = await apiClient.GetCustomerAsync(url, connectionId, emailAddress);

                                    if (response.IsSuccess)
                                    {
                                        // Success doesn't mean they exist, have to check array
                                        using JsonDocument doc = JsonDocument.Parse(response.Content);

                                        JsonElement customersElement = doc.RootElement.GetProperty("ecomCustomers");
                                        if (customersElement.ValueKind == JsonValueKind.Array && customersElement.GetArrayLength() != 0)
                                        {
                                            customerExists = true;
                                            JsonElement firstCustomer = customersElement[0]; // Or use EnumerateArray().First()
                                            if (firstCustomer.TryGetProperty("id", out JsonElement idElement))
                                            {
                                                customerId = idElement.GetString();
                                            }
                                        }
                                    }
                                    else
                                    {
                                        var responseContent = $"API error status: {response.StatusCode} {response.Content} {response.ErrorMessage}";
                                        WriteConsoleMessage(responseContent, connString);
                                        SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", responseContent).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                        daOrderUpdateData.ExecuteNonQuery();
                                        continue; // go to next row, do not process order if customer lookup failed
                                    }

                                    // create customer if it doesn't already exist
                                    if (!customerExists)
                                    {
                                        customerId = "";
                                        // add customer 
                                        var wrapper = new EcomCustomerWrapper
                                        {
                                            Customer = new EcomCustomer
                                            {
                                                ConnectionId = connectionId,
                                                ExternalId = externalId,
                                                Email = emailAddress
                                                // acceptsMarketing is already set to "1" by default
                                            }
                                        };

                                        // serialize to JSON
                                        // var options = new JsonSerializerOptions { WriteIndented = true };
                                        // string json = JsonSerializer.Serialize(wrapper, options);
                                        string json = JsonSerializer.Serialize(wrapper);
                                        WriteConsoleMessage($"Creating new ECom customer {json}", connString);

                                        ApiResponse createResponse = await apiClient.PostCustomerOrOrdersAsync("Create EComCustomer", url, json, "ecomCustomers"); // path is case sensitive
                                        if (createResponse.IsSuccess)
                                        {

                                            // Parse the string into a JsonDocument
                                            using JsonDocument doc = JsonDocument.Parse(createResponse.Content);

                                            // Navigate to the "ecomCustomer" object
                                            JsonElement custRoot = doc.RootElement;

                                            if (custRoot.TryGetProperty("ecomCustomer", out JsonElement ecomCustomer))
                                            {
                                                if (ecomCustomer.TryGetProperty("id", out JsonElement idElement))
                                                {
                                                    customerId = idElement.GetString();
                                                }
                                            }
                                            WriteConsoleMessage($"Customer id {customerId} created", connString);
                                        }
                                        else
                                        {
                                            var createResponseContent = $"API error status: {createResponse.StatusCode} {createResponse.Content} {createResponse.ErrorMessage}";
                                            SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", createResponseContent).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                            daOrderUpdateData.ExecuteNonQuery();
                                            continue; // go to next row, do not process order if customer could not be created
                                        }

                                    }

                                    // double check that we have a valid customer id assigned
                                    if (string.IsNullOrEmpty(customerId))
                                    {
                                        WriteConsoleMessage($"Customer id is blank for order {orderId}", connString);
                                        SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", $"Customer id is blank for order {orderId}").Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                        daOrderUpdateData.ExecuteNonQuery();
                                        continue; // do not process if we don't have a valid customer
                                    }

                                    WriteConsoleMessage($"Checking existence of order id {orderId} in Ecomm.", connString);

                                    // see if external order id already exists
                                    var existingOrderId = "-1";
                                    var processOrderToEcomm = false;
                                    ApiResponse checkOrderResponse = await apiClient.GetOrderAsync(url, connectionId, orderId);
                                    if (checkOrderResponse.IsSuccess)
                                    {
                                        // Parse the string into a JsonDocument
                                        using var doc = JsonDocument.Parse(checkOrderResponse.Content);
                                        // Navigate to the "ecomOrders" object
                                        var checkRoot = doc.RootElement;
                                        var orders = checkRoot.GetProperty("ecomOrders");
                                        if (orders.GetArrayLength() > 0)
                                        {
                                            var returnedExternalId = orders[0].GetProperty("externalid").GetString();
                                            if (returnedExternalId == orderId)
                                            {
                                                processOrderToEcomm = true;
                                                existingOrderId = orders[0].GetProperty("id").GetString();
                                                WriteConsoleMessage($"ExternalOrderId {orderId} found with Ecom order id {existingOrderId}, order will be updated.", connString);

                                                SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdatePostTypeString.Replace("<Order_Post_Type>", "U").Replace("<Response_Object>", checkOrderResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                daOrderUpdateData.ExecuteNonQuery();
                                            }
                                            else
                                            {
                                                WriteConsoleMessage($"Error: Returned ExternalId {returnedExternalId} does not match current externalId {orderId}", connString);
                                                SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", checkOrderResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                daOrderUpdateData.ExecuteNonQuery();
                                            }

                                        }
                                        else
                                        {
                                            processOrderToEcomm = true;
                                            WriteConsoleMessage($"External Id {orderId} not found, creating new.", connString);
                                            SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdatePostTypeString.Replace("<Order_Post_Type>", "I").Replace("<Response_Object>", checkOrderResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                            daOrderUpdateData.ExecuteNonQuery();
                                        }
                                    }
                                    else
                                    {
                                        var checkOrderResponseContent = $"API error status: {checkOrderResponse.StatusCode} {checkOrderResponse.Content} {checkOrderResponse.ErrorMessage}";
                                        SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", checkOrderResponseContent).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                        daOrderUpdateData.ExecuteNonQuery();
                                        continue; // go to next row, do not continue if check for existing order failed
                                    }

                                    if (processOrderToEcomm)
                                    {
                                        // at this point we have a customerid (either from lookup or create)
                                        // create the order products, discount and header info

                                        // Get tickets details for the order
                                        // Each product on the order comes from ticket buyer_type_code
                                        var orderProducts = new List<OrderProduct>();
                                        var orderTicketDetails = queryTicketDetails.Replace("<Order_Id>", orderId).Replace("<Transaction_Id>", currentTransactionId);
                                        SqlDataAdapter daApiTicketList = new SqlDataAdapter(orderTicketDetails, emlConnection);
                                        DataSet dsApiTicketList = new DataSet();
                                        daApiTicketList.Fill(dsApiTicketList);


                                        foreach (DataRow t in dsApiTicketList.Tables[0].Rows)
                                        {
                                            var orderProduct = new OrderProduct
                                            {
                                                ExternalId = t["buyer_type_code"].ToString(),
                                                Name = t["buyer_type_desc"].ToString(),
                                                Price = (int)((decimal)t["TicketPrice"] * 100),
                                                Quantity = (int)((decimal)t["TicketCount"]),
                                                //Category = t["buyer_type_group_id"].ToString(),
                                                //Sku = t["buyer_type_code"].ToString(),
                                                Description = t["buyer_type_desc"].ToString(),
                                                ImageUrl = "",
                                                ProductUrl = ""
                                            };
                                            orderProducts.Add(orderProduct);
                                        }

                                        // get coupon details if present
                                        var orderDiscounts = new List<OrderDiscount>();
                                        if (r["coupon"] != DBNull.Value && !String.IsNullOrEmpty(r["coupon"].ToString()))
                                        {
                                            var orderDiscount = new OrderDiscount();
                                            orderDiscount.Name = r["coupon"].ToString();
                                            orderDiscount.Type = "order";
                                            orderDiscount.DiscountAmount = 0;
                                            orderDiscounts.Add(orderDiscount);
                                        }
                                    ;

                                        // default order wrapper for insert
                                        var orderWrapper = new EcomOrderWrapper
                                        {
                                            EcomOrder = new EcomOrder
                                            {
                                                ExternalId = orderId,
                                                Source = "1",
                                                Email = emailAddress,
                                                ConnectionId = connectionId,
                                                CustomerId = customerId,
                                                OrderNumber = orderId,
                                                ExternalCreatedDate = createdDate,
                                                ExternalUpdatedDate = updatedDate,
                                                Currency = "USD",
                                                TotalPrice = (int)(((decimal)r["TicketDetailTotalPrice"] + (decimal)r["SalesTax"]) * 100),
                                                ShippingAmount = 0,
                                                TaxAmount = (int)((decimal)r["SalesTax"] * 100),
                                                DiscountAmount = 0,
                                                ShippingMethod = "",
                                                OrderUrl = "",
                                                OrderProducts = orderProducts,
                                                OrderDiscounts = orderDiscounts
                                            }
                                        };

                                        if (existingOrderId != "-1")
                                        {
                                            orderWrapper.EcomOrder.Source = null;
                                            orderWrapper.EcomOrder.ExternalCreatedDate = null;
                                            orderWrapper.EcomOrder.ConnectionId = null;
                                            orderWrapper.EcomOrder.CustomerId = null;
                                        }

                                        // now post the order
                                        // serialize to JSON
                                        //var orderOptions = new JsonSerializerOptions { WriteIndented = true };
                                        //string orderJson = JsonSerializer.Serialize(orderWrapper, orderOptions);
                                        var orderOptions = new JsonSerializerOptions { DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull };
                                        string orderJson = JsonSerializer.Serialize(orderWrapper, orderOptions);

                                        if (existingOrderId == "-1")
                                        {
                                            WriteConsoleMessage($"Creating new order - POST Order JSON: {orderJson}", connString);

                                            ApiResponse orderResponse = await apiClient.PostCustomerOrOrdersAsync("Create EComOrder", url, orderJson, "ecomOrders"); // path is case sensitive
                                            if (orderResponse.IsSuccess)
                                            {
                                                // Parse the string into a JsonDocument
                                                using JsonDocument doc = JsonDocument.Parse(orderResponse.Content);

                                                // Navigate to the "ecomOrder" object
                                                JsonElement orderRoot = doc.RootElement;

                                                if (orderRoot.TryGetProperty("ecomOrder", out JsonElement ecomOrder))
                                                {
                                                    if (ecomOrder.TryGetProperty("id", out JsonElement idElement))
                                                    {
                                                        var newOrderId = idElement.GetString();
                                                        WriteConsoleMessage($"Ecom order id {newOrderId} created with ExternalOrderId {orderId}", connString);

                                                        SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Y").Replace("<Response_Object>", orderResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                        daOrderUpdateData.ExecuteNonQuery();
                                                    }
                                                    else
                                                    {
                                                        WriteConsoleMessage($"Could not get id property from {orderResponse.Content}", connString);
                                                        SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Z").Replace("<Response_Object>", orderResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                        daOrderUpdateData.ExecuteNonQuery();
                                                    }

                                                }
                                                else
                                                {
                                                    WriteConsoleMessage($"Could not get ecomOrder property from {orderResponse.Content}", connString);
                                                    SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Z").Replace("<Response_Object>", orderResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                    daOrderUpdateData.ExecuteNonQuery();
                                                }
                                            }
                                            else
                                            {
                                                var orderResponseContent = $"API error status: {orderResponse.StatusCode} {orderResponse.Content} {orderResponse.ErrorMessage}";
                                                WriteConsoleMessage($"FAILED Order Post for ExternalOrderId: {orderId} Status: {orderResponse.StatusCode} ErrorMessage: {orderResponseContent}", connString);
                                                SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", orderResponseContent).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                daOrderUpdateData.ExecuteNonQuery();

                                            }
                                        }
                                        else
                                        {
                                            WriteConsoleMessage($"Updating existing order {existingOrderId} - PUT Order JSON: {orderJson}", connString);

                                            ApiResponse updateResponse = await apiClient.UpdateOrdersAsync(url, orderJson, "ecomOrders", existingOrderId); // path is case sensitive
                                            if (updateResponse.IsSuccess)
                                            {
                                                // Parse the string into a JsonDocument
                                                using JsonDocument doc = JsonDocument.Parse(updateResponse.Content);

                                                // Navigate to the "ecomOrder" object
                                                JsonElement orderRoot = doc.RootElement;

                                                if (orderRoot.TryGetProperty("ecomOrder", out JsonElement ecomOrder))
                                                {
                                                    if (ecomOrder.TryGetProperty("id", out JsonElement idElement))
                                                    {
                                                        var newOrderId = idElement.GetString();
                                                        if (newOrderId == existingOrderId)
                                                        {
                                                            WriteConsoleMessage($"Ecom order id {newOrderId} updated for ExternalOrderId {orderId}", connString);

                                                            SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Y").Replace("<Response_Object>", updateResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                            daOrderUpdateData.ExecuteNonQuery();
                                                        }
                                                        else
                                                        {
                                                            WriteConsoleMessage($"Updated order id {newOrderId} does not match order we attempted to update {existingOrderId}", connString);
                                                            SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Z").Replace("<Response_Object>", updateResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                            daOrderUpdateData.ExecuteNonQuery();
                                                        }
                                                    }
                                                    else
                                                    {
                                                        WriteConsoleMessage($"Could not get id property from {updateResponse.Content}", connString);
                                                        SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Z").Replace("<Response_Object>", updateResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                        daOrderUpdateData.ExecuteNonQuery();
                                                    }

                                                }
                                                else
                                                {
                                                    WriteConsoleMessage($"Could not get ecomOrder property from {updateResponse.Content}", connString);
                                                    SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Z").Replace("<Response_Object>", updateResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                    daOrderUpdateData.ExecuteNonQuery();
                                                }
                                            }
                                            else
                                            {
                                                var updateResponseContent = $"API error status: {updateResponse.StatusCode} {updateResponse.Content} {updateResponse.ErrorMessage}";
                                                WriteConsoleMessage($"FAILED to update order {existingOrderId} with ExternalOrderId: {orderId} Status: {updateResponse.StatusCode} ErrorMessage: {updateResponseContent}", connString);
                                                SqlCommand daOrderUpdateData = new SqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", updateResponse.ErrorMessage).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                daOrderUpdateData.ExecuteNonQuery();

                                            }
                                        }

                                    }

                                }
                                catch (Exception ex)
                                {

                                    if (loggingSettings.Debug) WriteConsoleMessage($"An error occured while processing Order ID {r["Order_Id"]} Email {r["Order_Email"]}  {ex.Message}", connString);
                                    WriteConsoleMessage(ex.Message, connString);

                                }
                            }
                            catch (Exception ex)
                            {
                                if (loggingSettings.Debug) WriteConsoleMessage("An error occured while constructing outgoing request to check for Customer.  See next record if an exception message is available.", connString);
                                WriteConsoleMessage(ex.Message, connString);
                            }
                        }




                        if (loggingSettings.Debug) WriteConsoleMessage("Completed calls to PV.  See previous lines for any errors that may have occurred.", connString);
                        WriteConsoleMessage("Ending Service", connString);
                    } // end try using (SqlConnection emlConnection = new SqlConnection(connString))
                    catch (Exception ex)
                    {
                        WriteConsoleMessage(ex.Message, connString);
                    }
                }

            }
        

        }
    }
}



