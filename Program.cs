using System;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text;
using System.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Oracle.ManagedDataAccess.Client;
using MySql.Data;
using MySql.Data.MySqlClient;
using ZstdNet;

namespace ACECommerce
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

        public APIClient(string apiToken)
        {
            _client = new HttpClient();
            _client.DefaultRequestHeaders.Add("User-Agent", "mt-activecampaign-agent/7.39.0");
            _client.DefaultRequestHeaders.Add("Accept", "application/json");
            _client.DefaultRequestHeaders.Add("Connection", "keep-alive");
            _client.DefaultRequestHeaders.Add("Api-Token", apiToken);
            _client.DefaultRequestHeaders.Add("Host", "medievaltimes.api-us1.com");
        }

        public async Task<ApiResponse> PostRecordAsync(string URL, string json, string path)
        {
            var url = $"https://{URL}/{path}";
            var jsonContent = new StringContent(json, Encoding.UTF8, "application/json");
            var requestMessage = new HttpRequestMessage(HttpMethod.Post, url);
            requestMessage.Content = jsonContent;

            requestMessage.Headers.Add("MT-Request-Token", Guid.NewGuid().ToString());

            try
            {
                HttpResponseMessage response = await _client.SendAsync(requestMessage);
                var apiResponse = new ApiResponse
                {
                    StatusCode = response.StatusCode,
                    IsSuccess = response.IsSuccessStatusCode
                };

                if (response.IsSuccessStatusCode)
                {
                    apiResponse.Content = await response.Content.ReadAsStringAsync();
                }
                else
                {
                    apiResponse.ErrorMessage = await response.Content.ReadAsStringAsync();
                }

                return apiResponse;


            }
            catch (HttpRequestException e)
            {

                Console.WriteLine($"Error: {e.Message}");
                return null;
            }
        }
        public async Task<ApiResponse> GetContactAsync(string URL, string emailAddress)
        {
            string url = $"https://{URL}/contacts?filters[email]={Uri.EscapeDataString(emailAddress)}&include=contactLists";

            var requestMessage = new HttpRequestMessage(HttpMethod.Get, url);

            requestMessage.Headers.Add("MT-Request-Token", Guid.NewGuid().ToString());

            try
            {
                HttpResponseMessage response = await _client.SendAsync(requestMessage);
                var apiResponse = new ApiResponse
                {
                    StatusCode = response.StatusCode,
                    IsSuccess = response.IsSuccessStatusCode
                };

                if (response.IsSuccessStatusCode)
                {
                    apiResponse.Content = await response.Content.ReadAsStringAsync();
                }
                else
                {
                    apiResponse.ErrorMessage = await response.Content.ReadAsStringAsync();
                }

                return apiResponse;

            }
            catch (HttpRequestException e)
            {
                Console.WriteLine($"Error: {e.Message}");
                return null;
            }
        }

        public async Task<ApiResponse> GetCustomerAsync(string URL, string connectionId, string emailAddress)
        {
            string url = $"https://{URL}/ecomCustomers?filters[connectionid]={connectionId}&filters[email]={Uri.EscapeDataString(emailAddress)}";

            var requestMessage = new HttpRequestMessage(HttpMethod.Get, url);

            requestMessage.Headers.Add("MT-Request-Token", Guid.NewGuid().ToString());

            try
            {
                HttpResponseMessage response = await _client.SendAsync(requestMessage);
                var apiResponse = new ApiResponse
                {
                    StatusCode = response.StatusCode,
                    IsSuccess = response.IsSuccessStatusCode
                };

                if (response.IsSuccessStatusCode)
                {
                    apiResponse.Content = await response.Content.ReadAsStringAsync();
                }
                else
                {
                    apiResponse.ErrorMessage = await response.Content.ReadAsStringAsync();
                }

                return apiResponse;

            }
            catch (HttpRequestException e)
            {
                //TODO: log this to more than the console and with more info!
                Console.WriteLine($"Error: {e.Message}");
                return null;
            }
        }

        public async Task<ApiResponse> GetOrderAsync(string URL, string connectionId, string externalId)
        {
            string url = $"https://{URL}/ecomOrders?filters[connectionid]={connectionId}&filters[externalid]={Uri.EscapeDataString(externalId)}";

            var requestMessage = new HttpRequestMessage(HttpMethod.Get, url);

            requestMessage.Headers.Add("MT-Request-Token", Guid.NewGuid().ToString());

            try
            {
                HttpResponseMessage response = await _client.SendAsync(requestMessage);
                var apiResponse = new ApiResponse
                {
                    StatusCode = response.StatusCode,
                    IsSuccess = response.IsSuccessStatusCode
                };

                if (response.IsSuccessStatusCode)
                {
                    apiResponse.Content = await response.Content.ReadAsStringAsync();
                }
                else
                {
                    apiResponse.ErrorMessage = await response.Content.ReadAsStringAsync();
                }

                return apiResponse;

            }
            catch (HttpRequestException e)
            {
                Console.WriteLine($"Error: {e.Message}");
                return null;
            }
        }

        public async Task<ApiResponse> PostCustomerOrOrdersAsync(string URL, string json, string path)
        {
            var url = $"https://{URL}/{path}";
            var jsonContent = new StringContent(json, Encoding.UTF8, "application/json");
            var requestMessage = new HttpRequestMessage(HttpMethod.Post, url);
            requestMessage.Content = jsonContent;

            requestMessage.Headers.Add("MT-Request-Token", Guid.NewGuid().ToString());

            try
            {
                HttpResponseMessage response = await _client.SendAsync(requestMessage);
                var apiResponse = new ApiResponse
                {
                    StatusCode = response.StatusCode,
                    IsSuccess = response.IsSuccessStatusCode
                };

                if (response.IsSuccessStatusCode)
                {
                    apiResponse.Content = await response.Content.ReadAsStringAsync();
                }
                else
                {
                    apiResponse.ErrorMessage = await response.Content.ReadAsStringAsync();
                }

                return apiResponse;


            }
            catch (HttpRequestException e)
            {

                Console.WriteLine($"Error: {e.Message}");
                return null;
            }
        }

        public async Task<ApiResponse> UpdateOrdersAsync(string URL, string json, string path, string existingOrderId)
        {
            var url = $"https://{URL}/{path}/{existingOrderId}";
            var jsonContent = new StringContent(json, Encoding.UTF8, "application/json");
            var requestMessage = new HttpRequestMessage(HttpMethod.Put, url);
            requestMessage.Content = jsonContent;

            requestMessage.Headers.Add("MT-Request-Token", Guid.NewGuid().ToString());

            try
            {
                HttpResponseMessage response = await _client.SendAsync(requestMessage);
                var apiResponse = new ApiResponse
                {
                    StatusCode = response.StatusCode,
                    IsSuccess = response.IsSuccessStatusCode
                };

                if (response.IsSuccessStatusCode)
                {
                    apiResponse.Content = await response.Content.ReadAsStringAsync();
                }
                else
                {
                    apiResponse.ErrorMessage = await response.Content.ReadAsStringAsync();
                }

                return apiResponse;


            }
            catch (HttpRequestException e)
            {

                Console.WriteLine($"Error: {e.Message}");
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

        private static void WriteConsoleMessage(string msg, string mySQLconnectionString)
        {
            WriteConsoleMessage(msg);

            using (MySqlConnection emlConnection = new MySqlConnection(mySQLconnectionString))
            {
                emlConnection.Open();
                // logic below corrects it so that msg values that contain single quotes still work
                using (MySqlCommand cmdWriteLog = new MySqlCommand("INSERT INTO tbl_MKTECommOrderInfoLog(LogData) VALUES (@msg)", emlConnection))
                {
                    cmdWriteLog.Parameters.AddWithValue("@msg", msg);
                    cmdWriteLog.ExecuteNonQuery();
                }
                // MySqlCommand cmdWriteLog = new MySqlCommand("INSERT INTO tbl_MKTECommOrderInfoLog(LogData) VALUES ('" + msg + "')", emlConnection);
                // cmdWriteLog.ExecuteNonQuery();
            }

        }

        private static void WriteConsoleMessage(string msg)
        {
            Console.WriteLine(string.Concat(DateTime.Now, ": ", msg));
        }

        private static void WriteConsoleMessage(Exception ex)
        {
            Console.WriteLine(string.Concat(DateTime.Now, ": ", ex.Message));
        }

        static async Task Main(string[] args)
        {
            IConfigurationBuilder builder = new ConfigurationBuilder().AddJsonFile("appsettings.json", false, true);
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


            WriteConsoleMessage("Beginning Service", databaseSettings.MT_EMLConnectionString);

            if (loggingSettings.Debug)
            {
                WriteConsoleMessage($"TransientFaultHandlingOptions.Enabled={transientFaultSettings.Enabled}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"TransientFaultHandlingOptions.AutoRetryDelay={transientFaultSettings.AutoRetryDelay}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"DatabaseSettings.ConnectionString={databaseSettings.PVConnectionString}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"ActiveCampaignAPISettings.ApiKey={acAPISettings.ApiKey}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"ActiveCampaignAPISettings.PostURL={acAPISettings.URL}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"LogLevel={loggingSettings.LogLevel}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"Debug={loggingSettings.Debug}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"UseDebugEmail={loggingSettings.UseDebugEmail}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"DebugEmail={loggingSettings.DebugEmail}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"DryRun={loggingSettings.DryRun}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"OverrideTranDate={loggingSettings.OverrideTranDate}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"OverrideTranDateValue={loggingSettings.OverrideTranDateValue}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"ConfirmationTrimInterval={loggingSettings.ConfirmationTrimInterval}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"ProcessOracleTableOnly={loggingSettings.ProcessOrdersTableOnly}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"ProcessTicketsTableOnly={loggingSettings.ProcessTicketsTableOnly}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"TransactionIdOverride={loggingSettings.TransactionIdOverride}", databaseSettings.MT_EMLConnectionString);
                WriteConsoleMessage($"TransactionList={loggingSettings.TransactionList}", databaseSettings.MT_EMLConnectionString);
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

                // first query to get orders header info
                // IMPORTANT: Use order table for order_id and transactionid, otherwise transactions get broken out differently
                // only get orders that have tickets, max() is used on order columns because there should only be one order per grouping
                string queryTicketOrders = @"select o.order_id, o.transactionid, o.order_email, o.Attending_Patron_Account_id, o.coupon, 
                        max(o.Tickets_Value) as TicketsValue, max(o.UpSells_Value) as UpSellsValue, sum(t.SALES_TAX) as SalesTax, max(o.Order_Date) as OrderDate, max(o.Insert_Dtm) as InsertDtm, max(o.last_updated_dtm) as LastUpdatedDtm
                        from tbl_MKTECommTicketDetail t
                        inner join tbl_MKTECommOrderInfo o on t.order_id = o.order_id
                        where o.order_update_status = 'P'
                        group by o.order_id, o.transactionid, o.order_email, o.Attending_Patron_Account_id, o.coupon
                        order by o.order_id, o.transactionid, o.order_email, o.Attending_Patron_Account_id, o.coupon";
//having t.order_id in (42761910,42761811,)";

                // second query runs for each order to get product list
                string queryTicketDetails = @"select t.buyer_type_code, t.buyer_type_desc, t.buyer_type_group_id, t.ticket_price,
                        SUM(t.ticket_count) as TicketCount, MAX(t.ticket_price) as TicketPrice
                        from tbl_MKTECommTicketDetail t
                        where Order_Id = <Order_Id>
                        group by t.buyer_type_code, t.buyer_type_desc, t.buyer_type_group_id, t.ticket_price";

                string queryOrdersSelectString = @"SELECT MAX(a.TRANSACTION_ID) as MAX_TRANSACTION_ID,  MAX(a.LAST_UPDATED_DATE) as LAST_UPDATED_DATE, b.ORDER_ID, d.EMAIL, 
                                                            d.ATTENDING_PATRON_ACCOUNT_ID,  
                                                            dm.DELIVERY_METHOD_CODE, o.SUPPLIER_ID, 'N' as CELEBRATING, 
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
                                                            NVL(MAX(COUPON_CODE),'') as COUPON,
                                                            SUM(CASE WHEN p.PRICE_SCALE_CODE = 'NONADM' AND bt.DISPLAY_INDICATOR <> 'T' THEN PRICE ELSE 0.00 END) AS UPSELLS_VALUE, 
                                                            '' as UPSELLS_DATA,
                                                            o.ORDER_DATE,
                                                            e.EVENT_DATE,
                                                            CASE WHEN ac.ACCOUNT_TYPE_CODE = 'IND' THEN 'IND' ELSE act.PATRON_ACCOUNT_TYPE_CODE END AS GUEST_TYPE,
                                                            ec.EVENT_CATEGORY_CODE as EVENT_TYPE,
                                                            ag.DESCRIPTION as Agency
                                                FROM	
                                                (SELECT ORDER_ID, ORDER_LINE_ITEM_ID, TRANSACTION_ID, EVENT_ID FROM Order_Line_Item WHERE ORDER_ID IN 
                                                    (SELECT ORDER_ID FROM Order_Line_Item WHERE Transaction_Id IN 
                                                        (SELECT TRANSACTION_ID FROM TRANSACTION  WHERE LAST_UPDATED_DATE + (5/24/60) > to_timestamp('<Last_Updated_Dtm>', 'MM/DD/YYYY HH:MI:SS AM')) 
                                                    )
                                                ) b
                                                INNER JOIN  TRANSACTION a
                                                ON b.TRANSACTION_ID = a.TRANSACTION_ID
                                                INNER JOIN  EVENT e 
                                                ON          b.EVENT_ID = e.EVENT_ID AND 
                                                            b.EVENT_ID NOT IN (SELECT EVENT_ID FROM Event WHERE EVENT_CODE like '%GIFTCERT%' OR EVENT_CODE like '%BULK%') 
                                                INNER JOIN  EVENT_CATEGORY ec
                                                ON          e.EVENT_CATEGORY_ID = ec.EVENT_CATEGORY_ID
                                                INNER JOIN  PATRON_ORDER o 
                                                ON          b.ORDER_ID = o.ORDER_ID 
                                                INNER JOIN  AGENCY ag
                                                ON          o.CREATED_BY_AGENCY_ID = ag.AGENCY_ID
                                                INNER JOIN  PATRON_ACCOUNT act 
                                                ON          o.FINANCIAL_PATRON_ACCOUNT_ID = act.PATRON_ACCOUNT_ID 
                                                INNER JOIN	PATRON_ACCOUNT_TYPE ac
                                                ON				act.PATRON_ACCOUNT_TYPE_CODE = ac.PATRON_ACCOUNT_TYPE_CODE  
                                                INNER JOIN  Ticket t   
                                                ON          b.ORDER_LINE_ITEM_ID = t.ORDER_LINE_ITEM_ID AND   
                                                            t.REMOVE_ORDER_LINE_ITEM_ID IS NULL 
                                                LEFT JOIN   COUPON cp
                                                ON          t.COUPON_ID = cp.COUPON_ID
                                                INNER JOIN	PRICE_SCALE p
                                                ON			p.PRICE_SCALE_ID = t.PRICE_SCALE_ID
                                                INNER JOIN	BUYER_TYPE bt
                                                ON			t.BUYER_TYPE_ID = bt.BUYER_TYPE_ID
                                                INNER JOIN  Ticket_Delivery td   
                                                ON			td.TICKET_ID = t.TICKET_ID   
                                                AND			td.REMOVE_TRANSACTION_ID IS NULL    
                                                INNER JOIN  Delivery d   
                                                ON			d.DELIVERY_ID = td.DELIVERY_ID   
                                                AND			d.DELIVERY_STATUS_CODE <> 'C'   
                                                INNER JOIN	(SELECT * FROM delivery_method WHERE DELIVERY_TYPE_CODE = 'ETH') dm   
                                                ON			dm.DELIVERY_METHOD_ID = d.DELIVERY_METHOD_ID   
                                                GROUP BY    b.ORDER_ID, o.SUPPLIER_ID, d.EMAIL, d.ATTENDING_PATRON_ACCOUNT_ID, dm.DELIVERY_METHOD_CODE, -- p.PRICE_SCALE_CODE, 
                                                            CASE WHEN ac.ACCOUNT_TYPE_CODE = 'IND' THEN 'IND' ELSE act.PATRON_ACCOUNT_TYPE_CODE END, 
                                                            o.ORDER_DATE,
                                                            e.EVENT_DATE,
                                                            ec.EVENT_CATEGORY_CODE,
                                                            ag.DESCRIPTION";

               

                string queryOrdersSelectWithOverrideString = @"SELECT MAX(a.TRANSACTION_ID) as MAX_TRANSACTION_ID, MAX(a.LAST_UPDATED_DATE) as LAST_UPDATED_DATE, b.ORDER_ID, d.EMAIL,  
                                                            d.ATTENDING_PATRON_ACCOUNT_ID, 
                                                            dm.DELIVERY_METHOD_CODE, o.SUPPLIER_ID, 'N' as CELEBRATING, MAX(CASE WHEN p.PRICE_SCALE_CODE <> 'NONADM' THEN CONCAT(p.PUBLIC_DESCRIPTION,CASE WHEN p.PUBLIC_DESCRIPTION <> 'General Admission' THEN ' Upgrade' ELSE '' END)  ELSE NULL END) AS PACKAGE_TYPE,
                                                            SUM(CASE WHEN (bt.DESCRIPTION NOT LIKE ('%Child%') AND p.PRICE_SCALE_CODE <> 'NONADM') THEN 1 ELSE 0 END) AS ADULT_TICKETS,
                                                            SUM(CASE WHEN (bt.DESCRIPTION LIKE ('%Child%') AND p.PRICE_SCALE_CODE <> 'NONADM')THEN 1 ELSE 0 END) AS CHILD_TICKETS,
                                                            SUM(CASE WHEN (bt.DISPLAY_INDICATOR = 'A' AND p.PRICE_SCALE_CODE IN ('GA','ROYAL','CELEB','KINGS','QUEENS')) THEN PRICE ELSE 0.00 END) + 
                                                                SUM(CASE WHEN (bt.DISPLAY_INDICATOR = 'C' AND p.PRICE_SCALE_CODE IN ('GA','ROYAL','CELEB','KINGS','QUEENS')) THEN PRICE ELSE 0.00 END) AS TICKET_VALUE,
                                                            '' as COUPON,
                                                            0.00 as PACKAGE_VALUE,
                                                            SUM(CASE WHEN p.PRICE_SCALE_CODE = 'NONADM' AND bt.DISPLAY_INDICATOR <> 'T' THEN PRICE ELSE 0.00 END) AS UPSELLS_VALUE, 
                                                            '' as UPSELLS_DATA,
                                                            o.ORDER_DATE,
                                                            e.EVENT_DATE,
                                                            CASE WHEN ac.ACCOUNT_TYPE_CODE = 'IND' THEN 'IND' ELSE act.PATRON_ACCOUNT_TYPE_CODE END AS GUEST_TYPE,
                                                            ec.EVENT_CATEGORY_CODE as EVENT_TYPE,
                                                            ag.DESCRIPTION as Agency
                                                FROM		(SELECT TRANSACTION_ID, LAST_UPDATED_DATE FROM TRANSACTION WHERE TRANSACTION_ID IN (<TRANSACTIONLIST>)) a    
                                                INNER JOIN  Order_Line_Item b   
                                                ON          a.transaction_id = b.TRANSACTION_ID  
                                                INNER JOIN  EVENT e 
                                                ON          b.EVENT_ID = e.EVENT_ID AND 
                                                            b.EVENT_ID NOT IN (SELECT EVENT_ID FROM Event WHERE EVENT_CODE like '%GIFTCERT%' OR EVENT_CODE like '%BULK%') 
                                                INNER JOIN  EVENT_CATEGORY ec
                                                ON          e.EVENT_CATEGORY_ID = ec.EVENT_CATEGORY_ID
                                                INNER JOIN  PATRON_ORDER o 
                                                ON          b.ORDER_ID = o.ORDER_ID 
                                                INNER JOIN  AGENCY ag
                                                ON          o.CREATED_BY_AGENCY_ID = ag.AGENCY_ID
                                                INNER JOIN  PATRON_ACCOUNT act 
                                                ON          o.FINANCIAL_PATRON_ACCOUNT_ID = act.PATRON_ACCOUNT_ID 
                                                INNER JOIN	PATRON_ACCOUNT_TYPE ac
                                                ON				act.PATRON_ACCOUNT_TYPE_CODE = ac.PATRON_ACCOUNT_TYPE_CODE  
                                                INNER JOIN  Ticket t   
                                                ON          b.ORDER_LINE_ITEM_ID = t.ORDER_LINE_ITEM_ID AND   
                                                            t.REMOVE_ORDER_LINE_ITEM_ID IS NULL 
                                                INNER JOIN	PRICE_SCALE p
                                                ON			p.PRICE_SCALE_ID = t.PRICE_SCALE_ID
                                                INNER JOIN	BUYER_TYPE bt
                                                ON			t.BUYER_TYPE_ID = bt.BUYER_TYPE_ID
                                                INNER JOIN  Ticket_Delivery td   
                                                ON			td.TICKET_ID = t.TICKET_ID   
                                                AND			td.REMOVE_TRANSACTION_ID IS NULL    
                                                INNER JOIN  Delivery d   
                                                ON			d.DELIVERY_ID = td.DELIVERY_ID   
                                                AND			d.DELIVERY_STATUS_CODE <> 'C'   
                                                INNER JOIN	(SELECT * FROM delivery_method WHERE DELIVERY_TYPE_CODE = 'ETH') dm   
                                                ON			dm.DELIVERY_METHOD_ID = d.DELIVERY_METHOD_ID   
                                                GROUP BY    b.ORDER_ID, o.SUPPLIER_ID, d.EMAIL, d.ATTENDING_PATRON_ACCOUNT_ID, dm.DELIVERY_METHOD_CODE, -- p.PRICE_SCALE_CODE, 
                                                            CASE WHEN ac.ACCOUNT_TYPE_CODE = 'IND' THEN 'IND' ELSE act.PATRON_ACCOUNT_TYPE_CODE END, 
                                                            o.ORDER_DATE,
                                                            e.EVENT_DATE,
                                                            ec.EVENT_CATEGORY_CODE,
                                                            ag.DESCRIPTION";

                string queryOrdersVerifyString = "SELECT COUNT(*) as PreviousOrderCount FROM tbl_MKTECommOrderInfo WHERE TransactionId = <VALUE> AND Last_Updated_Dtm BETWEEN DATE_SUB(STR_TO_DATE('<Last_Updated_Dtm>', '%m/%d/%Y %h:%i:%s %p'), INTERVAL 1 SECOND) AND " +
                                                "DATE_ADD(STR_TO_DATE('<Last_Updated_Dtm>', '%m/%d/%Y %h:%i:%s %p'), INTERVAL 1 SECOND)";

                string queryAccountsToUpdateString = @"SELECT TransactionId, Order_Id, AC_ID, LocationDesc as Castle, Celebrating, Adult_Tickets + Child_Tickets as Number_Of_Tickets, Adult_Tickets, Child_Tickets, Tickets_Value, Coupon, Package_Type, Package_Value, 
                                                    Package_Type, Upsells_Value, Upsells_Data, Order_Date, Event_Date,  Guest_Type, Event_Type, Agency
                                                    FROM  mt_eml.tbl_MKTECommOrderInfo a
                                                    INNER JOIN tbl_EMLLocations b ON a.SUPPLIER_ID = b.SupplierId
                                                    WHERE Order_Update_Status = 'P' AND AC_Exists = 'Y' AND TRIM(IFNULL(AC_Active_List,'')) <> ''";

                string queryOrderIdsListString = @"SELECT DISTINCT Order_Id 
                                                    FROM  mt_eml.tbl_MKTECommOrderInfo
                                                    WHERE Order_Update_Status = 'P' AND AC_Exists = 'Y' AND TRIM(IFNULL(AC_Active_List,'')) <> ''";

                string queryOrdersInsertString = "INSERT INTO tbl_MKTECommOrderInfo(TransactionId,Last_Updated_Dtm,Order_Id,Order_Email,Delivery_Type,SUPPLIER_ID,Celebrating,Adult_Tickets,Child_Tickets,Tickets_Value,Coupon,Package_Type,Package_Value,Upsells_Value,Upsells_Data,Order_Date,Event_Date,Guest_Type,Event_Type,Agency,Attending_Patron_Account_Id) SELECT <VALUES>";

                string queryTicketsInsertString = "INSERT INTO tbl_MKTECommTicketDetail(TRANSACTION_ID, EVENT_ID, EVENT_CODE, EVENT_DATE_TIME, SUPPLIER_ID, PATRON_ACCOUNT_ID, ORDER_ID, PRICE_SCALE, BUYER_TYPE_CODE, BUYER_TYPE_DESC, BUYER_TYPE_GROUP_ID, REPORT_BUYER_TYPE_GROUP_ID, DISPLAY_INDICATOR, TAX_EXEMPT, TICKET_ID, PAYMENT_STATUS_CODE, TICKET_PRICE, CONV_FEE, SALES_TAX, GRATUITY, ALLOCATION, INC_SALES_TAX, TICKET_COUNT) SELECT <VALUES>";


                string queryUpdateOrdersToSkip = "UPDATE mt_eml.tbl_MKTECommOrderInfo SET Order_Update_Status = 'X' WHERE Order_Update_Status = 'P'; " +
                                                 "UPDATE mt_eml.tbl_MKTECommOrderInfo SET Order_Update_Status = 'S' WHERE Order_Update_Status = 'N' AND SUPPLIER_ID NOT IN " +
                                                    "(SELECT SupplierId FROM tbl_EMLLocations WHERE EmailActive = 'Y'); " +
                                                    "UPDATE mt_eml.tbl_MKTECommOrderInfo SET Order_Update_Status = 'P' WHERE Order_Update_Status = 'N' AND SUPPLIER_ID IN " +
                                                    "(SELECT SupplierId FROM tbl_EMLLocations WHERE EmailActive = 'Y')";

                string queryOrdersToProcessString = @"SELECT TransactionId,Last_Updated_Dtm,Order_Id,Order_Email,Delivery_Type,Celebrating,Adult_Tickets,Child_Tickets,Tickets_Value,Coupon,Package_Type,Package_Value,Upsells_Value,Upsells_Data,Order_Date,Event_Date,Guest_Type,Event_Type,Agency, LocationDesc as Castle
                                                    FROM (SELECT * FROM tbl_MKTECommOrderInfo WHERE Order_Update_Status = 'P') a 
                                                    INNER JOIN tbl_EMLLocations b ON a.SUPPLIER_ID = b.SupplierId;";

                string queryOrderUpdateString = "UPDATE tbl_MKTECommOrderInfo SET Order_Update_Status = '<Order_Update_Status>', Order_Updated_Dtm = CURRENT_TIMESTAMP, Response_Object = '<Response_Object>' WHERE TransactionId = <MaxTransactionId>";

                string queryTicketUpdateString = "UPDATE tbl_MKTECommTicketDetail SET Ticket_Update_Status = '<Ticket_Update_Status>', Ticket_Updated_Dtm = CURRENT_TIMESTAMP, Response_Object = '<Response_Object>' WHERE Ticket_ID = <Ticket_Id>";


                string queryOrderUpdatePostTypeString = "UPDATE tbl_MKTECommOrderInfo SET Order_Post_Type = '<Order_Post_Type>', Order_Updated_Dtm = CURRENT_TIMESTAMP, Response_Object = '<Response_Object>' WHERE TransactionId = <MaxTransactionId>";

                string queryOrderACDataString = "UPDATE tbl_MKTECommOrderInfo SET AC_Exists ='<AC_Exists>', AC_ID = <AC_ID>, AC_Active_List = '<AC_Active_List>' WHERE TransactionId = <MaxTransactionId>";

                string queryRecordsMaintenanceString = "DELETE t FROM tbl_MKTECommTicketDetail t " +
                                                        " INNER JOIN tbl_MKTECommOrderInfo o ON t.ORDER_ID = o.ORDER_ID " +
                                                        " WHERE o.Last_Updated_Dtm < NOW()- interval <ConfirmationTrimInterval> day; " +

                                                        "DELETE FROM tbl_MKTECommOrderInfo WHERE Last_Updated_Dtm < NOW()- interval <ConfirmationTrimInterval> day; " +

                                                        "DELETE FROM tbl_MKTECommOrderInfoLog WHERE LogDate < NOW()- interval <ConfirmationTrimInterval> day; ";


                using (MySqlConnection emlConnection = new MySqlConnection(databaseSettings.MT_EMLConnectionString))
                {
                    try
                    {
                        if (loggingSettings.Debug) WriteConsoleMessage("Opening connection to EML", databaseSettings.MT_EMLConnectionString);
                        emlConnection.Open();

                        MySqlCommand cmdRecordsMaintenance = new MySqlCommand(queryRecordsMaintenanceString.Replace("<ConfirmationTrimInterval>", loggingSettings.ConfirmationTrimInterval), emlConnection);
                        cmdRecordsMaintenance.ExecuteNonQuery();

                        string maxLast_Updated_DtmString = string.Empty;

                        if (!loggingSettings.OverrideTranDate)
                        {
                            MySqlDataAdapter daMaxTransactionId = new MySqlDataAdapter("SELECT MAX(Last_Updated_Dtm) as MaxLast_Updated_Dtm FROM tbl_MKTECommOrderInfo", emlConnection);
                            DataSet dsMaxTransactionId = new DataSet();
                            daMaxTransactionId.Fill(dsMaxTransactionId);


                            foreach (DataRow r in dsMaxTransactionId.Tables[0].Rows)
                            {
                                maxLast_Updated_DtmString = r["MaxLast_Updated_Dtm"].ToString();
                            }
                        }
                        else
                        {
                            maxLast_Updated_DtmString = loggingSettings.OverrideTranDateValue.ToString("MM/dd/yyyy hh:mm:ss tt");
                        }

                        if (!loggingSettings.ProcessOrdersTableOnly)
                        {
                            using (OracleConnection pvConnection = new OracleConnection(databaseSettings.PVConnectionString))
                            {


                                try
                                {
                                    int rowcount = 0;
                                    int totalRowcount = 0;
                                    string ordersString = string.Empty;

                                    if (loggingSettings.Debug) WriteConsoleMessage("Executing orders query in PV for ActiveCampaign updates", databaseSettings.MT_EMLConnectionString);


                                    OracleDataAdapter daOrders = new OracleDataAdapter(
                                        loggingSettings.TransactionIdOverride ? queryOrdersSelectWithOverrideString.Replace("<TRANSACTIONLIST>", loggingSettings.TransactionList) :
                                        queryOrdersSelectString.Replace("<Last_Updated_Dtm>", maxLast_Updated_DtmString),
                                            pvConnection);

                                    DataSet dsOrders = new DataSet();
                                    daOrders.SelectCommand.CommandTimeout = 300;
                                    daOrders.Fill(dsOrders);

                                    MySqlCommand cmdInsertOrder = new MySqlCommand();
                                    cmdInsertOrder.Connection = emlConnection;
                                    string orderValuesString = string.Empty;

                                    if (loggingSettings.Debug) WriteConsoleMessage("Inserting orders into tbl_MKTECommOrderInfo", databaseSettings.MT_EMLConnectionString);
                                    foreach (DataRow r in dsOrders.Tables[0].Rows)
                                    {
                                        MySqlDataAdapter daVerifyOrder = new MySqlDataAdapter(queryOrdersVerifyString.Replace("<VALUE>", r["MAX_TRANSACTION_ID"].ToString()).Replace("<Last_Updated_Dtm>", r["LAST_UPDATED_DATE"].ToString()), emlConnection);
                                        DataSet dsVerifyOrder = new DataSet();
                                        daVerifyOrder.Fill(dsVerifyOrder);
                                        DataRow r2 = dsVerifyOrder.Tables[0].Rows[0];
                                        if (0 == int.Parse(r2["PreviousOrderCount"].ToString()))
                                        {
                                            if (loggingSettings.Debug) WriteConsoleMessage("Inserting TransactionId " + r["MAX_TRANSACTION_ID"].ToString(), databaseSettings.MT_EMLConnectionString);

                                            orderValuesString =
                                                r["MAX_TRANSACTION_ID"] + ", " +
                                                "STR_TO_DATE('" + (r["LAST_UPDATED_DATE"]) + "', '%m/%d/%Y %h:%i:%s %p'), " +
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
                                                "STR_TO_DATE('" + (r["ORDER_DATE"]) + "', '%m/%d/%Y %h:%i:%s %p'), " +
                                                "STR_TO_DATE('" + (r["EVENT_DATE"]) + "', '%m/%d/%Y %h:%i:%s %p'), " +
                                                "'" + r["GUEST_TYPE"] + "', " +
                                                "'" + r["EVENT_TYPE"] + "', " +
                                                "'" + r["Agency"] + "'," + 
                                                r["ATTENDING_PATRON_ACCOUNT_ID"];

                                            cmdInsertOrder.CommandText = queryOrdersInsertString.Replace("<VALUES>", orderValuesString);
                                            cmdInsertOrder.ExecuteNonQuery();
                                        }
                                        else
                                        {
                                            if (loggingSettings.Debug) WriteConsoleMessage($"Skipping TransactionId {r["MAX_TRANSACTION_ID"].ToString()} because it was already processed", databaseSettings.MT_EMLConnectionString);
                                        }
                                    }
                                    if (loggingSettings.Debug) WriteConsoleMessage("Finished inserting orders into tbl_MKTECommOrderInfo", databaseSettings.MT_EMLConnectionString);

                                    if (loggingSettings.Debug) WriteConsoleMessage("Update orders to skip if location not active", databaseSettings.MT_EMLConnectionString);
                                    MySqlCommand cmdUpdateOrdersToSkip = new MySqlCommand(queryUpdateOrdersToSkip, emlConnection);
                                    cmdUpdateOrdersToSkip.ExecuteNonQuery();
                                }
                                catch (Exception ex)
                                {
                                    WriteConsoleMessage(ex.Message, databaseSettings.MT_EMLConnectionString);
                                }
                            }

                            try
                            {
                                if (loggingSettings.Debug) WriteConsoleMessage("Getting orders to process", databaseSettings.MT_EMLConnectionString);

                                MySqlDataAdapter daOrdersToProcess = new MySqlDataAdapter(queryOrdersToProcessString, emlConnection);
                                DataSet dsOrdersToProcess = new DataSet();
                                daOrdersToProcess.Fill(dsOrdersToProcess);

                                foreach (DataRow r in dsOrdersToProcess.Tables[0].Rows)
                                {

                                    string currentTransactionId = r["TransactionId"].ToString();
                                    bool isActive = false;

                                    try
                                    {
                                        if (loggingSettings.Debug) WriteConsoleMessage($"Checking existence and status of {r["Order_Email"].ToString()} in ActiveCampaign", databaseSettings.MT_EMLConnectionString);

                                        var apiClient = new APIClient(acAPISettings.ApiKey);
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
                                                        if (loggingSettings.Debug) WriteConsoleMessage($"Found {r["Order_Email"].ToString()} in ActiveCampaign with Id = {ac_id} active on at least one list", databaseSettings.MT_EMLConnectionString);
                                                        MySqlCommand daACData = new MySqlCommand(queryOrderACDataString.Replace("<AC_Exists>", "Y").Replace("<AC_ID>", ac_id).Replace("<AC_Active_List>", listsWithStatusOne).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                        daACData.ExecuteNonQuery();
                                                        isActive = true;
                                                    }
                                                    else
                                                    {
                                                        ac_id = contactWithLists.Contacts.FirstOrDefault()?.Id;
                                                        if (loggingSettings.Debug) WriteConsoleMessage($"Found {r["Order_Email"].ToString()} in ActiveCampaign with Id = {ac_id} but no active lists", databaseSettings.MT_EMLConnectionString);
                                                        MySqlCommand daACData = new MySqlCommand(queryOrderACDataString.Replace("<AC_Exists>", "Y").Replace("<AC_ID>", ac_id).Replace("<Order_Update_Status>", "S").Replace("<AC_Active_List>", "").Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                        daACData.ExecuteNonQuery();
                                                        isActive = false;
                                                    }
                                                }
                                                else
                                                {
                                                    string jsonResponse = JsonSerializer.Serialize(contactWithLists);

                                                    MySqlCommand daACData = new MySqlCommand(queryOrderACDataString.Replace("<AC_Exists>", "N").Replace("<AC_ID>", "0").Replace("<AC_Active_List>", string.Empty).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                    daACData.ExecuteNonQuery();

                                                    MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "S").Replace("<Response_Object>", jsonResponse).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                    daOrderUpdateData.ExecuteNonQuery();

                                                    isActive = false;
                                                }
                                            }
                                            else
                                            {
                                                MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", response.ErrorMessage).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                daOrderUpdateData.ExecuteNonQuery();
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            try
                                            {
                                                if (int.TryParse(currentTransactionId, out int id))
                                                {
                                                    MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", ex.Message).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                    daOrderUpdateData.ExecuteNonQuery();
                                                }
                                            }
                                            catch (Exception ex2) { WriteConsoleMessage($"We ignored an error within a catch block.  {ex2.Message}", databaseSettings.MT_EMLConnectionString); }

                                            if (loggingSettings.Debug) WriteConsoleMessage($"An error occured while processing transaction ID {currentTransactionId}.  {ex.Message}", databaseSettings.MT_EMLConnectionString);
                                            WriteConsoleMessage(ex.Message, databaseSettings.MT_EMLConnectionString);
                                            isActive = false;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        if (loggingSettings.Debug) WriteConsoleMessage("An error occured while constructing outgoing request for AC_ID.  See next record if an exception message is available.", databaseSettings.MT_EMLConnectionString);
                                        WriteConsoleMessage(ex.Message, databaseSettings.MT_EMLConnectionString);
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                WriteConsoleMessage(ex.Message, databaseSettings.MT_EMLConnectionString);
                            }

                        }
                        else
                        {
                            if (loggingSettings.Debug) WriteConsoleMessage("Skipped checking PV for orders due to runtime settings.  Check appSettings.json if this is unintended.", databaseSettings.MT_EMLConnectionString);
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


                        // TODO: add/update similar statuses for ticketDetail table

                        if (!loggingSettings.ProcessTicketsTableOnly)
                        {
                            // get order id list
                            MySqlDataAdapter daOrderIdList = new MySqlDataAdapter(queryOrderIdsListString, emlConnection);
                            DataSet dsOrderIdList = new DataSet();
                            daOrderIdList.Fill(dsOrderIdList);

                            // turn dataset above into comma-separated list of order id's with single quotes
                            string orderIdListString = string.Join(",",
                                dsOrderIdList.Tables[0].AsEnumerable()
                                .Select(row => $"'{row[0].ToString()}'")
                            );

                            queryTicketsSelectString = queryTicketsSelectString.Replace("<Order_Id_List>", orderIdListString);

                            using (OracleConnection pvConnection = new OracleConnection(databaseSettings.PVConnectionString))
                            {


                                try
                                {

                                    if (loggingSettings.Debug) WriteConsoleMessage("Executing tickets query in PV for ActiveCampaign updates", databaseSettings.MT_EMLConnectionString);


                                    OracleDataAdapter daTickets = new OracleDataAdapter(
                                        //loggingSettings.TransactionIdOverride ? queryOrdersSelectWithOverrideString.Replace("<TRANSACTIONLIST>", loggingSettings.TransactionList) :
                                        queryTicketsSelectString.Replace("<Order_Id_List>", orderIdListString),
                                            pvConnection);

                                    DataSet dsTickets = new DataSet();
                                    daTickets.SelectCommand.CommandTimeout = 600;
                                    daTickets.Fill(dsTickets);

                                    MySqlCommand cmdInsertTicket = new MySqlCommand();
                                    cmdInsertTicket.Connection = emlConnection;
                                    string ticketValuesString = string.Empty;

                                    if (loggingSettings.Debug) WriteConsoleMessage("Inserting tickets into tbl_MKTECommTicketDetail", databaseSettings.MT_EMLConnectionString);
                                    foreach (DataRow r in dsTickets.Tables[0].Rows)
                                    {

                                        if (loggingSettings.Debug) WriteConsoleMessage("Inserting TransactionId to TicketDetails " + r["TRANSACTION_ID"].ToString(), databaseSettings.MT_EMLConnectionString);

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
                                        //if (loggingSettings.Debug) WriteConsoleMessage(cmdInsertTicket.CommandText);

                                        cmdInsertTicket.ExecuteNonQuery();

                                    }
                                    if (loggingSettings.Debug) WriteConsoleMessage("Finished inserting tickets into tbl_MKTECommTicketDetail", databaseSettings.MT_EMLConnectionString);

                                }
                                catch (Exception ex)
                                {
                                    WriteConsoleMessage(ex.Message, databaseSettings.MT_EMLConnectionString);
                                }
                            }
                        }
                        else
                        {
                            if (loggingSettings.Debug) WriteConsoleMessage("Skipped getting Tickets for Orders due to runtime settings.  Check appSettings.json if this is unintended.", databaseSettings.MT_EMLConnectionString);
                        }

                        // build customer and order info for EComm API
                        // Order info has either 0 or 1 coupons - keep this together with header info
                        MySqlDataAdapter daApiOrderList = new MySqlDataAdapter(queryTicketOrders, emlConnection);
                        DataSet dsApiOrderList = new DataSet();
                        daApiOrderList.Fill(dsApiOrderList);

                        foreach (DataRow r in dsApiOrderList.Tables[0].Rows)
                        {
                            try
                            {
                                var apiClient = new APIClient(acAPISettings.ApiKey);
                                string url = acAPISettings.URL;

                                if (loggingSettings.Debug) WriteConsoleMessage($"Checking existence of {r["Order_Email"].ToString()} as an Ecomm Customer", databaseSettings.MT_EMLConnectionString);

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
                                    //TODO: see GetCustomerAsync for TODO details
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
                                        // todo: (TEST this) handle failure as an ORDER status update (not ticket yet)
                                        MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", response.ErrorMessage).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                        daOrderUpdateData.ExecuteNonQuery();
                                    }

                                    // create customer if it doesn't already exist
                                    if (!customerExists)
                                    {
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
                                        var options = new JsonSerializerOptions { WriteIndented = true };
                                        string json = JsonSerializer.Serialize(wrapper, options);
                                        // TODO: remove indent above and log json

                                        ApiResponse createResponse = await apiClient.PostCustomerOrOrdersAsync(url, json, "ecomCustomers"); // path is case sensitive
                                        if (createResponse.IsSuccess)
                                        {
                                            //TODO: log customer id created
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
                                        }
                                        else
                                        {
                                            // todo: handle failure
                                            // Success doesn't mean they exist, have to check array
                                            using JsonDocument doc = JsonDocument.Parse(createResponse.Content);

                                        }

                                    }

                                    WriteConsoleMessage($"Checking existence of order id {orderId} in Ecomm.", databaseSettings.MT_EMLConnectionString);

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
                                                WriteConsoleMessage($"ExternalOrderId {orderId} found with Ecom order id {existingOrderId}, order will be updated.", databaseSettings.MT_EMLConnectionString);

                                                MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdatePostTypeString.Replace("<Order_Post_Type>", "U").Replace("<Response_Object>", checkOrderResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                daOrderUpdateData.ExecuteNonQuery();
                                            }
                                            else
                                            {
                                                WriteConsoleMessage($"Error: Returned ExternalId {returnedExternalId} does not match current externalId {orderId}", databaseSettings.MT_EMLConnectionString);
                                                MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", checkOrderResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                daOrderUpdateData.ExecuteNonQuery();
                                            }

                                        }
                                        else
                                        {
                                            processOrderToEcomm = true;
                                            WriteConsoleMessage($"External Id {orderId} not found, creating new.", databaseSettings.MT_EMLConnectionString);
                                            MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdatePostTypeString.Replace("<Order_Post_Type>", "I").Replace("<Response_Object>", checkOrderResponse.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                            daOrderUpdateData.ExecuteNonQuery();
                                        }
                                    }

                                    if (processOrderToEcomm)
                                    {
                                        // at this point we have a customerid (either from lookup or create)
                                        // create the order products, discount and header info

                                        // Get tickets details for the order
                                        // Each product on the order comes from ticket buyer_type_code
                                        var orderProducts = new List<OrderProduct>();
                                        var orderTicketDetails = queryTicketDetails.Replace("<Order_Id>", orderId);
                                        MySqlDataAdapter daApiTicketList = new MySqlDataAdapter(orderTicketDetails, emlConnection);
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
                                                TotalPrice = (int)(((decimal)r["TicketsValue"] + (decimal)r["UpSellsValue"] + (decimal)r["SalesTax"]) * 100),
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
                                            WriteConsoleMessage($"Creating new order - POST Order JSON: {orderJson}", databaseSettings.MT_EMLConnectionString);

                                            ApiResponse orderResponse = await apiClient.PostCustomerOrOrdersAsync(url, orderJson, "ecomOrders"); // path is case sensitive
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
                                                        WriteConsoleMessage($"Ecom order id {newOrderId} created with ExternalOrderId {orderId}", databaseSettings.MT_EMLConnectionString);

                                                        MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Y").Replace("<Response_Object>", response.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                        daOrderUpdateData.ExecuteNonQuery();
                                                    }
                                                    else
                                                    {
                                                        WriteConsoleMessage($"Could not get id property from {orderResponse.Content}", databaseSettings.MT_EMLConnectionString);
                                                        MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Z").Replace("<Response_Object>", response.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                        daOrderUpdateData.ExecuteNonQuery();
                                                    }

                                                }
                                                else
                                                {
                                                    WriteConsoleMessage($"Could not get ecomOrder property from {orderResponse.Content}", databaseSettings.MT_EMLConnectionString);
                                                    MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Z").Replace("<Response_Object>", response.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                    daOrderUpdateData.ExecuteNonQuery();
                                                }
                                            }
                                            else
                                            {
                                                //using JsonDocument doc = JsonDocument.Parse(orderResponse.Content);
                                                WriteConsoleMessage($"FAILED Order Post for ExternalOrderId: {orderId} Status: {orderResponse.StatusCode} ErrorMessage: {orderResponse.ErrorMessage}", databaseSettings.MT_EMLConnectionString);
                                                MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", response.ErrorMessage).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                daOrderUpdateData.ExecuteNonQuery();

                                            }
                                        }
                                        else
                                        {
                                            WriteConsoleMessage($"Updating existing order {existingOrderId} - PUT Order JSON: {orderJson}", databaseSettings.MT_EMLConnectionString);

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
                                                            WriteConsoleMessage($"Ecom order id {newOrderId} updated for ExternalOrderId {orderId}", databaseSettings.MT_EMLConnectionString);

                                                            MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Y").Replace("<Response_Object>", response.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                            daOrderUpdateData.ExecuteNonQuery();
                                                        }
                                                        else
                                                        {
                                                            WriteConsoleMessage($"Updated order id {newOrderId} does not match order we attempted to update {existingOrderId}", databaseSettings.MT_EMLConnectionString);
                                                            MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Z").Replace("<Response_Object>", response.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                            daOrderUpdateData.ExecuteNonQuery();
                                                        }
                                                    }
                                                    else
                                                    {
                                                        WriteConsoleMessage($"Could not get id property from {updateResponse.Content}", databaseSettings.MT_EMLConnectionString);
                                                        MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Z").Replace("<Response_Object>", response.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                        daOrderUpdateData.ExecuteNonQuery();
                                                    }

                                                }
                                                else
                                                {
                                                    WriteConsoleMessage($"Could not get ecomOrder property from {updateResponse.Content}", databaseSettings.MT_EMLConnectionString);
                                                    MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "Z").Replace("<Response_Object>", response.Content.Replace("'", "''")).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                    daOrderUpdateData.ExecuteNonQuery();
                                                }
                                            }
                                            else
                                            {
                                                //using JsonDocument doc = JsonDocument.Parse(orderResponse.Content);
                                                WriteConsoleMessage($"FAILED to update order {existingOrderId} with ExternalOrderId: {orderId} Status: {updateResponse.StatusCode} ErrorMessage: {updateResponse.ErrorMessage}", databaseSettings.MT_EMLConnectionString);
                                                MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", response.ErrorMessage).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                                daOrderUpdateData.ExecuteNonQuery();

                                            }
                                        }
                                        
                                    }
                                    
                                }
                                catch (Exception ex)
                                {
                                    // todo: reinstate as ticket update
                                    // try
                                    // {
                                    //     if (int.TryParse(currentTransactionId, out int id))
                                    //     {
                                    //         MySqlCommand daOrderUpdateData = new MySqlCommand(queryOrderUpdateString.Replace("<Order_Update_Status>", "E").Replace("<Response_Object>", ex.Message).Replace("<MaxTransactionId>", currentTransactionId), emlConnection);
                                    //         daOrderUpdateData.ExecuteNonQuery();
                                    //     }

                                    // }
                                    // catch (Exception ex2) { WriteConsoleMessage($"We ignored an error within a catch block.  {ex2.Message}", databaseSettings.MT_EMLConnectionString); }

                                    if (loggingSettings.Debug) WriteConsoleMessage($"An error occured while processing Order ID {r["Order_Id"]} Email {r["Order_Email"]}  {ex.Message}", databaseSettings.MT_EMLConnectionString);
                                    WriteConsoleMessage(ex.Message, databaseSettings.MT_EMLConnectionString);

                                }
                            }
                            catch (Exception ex)
                            {
                                if (loggingSettings.Debug) WriteConsoleMessage("An error occured while constructing outgoing request to check for Customer.  See next record if an exception message is available.", databaseSettings.MT_EMLConnectionString);
                                WriteConsoleMessage(ex.Message, databaseSettings.MT_EMLConnectionString);
                            }
                        }
                        



                        if (loggingSettings.Debug) WriteConsoleMessage("Completed calls to PV.  See previous lines for any errors that may have occurred.", databaseSettings.MT_EMLConnectionString);
                            WriteConsoleMessage("Ending Service", databaseSettings.MT_EMLConnectionString);
                        } // end try using (MySqlConnection emlConnection = new MySqlConnection(databaseSettings.MT_EMLConnectionString))
                    catch (Exception ex)
                    {
                        WriteConsoleMessage(ex.Message, databaseSettings.MT_EMLConnectionString);
                    }
                }

            }

        }
    }
}



