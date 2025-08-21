using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace BQ_ACECommerce
{

    public class CustomFieldUpdater
{
    [JsonPropertyName("fieldValue")]
    public FieldValueDetails FieldValue { get; set; }

    public class FieldValueDetails
    {
        [JsonPropertyName("contact")]
        public int Contact { get; set; }

        [JsonPropertyName("field")]
        public int CustomFieldId { get; set; }

        [JsonPropertyName("value")]
        public string Value { get; set; }
    }

}

    public class GuestTypeOptions
    {
        public List<Option> Options { get; set; }
    }

    public class AgencyOptions
    {
        public List<Option> Options { get; set; }
    }


    public class Option
    {
        public string Id { get; set; }
        public string Value { get; set; }
    }

    public class RootPurchaseObject
    {
        [JsonPropertyName("record")]
        public Record Record { get; set; }
    }

    public class Record
    {
        [JsonPropertyName("id")]
        public string Id { get; set; } // Typically empty for new entries
        [JsonPropertyName("externalId")]
        public string ExternalId { get; set; }
        [JsonPropertyName("fields")]
        public List<Field> Fields { get; set; }
        [JsonPropertyName("relationships")]
        public Relationships Relationships { get; set; }
    }

    public class Field
    {
        [JsonPropertyName("id")]
        public string Id { get; set; }
        [JsonPropertyName("value")]
        public dynamic Value { get; set; } 
    }

    public class Relationships
    {
        [JsonPropertyName("primary-contact")]
        public List<int> PrimaryContact { get; set; }
    }

    public class ACContactWithLists
    {
        [JsonPropertyName("contactLists")]
        public List<ContactList> ContactLists { get; set; }

        [JsonPropertyName("scoreValues")]
        public List<object> ScoreValues { get; set; }

        [JsonPropertyName("contacts")]
        public List<Contact> Contacts { get; set; }

        [JsonPropertyName("meta")]
        public Meta Meta { get; set; }
    }

    public class ContactList
    {
        [JsonPropertyName("contact")]
        public string Contact { get; set; }

        [JsonPropertyName("list")]
        public string List { get; set; }

        [JsonPropertyName("form")]
        public object Form { get; set; }

        [JsonPropertyName("seriesid")]
        public string SeriesId { get; set; }

        [JsonPropertyName("sdate")]
        public DateTimeOffset? SDate { get; set; }

        [JsonPropertyName("udate")]
        public DateTimeOffset? UDate { get; set; }

        [JsonPropertyName("status")]
        public string Status { get; set; }

        [JsonPropertyName("responder")]
        public string Responder { get; set; }

        [JsonPropertyName("sync")]
        public string Sync { get; set; }

        [JsonPropertyName("unsubreason")]
        public string UnsubReason { get; set; }

        [JsonPropertyName("campaign")]
        public string Campaign { get; set; }

        [JsonPropertyName("message")]
        public string Message { get; set; }

        [JsonPropertyName("first_name")]
        public string FirstName { get; set; }

        [JsonPropertyName("last_name")]
        public string LastName { get; set; }

        [JsonPropertyName("ip4Sub")]
        public string IP4Sub { get; set; }

        [JsonPropertyName("sourceid")]
        public string SourceId { get; set; }

        [JsonPropertyName("autosyncLog")]
        public object AutoSyncLog { get; set; }

        [JsonPropertyName("ip4_last")]
        public string IP4Last { get; set; }

        [JsonPropertyName("ip4Unsub")]
        public string IP4Unsub { get; set; }

        [JsonPropertyName("created_timestamp")]
        public string CreatedTimestamp { get; set; }

        [JsonPropertyName("updated_timestamp")]
        public string UpdatedTimestamp { get; set; }

        [JsonPropertyName("created_by")]
        public object CreatedBy { get; set; }

        [JsonPropertyName("updated_by")]
        public object UpdatedBy { get; set; }

        [JsonPropertyName("unsubscribeAutomation")]
        public object UnsubscribeAutomation { get; set; }

        [JsonPropertyName("links")]
        public Dictionary<string, string> Links { get; set; }

        [JsonPropertyName("id")]
        public string Id { get; set; }

        [JsonPropertyName("automation")]
        public object Automation { get; set; }
    }

    public class Contact
    {
        [JsonPropertyName("cdate")]
        public DateTimeOffset? CDate { get; set; }

        [JsonPropertyName("email")]
        public string Email { get; set; }

        [JsonPropertyName("phone")]
        public string Phone { get; set; }

        [JsonPropertyName("firstName")]
        public string FirstName { get; set; }

        [JsonPropertyName("lastName")]
        public string LastName { get; set; }

        [JsonPropertyName("orgid")]
        public string OrgId { get; set; }

        [JsonPropertyName("orgname")]
        public string OrgName { get; set; }

        [JsonPropertyName("segmentio_id")]
        public string SegmentioId { get; set; }

        [JsonPropertyName("bounced_hard")]
        public string BouncedHard { get; set; }

        [JsonPropertyName("bounced_soft")]
        public string BouncedSoft { get; set; }

        [JsonPropertyName("bounced_date")]
        public object BouncedDate { get; set; }

        [JsonPropertyName("ip")]
        public string IP { get; set; }

        [JsonPropertyName("ua")]
        public string UA { get; set; }

        [JsonPropertyName("hash")]
        public string Hash { get; set; }

        [JsonPropertyName("socialdata_lastcheck")]
        public object SocialDataLastCheck { get; set; }

        [JsonPropertyName("email_local")]
        public string EmailLocal { get; set; }

        [JsonPropertyName("email_domain")]
        public string EmailDomain { get; set; }

        [JsonPropertyName("sentcnt")]
        public string SentCnt { get; set; }

        [JsonPropertyName("rating_tstamp")]
        public object RatingTimestamp { get; set; }

        [JsonPropertyName("gravatar")]
        public string Gravatar { get; set; }

        [JsonPropertyName("deleted")]
        public string Deleted { get; set; }

        [JsonPropertyName("anonymized")]
        public string Anonymized { get; set; }

        [JsonPropertyName("adate")]
        public DateTimeOffset? ADate { get; set; }

        [JsonPropertyName("udate")]
        public DateTimeOffset? UDate { get; set; }

        [JsonPropertyName("edate")]
        public DateTimeOffset? EDate { get; set; }

        [JsonPropertyName("deleted_at")]
        public object DeletedAt { get; set; }

        [JsonPropertyName("created_utc_timestamp")]
        public string CreatedUtcTimestamp { get; set; }

        [JsonPropertyName("updated_utc_timestamp")]
        public string UpdatedUtcTimestamp { get; set; }

        [JsonPropertyName("created_timestamp")]
        public string CreatedTimestamp { get; set; }

        [JsonPropertyName("updated_timestamp")]
        public string UpdatedTimestamp { get; set; }

        [JsonPropertyName("created_by")]
        public string CreatedBy { get; set; }

        [JsonPropertyName("updated_by")]
        public string UpdatedBy { get; set; }

        [JsonPropertyName("mpp_tracking")]
        public string MppTracking { get; set; }

        [JsonPropertyName("last_click_date")]
        public object LastClickDate { get; set; }

        [JsonPropertyName("last_open_date")]
        public object LastOpenDate { get; set; }

        [JsonPropertyName("last_mpp_open_date")]
        public object LastMppOpenDate { get; set; }

        [JsonPropertyName("scoreValues")]
        public List<object> ScoreValues { get; set; }

        [JsonPropertyName("accountContacts")]
        public List<object> AccountContacts { get; set; }

        [JsonPropertyName("contactLists")]
        public List<string> ContactListIds { get; set; }

        [JsonPropertyName("links")]
        public Dictionary<string, string> Links { get; set; }

        [JsonPropertyName("id")]
        public string Id { get; set; }

        [JsonPropertyName("organization")]
        public object Organization { get; set; }
    }

    public class Meta
    {
        [JsonPropertyName("page_input")]
        public Dictionary<string, object> PageInput { get; set; }

        [JsonPropertyName("total")]
        public string Total { get; set; }

        [JsonPropertyName("sortable")]
        public bool Sortable { get; set; }
    }

}