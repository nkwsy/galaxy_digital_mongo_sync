# Galaxy Digital API Data Schema Documentation

## Table of Contents
1. [Introduction](#introduction)
2. [Core Resources](#core-resources)
   - [Agencies](#agencies)
   - [Users](#users)
   - [Needs (Opportunities)](#needs-opportunities)
   - [Hours](#hours)
   - [Responses](#responses)
   - [Events](#events)
3. [Aggregated Collections](#aggregated-collections)
   - [User Activity Summary](#user-activity-summary)
   - [Opportunity Activity](#opportunity-activity)
   - [Agency Activity](#agency-activity)
   - [Shift Status](#shift-status)
   - [Monthly Activity](#monthly-activity)
4. [Entity Relationships](#entity-relationships)
5. [Sync Metadata](#sync-metadata)
6. [Indexing Strategy](#indexing-strategy)
7. [Data Synchronization](#data-synchronization)
8. [Aggregation Reports](#aggregation-reports)

## Introduction

This document outlines the data schema and relationships for the Galaxy Digital API integration. It serves as a reference for understanding the structure of data synchronized from the Galaxy Digital volunteer management platform to a MongoDB database.

## Core Resources

### Agencies

Agencies represent organizations that offer volunteer opportunities.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Unique identifier |
| agency_name | String | Name of the agency |
| agency_status | String | Status (Active, Inactive, etc.) |
| agency_description | String | Description of the agency |
| agency_address | String | Physical address |
| agency_city | String | City |
| agency_state | String | State/Province |
| agency_zip | String | Postal/ZIP code |
| agency_phone | String | Contact phone number |
| agency_email | String | Contact email |
| agency_website | String | Website URL |
| agency_facebook | String | Facebook page |
| agency_twitter | String | Twitter handle |
| agency_instagram | String | Instagram handle |
| agency_created | DateTime | Creation timestamp |
| agency_updated | DateTime | Last update timestamp |
| _synced_at | DateTime | When the document was last synced |
| _sync_source | String | Source of the sync (always "galaxy_api") |

### Users

Users represent volunteers registered in the system.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Unique identifier |
| user_email | String | Email address (unique) |
| user_fname | String | First name |
| user_lname | String | Last name |
| user_status | String | Status (Active, Inactive, etc.) |
| user_address | String | Physical address |
| user_city | String | City |
| user_state | String | State/Province |
| user_zip | String | Postal/ZIP code |
| user_phone | String | Contact phone number |
| user_mobile | String | Mobile phone number |
| user_birthdate | Date | Birth date |
| user_created | DateTime | Creation timestamp |
| user_updated | DateTime | Last update timestamp |
| user_groups | Array | Groups the user belongs to |
| user_skills | Array | Skills the user has |
| user_interests | Array | Interest areas |
| _synced_at | DateTime | When the document was last synced |
| _sync_source | String | Source of the sync (always "galaxy_api") |

### Needs (Opportunities)

Needs represent volunteer opportunities offered by agencies.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Unique identifier |
| agency_id | Integer | Reference to the agency |
| need_title | String | Title of the opportunity |
| need_description | String | Detailed description |
| need_status | String | Status (Active, Inactive, etc.) |
| need_address | String | Location address |
| need_city | String | City |
| need_state | String | State/Province |
| need_zip | String | Postal/ZIP code |
| need_start_date | DateTime | Start date and time |
| need_end_date | DateTime | End date and time |
| need_duration | Number | Duration in hours |
| need_shifts | Array | Available shifts |
| need_skills | Array | Required skills |
| need_interests | Array | Related interest areas |
| need_min_age | Integer | Minimum age requirement |
| need_created | DateTime | Creation timestamp |
| need_updated | DateTime | Last update timestamp |
| _synced_at | DateTime | When the document was last synced |
| _sync_source | String | Source of the sync (always "galaxy_api") |

### Hours

Hours represent time logged by volunteers for specific opportunities.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Unique identifier |
| user | Object | Embedded user information |
| user.id | Integer | Reference to the user |
| user.user_email | String | User email |
| user.user_fname | String | User first name |
| user.user_lname | String | User last name |
| need | Object | Embedded need information |
| need.id | Integer | Reference to the need/opportunity |
| need.title | String | Need title |
| need.agency_id | Integer | Reference to the agency |
| hour_date_start | DateTime | Start date and time |
| hour_date_end | DateTime | End date and time |
| hour_duration | Number | Duration in hours |
| hour_status | String | Status (Approved, Pending, Rejected) |
| hour_notes | String | Additional notes |
| hour_created | DateTime | Creation timestamp |
| hour_updated | DateTime | Last update timestamp |
| _synced_at | DateTime | When the document was last synced |
| _sync_source | String | Source of the sync (always "galaxy_api") |

### Responses

Responses represent volunteer sign-ups for opportunities.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Unique identifier |
| user | Object | Embedded user information |
| user.id | Integer | Reference to the user |
| user.user_email | String | User email |
| user.user_fname | String | User first name |
| user.user_lname | String | User last name |
| need | Object | Embedded need information |
| need.id | Integer | Reference to the need/opportunity |
| need.title | String | Need title |
| need.agency_id | Integer | Reference to the agency |
| response_status | String | Status (Confirmed, Pending, Declined) |
| response_date | DateTime | Response date |
| response_shift | Object | Selected shift information |
| response_created | DateTime | Creation timestamp |
| response_updated | DateTime | Last update timestamp |
| _synced_at | DateTime | When the document was last synced |
| _sync_source | String | Source of the sync (always "galaxy_api") |

### Events

Events represent special activities or occasions in the system.

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Unique identifier |
| event_title | String | Event title |
| event_description | String | Detailed description |
| event_area_id | Integer | Area/region identifier |
| event_date_start | DateTime | Start date and time |
| event_date_end | DateTime | End date and time |
| event_address | String | Location address |
| event_city | String | City |
| event_state | String | State/Province |
| event_zip | String | Postal/ZIP code |
| event_created | DateTime | Creation timestamp |
| event_updated | DateTime | Last update timestamp |
| _synced_at | DateTime | When the document was last synced |
| _sync_source | String | Source of the sync (always "galaxy_api") |

## Aggregated Collections

These collections are generated through MongoDB aggregation pipelines and provide analytical insights.

### User Activity Summary

| Field | Type | Description |
|-------|------|-------------|
| _id | Integer | User ID |
| user_info | Object | Embedded user information |
| total_hours | Number | Total hours logged |
| shifts_attended | Integer | Number of shifts attended |
| opportunities | Array | List of opportunity IDs |
| first_activity | DateTime | First activity date |
| last_activity | DateTime | Last activity date |
| all_hours | Array | Detailed hours information |
| avg_hours_per_shift | Number | Average hours per shift |
| days_since_last_activity | Number | Days since last activity |
| opportunity_count | Integer | Number of unique opportunities |
| _synced_at | DateTime | When the document was generated |
| _sync_source | String | Source (always "aggregation") |

### Opportunity Activity

| Field | Type | Description |
|-------|------|-------------|
| _id | Integer | Need/Opportunity ID |
| need_info | Object | Embedded need information |
| agency_id | Integer | Reference to the agency |
| total_hours | Number | Total hours logged |
| volunteer_count | Integer | Number of unique volunteers |
| first_activity | DateTime | First activity date |
| last_activity | DateTime | Last activity date |
| shifts_count | Integer | Number of shifts |
| hours_by_month | Array | Hours breakdown by month |
| avg_hours_per_volunteer | Number | Average hours per volunteer |
| avg_shift_duration | Number | Average shift duration |
| days_since_last_activity | Number | Days since last activity |
| _synced_at | DateTime | When the document was generated |
| _sync_source | String | Source (always "aggregation") |

### Agency Activity

| Field | Type | Description |
|-------|------|-------------|
| _id | Integer | Agency ID |
| agency_name | String | Agency name |
| agency_info | Object | Embedded agency information |
| total_hours | Number | Total volunteer hours |
| volunteer_count | Integer | Number of unique volunteers |
| opportunity_count | Integer | Number of unique opportunities |
| first_activity | DateTime | First activity date |
| last_activity | DateTime | Last activity date |
| opportunities | Array | Detailed opportunity information |
| avg_hours_per_volunteer | Number | Average hours per volunteer |
| days_since_last_activity | Number | Days since last activity |
| _synced_at | DateTime | When the document was generated |
| _sync_source | String | Source (always "aggregation") |

### Shift Status

| Field | Type | Description |
|-------|------|-------------|
| id | Integer | Shift/need ID |
| start | DateTime | Shift start date and time |
| end | DateTime | Shift end date and time |
| duration | Number | Expected duration in hours |
| slots | Integer | Total available volunteer slots |
| need_id | Integer | Reference to the need |
| title | String | Shift title |
| users | Array | List of volunteers with their status |
| users[].id | Integer | Volunteer user ID |
| users[].domain_id | Integer | Domain ID |
| users[].user_fname | String | Volunteer first name |
| users[].user_lname | String | Volunteer last name |
| users[].user_email | String | Volunteer email |
| users[].checkin_status | String | Status: pending, active, completed, absent, cancelled |
| users[].hour_id | Integer | Reference to hour record (if exists) |
| users[].hour_status | String | Hour approval status |
| users[].hour_duration | Number | Actual hours logged |
| users[].hour_date_start | DateTime | Actual start time |
| users[].hour_date_end | DateTime | Actual end time |
| users[].hour_date_created | DateTime | When check-in occurred |
| users[].hour_date_updated | DateTime | When check-out occurred |
| slots_filled | Integer | Number of non-cancelled volunteers |
| _synced_at | DateTime | When the document was generated |
| _sync_source | String | Source (always "aggregation") |

### Monthly Activity

| Field | Type | Description |
|-------|------|-------------|
| _id | String | Year-Month (YYYY-MM format) |
| total_hours | Number | Total hours logged |
| volunteer_count | Integer | Number of unique volunteers |
| opportunity_count | Integer | Number of unique opportunities |
| agency_count | Integer | Number of unique agencies |
| shifts_count | Integer | Number of shifts |
| avg_hours_per_volunteer | Number | Average hours per volunteer |
| avg_shift_duration | Number | Average shift duration |
| _synced_at | DateTime | When the document was generated |
| _sync_source | String | Source (always "aggregation") |

## Entity Relationships

The following diagram illustrates the relationships between the core entities:

```
Agency (1) ----< Needs (Many)
                    |
                    |
                    v
User (1) -----< Responses (Many) >----- Needs (1)
   |
   |
   v
Hours (Many) >----- Needs (1)
```

### Relationship Details:

1. **Agency to Needs**: One-to-many. An agency can offer multiple volunteer opportunities (needs).
   - Foreign key: `needs.agency_id` references `agencies.id`

2. **User to Responses**: One-to-many. A user can sign up for multiple opportunities.
   - Embedded reference: `responses.user.id` references `users.id`

3. **Needs to Responses**: One-to-many. An opportunity can have multiple volunteer sign-ups.
   - Embedded reference: `responses.need.id` references `needs.id`

4. **User to Hours**: One-to-many. A user can log hours for multiple activities.
   - Embedded reference: `hours.user.id` references `users.id`

5. **Needs to Hours**: One-to-many. Hours can be logged against a specific opportunity.
   - Embedded reference: `hours.need.id` references `needs.id`

## Sync Metadata

The system maintains sync metadata to track the last successful synchronization for each resource:

| Field | Type | Description |
|-------|------|-------------|
| resource | String | Resource name (e.g., "users", "agencies") |
| last_sync | DateTime | Last successful sync timestamp |
| last_success | DateTime | Last successful completion timestamp |

## Indexing Strategy

The system creates the following indexes to optimize query performance:

| Collection | Field | Direction | Purpose |
|------------|-------|-----------|---------|
| agencies | id | Ascending | Primary lookup |
| agencies | agency_name | Ascending | Name-based search |
| agencies | agency_status | Ascending | Status filtering |
| users | id | Ascending | Primary lookup |
| users | user_email | Ascending | Email-based lookup |
| users | user_fname | Ascending | Name-based search |
| users | user_lname | Ascending | Name-based search |
| users | user_status | Ascending | Status filtering |
| needs | id | Ascending | Primary lookup |
| needs | agency_id | Ascending | Agency-based filtering |
| needs | need_title | Text | Full-text search |
| needs | need_status | Ascending | Status filtering |
| events | id | Ascending | Primary lookup |
| events | event_area_id | Ascending | Area-based filtering |
| events | event_date_start | Ascending | Date-based filtering |
| hours | id | Ascending | Primary lookup |
| hours | user.id | Ascending | User-based filtering |
| hours | need.id | Ascending | Need-based filtering |
| hours | hour_date_start | Ascending | Date-based filtering |
| responses | id | Ascending | Primary lookup |
| responses | user.id | Ascending | User-based filtering |
| responses | need.id | Ascending | Need-based filtering |
| user_activity_summary | _id | Ascending | Primary lookup |
| user_activity_summary | total_hours | Descending | Hours-based sorting |
| user_activity_summary | shifts_attended | Descending | Shift-based sorting |
| user_activity_summary | last_activity | Descending | Recency-based sorting |
| user_activity_summary | days_since_last_activity | Ascending | Inactivity-based sorting |
| opportunity_activity | _id | Ascending | Primary lookup |
| opportunity_activity | total_hours | Descending | Hours-based sorting |
| opportunity_activity | volunteer_count | Descending | Popularity-based sorting |
| opportunity_activity | last_activity | Descending | Recency-based sorting |
| agency_activity | _id | Ascending | Primary lookup |
| agency_activity | total_hours | Descending | Hours-based sorting |
| agency_activity | volunteer_count | Descending | Volunteer-based sorting |
| agency_activity | opportunity_count | Descending | Opportunity-based sorting |
| monthly_activity | _id | Ascending | Chronological sorting |
| monthly_activity | total_hours | Descending | Hours-based sorting |

## Data Synchronization

Data is synchronized from the Galaxy Digital API to MongoDB using the following process:

1. **Incremental Sync**: Resources are synced incrementally using the `since_updated` field when available.
2. **Pagination**: API results are paginated with a default page size of 100 items.
3. **Upsert Strategy**: Documents are updated if they exist, or inserted if they don't.
4. **Metadata Tracking**: Sync metadata is maintained to track the last successful sync for each resource.
5. **Error Handling**: The system includes robust error handling for API rate limits, authentication issues, and connection problems.

## Aggregation Reports

The system generates aggregated reports to provide insights into volunteer activity:

1. **User Activity**: Summarizes each volunteer's activity, including total hours, shifts, and participation patterns.
2. **Opportunity Activity**: Analyzes participation and hours for each volunteer opportunity.
3. **Agency Activity**: Provides metrics on volunteer engagement for each agency.
4. **Time-Based Activity**: Tracks activity patterns over time, including monthly summaries.

These aggregations are refreshed during each sync cycle or can be generated on-demand.
