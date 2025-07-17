# Get Account Details

Get detailed information for a specific dbt Cloud account.

This tool retrieves comprehensive account information from the dbt Cloud Admin API v2.

## Parameters

- **account_id** (required): The dbt Cloud account ID to retrieve details for

## Returns

Account object with details including:

- Account ID, name, and plan type
- Billing information and seat allocations
- Feature flags and account limits
- Account settings and preferences
- Lock status and trial information
- Created/updated timestamps

## Use Cases

- Check account plan and feature availability
- Verify account status and billing information
- Get account limits for resource planning
- Troubleshoot account-specific issues

## Example Response

```json
{
  "id": 123,
  "name": "My Organization",
  "plan": "enterprise",
  "developer_seats": 25,
  "run_slots": 10,
  "locked": false
}
```
