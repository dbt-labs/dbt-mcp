# List Users

List all users in a dbt platform account.

This tool retrieves all users who have access to the specified dbt platform account, including their basic information and permissions.

## Parameters

- **account_id** (required): The dbt platform account ID

## Returns

List of user objects with information including:

- User ID, first name, last name, and email
- Account activity and login information
- Connected authentication providers (GitHub, GitLab, SSO)
- Permission levels and license types
- Account creation and last login timestamps
- Staff and active status flags

## User Information

Each user object contains:

- **Basic Info**: Name, email, creation date
- **Authentication**: Connected providers and token usage
- **Permissions**: License allocations and group memberships
- **Activity**: Last login and API token usage
- **Status**: Active, staff, and SSO-only flags

## Use Cases

- Audit user access and permissions
- Review account user list for compliance
- Check user authentication methods
- Monitor user activity and last login times
- Manage user permissions and licenses
- Identify inactive or unused accounts

## Permission Types

Users may have different license types:
- **Developer**: Full development access
- **Read Only**: View-only access
- **Analyst**: Business user access
- **IT**: Administrative access

## Example Usage

```python
# List all users in an account
users = list_users(account_id=123)

# Example response includes:
# [
#   {
#     "id": 456,
#     "first_name": "Jane",
#     "last_name": "Doe", 
#     "email": "jane@company.com",
#     "is_active": true,
#     "last_login": "2025-01-15T10:30:00Z"
#   }
# ]
```

Use this information for user management, access auditing, and account administration.
