# Get User Details

Get detailed information for a specific user across all accessible accounts.

This tool retrieves comprehensive user information including permissions, authentication details, and account associations.

## Parameters

- **user_id** (required): The user ID to retrieve details for

## Returns

User object with detailed information including:

- **Personal Information**: First name, last name, email address
- **Authentication**: Connected providers (GitHub, GitLab, Azure AD, Enterprise SSO)
- **Account Activity**: Last login, API token usage, email verification status
- **Permissions**: License allocations across accounts and permission groups
- **Provider Details**: Usernames for connected Git providers
- **Account Status**: Active status, staff privileges, SSO-only restrictions

## Authentication Providers

The user may be connected to:

- **GitHub**: For Git integration and authentication
- **GitLab**: Alternative Git provider integration  
- **Azure Active Directory**: Enterprise SSO integration
- **Enterprise SSO**: Custom SAML/OIDC providers
- **Email**: Traditional email/password authentication

## Use Cases

- Get user details for support and troubleshooting
- Audit user permissions across accounts
- Check authentication provider connections
- Verify user access and license allocations
- Review user activity and login patterns
- Validate SSO configuration and connectivity

## Permission Information

The response includes detailed permission data:
- License types and allocations per account
- Group memberships and inherited permissions
- Account-specific access levels
- Authentication provider linkages

## Example Usage

```python
# Get detailed user information
user = get_user(user_id=456)

# Response includes comprehensive user data:
# {
#   "id": 456,
#   "first_name": "Jane",
#   "last_name": "Doe",
#   "email": "jane@company.com", 
#   "github_connected": true,
#   "permissions": [...],
#   "licenses": [...]
# }
```

This tool provides the most complete view of a user's account access and configuration.
