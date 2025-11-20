# Snowflake Reader Account Provisioner

This project automates the creation and configuration of a Snowflake Managed Reader Account. It sets up a secure data share from a Provider account, provisions the Reader account, and creates a user within that Reader account for access.

## Prerequisites

- Python 3.9+
- A Snowflake account with `ACCOUNTADMIN` or sufficient privileges to create shares and managed accounts.

## Installation

1. **Set up a virtual environment** (optional but recommended):
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Create a `config.yaml` file in the root directory. This file contains credentials for the provider account and configuration details for the reader account to be created.

**Note:** This file is excluded from version control by default for security.

### `config.yaml` Template

```yaml
provider:
  account: "<PROVIDER_ACCOUNT_IDENTIFIER>"   # e.g., ABC12345.us-east-1
  user: "<PROVIDER_USERNAME>"
  password: "<PROVIDER_PASSWORD>"
  role: "ACCOUNTADMIN"

reader:
  account_name: "<READER_MANAGED_ACCOUNT_NAME>"   # name for the Managed Reader Account
  admin_user: "<READER_ADMIN_USERNAME>"           # initial admin user for the reader account
  admin_password: "<READER_ADMIN_TEMP_PASSWORD>"  # initial admin password
  warehouse_name: "<READER_WAREHOUSE_NAME>"
  db_name: "<READER_DATABASE_NAME_FROM_SHARE>"

reader_user:
  name: "<END_USER_LOGIN_NAME>"         # username to create inside the reader account
  email: "<END_USER_EMAIL>"             # where credentials will be sent
  temp_password: "<END_USER_TEMP_PASSWORD>"

share:
  name: "<SHARE_NAME>"                   # name of the provider share

data:
  provider_database: "<PROVIDER_DATABASE>"   # database in provider to share from
  shared_schema: "<SHARED_SCHEMA>"           # schema that contains the secure view
  shared_view_name: "<SECURE_VIEW_NAME>"     # secure view that is shared
  source_table: "<SOURCE_TABLE>"             # source table used to build the secure view

# Optional: SMTP settings for emailing credentials to the reader user
# If omitted or incomplete (no host), the script will skip sending email.
smtp:
  host: "smtp.example.com"             # REQUIRED to send email
  port: 587                             # optional; defaults based on TLS/SSL (see below)
  user: "smtp-user@example.com"        # optional; used for AUTH if provided
  password: "<SMTP_PASSWORD>"          # optional; used for AUTH if provided
  from: "no-reply@example.com"         # optional; defaults to smtp.user or 'no-reply@snowflake'
  use_tls: true                         # optional; default true (ignored if use_ssl true)
  use_ssl: false                        # optional; default false (takes precedence over use_tls)
```

### SMTP behavior and defaults

- Emails are only sent when a new `reader_user` is created during provisioning. Existing users are updated but not re-emailed.
- If `smtp.host` is missing, the email step is skipped and provisioning continues.
- Port selection if `smtp.port` is not provided:
  - `use_ssl: true` → port 465
  - else if `use_tls: true` (default) → port 587
  - else → port 25
- The From address is chosen as `smtp.from` if set, otherwise `smtp.user` if set, otherwise `no-reply@snowflake`.
- If `smtp.user` and `smtp.password` are provided, SMTP AUTH LOGIN is performed; otherwise the script attempts to send without authentication (typical for allowlisted IPs or internal relays).

### Notes on security

- Do not commit real credentials in `config.yaml`. The file is excluded via `.gitignore`, but treat it as sensitive.
- Prefer using an app-specific SMTP credential with least privileges.
- Consider using a secrets manager or environment templating to populate values at runtime.
