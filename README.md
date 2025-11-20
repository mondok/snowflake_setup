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
  shared_schema: "<SHARED_SCHEMA>"           # schema that will contain the shared secure view(s)

  # Preferred: list one or more source tables to expose as secure views
  # Each entry maps a provider table to a secure view name, with an optional WHERE filter
  objects:
    - shared_view_name: "<SECURE_VIEW_NAME_1>"
      source_table: "<SOURCE_TABLE_1>"
      # view_where is OPTIONAL; include with or without leading WHERE
      view_where: "NPI IS NOT NULL"
    - shared_view_name: "<SECURE_VIEW_NAME_2>"
      source_table: "<SOURCE_TABLE_2>"
      # view_where: "<PREDICATE>"

  # Backward compatibility: If 'objects' is omitted, the script falls back to these single-item keys
  # shared_view_name: "<LEGACY_SECURE_VIEW_NAME>"
  # source_table: "<LEGACY_SOURCE_TABLE>"
  # view_where: "<LEGACY_OPTIONAL_PREDICATE>"

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

## Script flow and SQL execution order

Below is the exact order of operations performed by `provision_reader.py`, including the SQL statements executed and which Snowflake account they run in.

High-level phases:
- Provider account (your main Snowflake account): prepare data, create a secure view, create and grant a Share, create/ensure the Managed Reader Account, and add it to the Share.
- Reader account (the managed account that Snowflake hosts for your recipients): create a warehouse, create a database FROM SHARE, grant access, create/update a user, and run a simple test query.

Notes on idempotency:
- The script is safe to re-run. Objects are created with IF NOT EXISTS or OR REPLACE where appropriate, and errors indicating “already exists” are handled.
- Some steps (e.g., adding the account to the share) may surface a benign message if already applied; the script recognizes those and continues.

1) Provider account
- Determine provider account identifier used later by the reader when referencing the share:
  - SQL: `SELECT CURRENT_ACCOUNT()`

- Ensure shared schema and secure view(s) exist (adjust names from your config):
  - SQL: `CREATE SCHEMA IF NOT EXISTS <provider_database>.<shared_schema>`
  - For each entry in `data.objects` (or the single legacy item), the script executes:
    ```sql
    CREATE OR REPLACE SECURE VIEW <provider_database>.<shared_schema>.<shared_view_name> AS
    SELECT *
    FROM <provider_database>.PUBLIC.<source_table>
    -- Optional filter from config (omit entirely if not set)
    [WHERE <predicate>]
    ```

- Create or replace the share and grant privileges from the provider to the share:
  - SQL: `CREATE OR REPLACE SHARE <share_name>`
  - SQL: `GRANT USAGE ON DATABASE <provider_database> TO SHARE <share_name>`
  - SQL: `GRANT USAGE ON SCHEMA <provider_database>.<shared_schema> TO SHARE <share_name>`
  - SQL: For each secure view created: `GRANT SELECT ON VIEW <provider_database>.<shared_schema>.<shared_view_name> TO SHARE <share_name>`

- Ensure the Managed Reader Account exists (created if missing):
  - Lookup existing:
    - SQL: `SHOW MANAGED ACCOUNTS LIKE '<reader_account_name>'`
  - If not found, create:
    ```sql
    CREATE MANAGED ACCOUNT <reader_account_name>
      TYPE = READER
      ADMIN_NAME = '<reader_admin_user>'
      ADMIN_PASSWORD = '<reader_admin_password>'
      COMMENT = 'Automated reader account';
    ```
  - Re-check:
    - SQL: `SHOW MANAGED ACCOUNTS LIKE '<reader_account_name>'`

- Add the reader account (by locator) to the share:
  - SQL: `ALTER SHARE <share_name> ADD ACCOUNTS = <reader_account_locator>`

2) Reader account
- Connect using the account URL returned from `SHOW MANAGED ACCOUNTS` (the script derives the connector account identifier from this URL). The reader admin role used is `ACCOUNTADMIN` within the reader account.

- Ensure a warehouse exists for the reader:
  ```sql
  CREATE OR REPLACE WAREHOUSE <reader_warehouse_name>
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
  ```

- Create or replace a database from the provider share:
  ```sql
  CREATE OR REPLACE DATABASE <reader_db_name>
    FROM SHARE <provider_current_account_id>.<share_name>;
  ```
  Note: `<provider_current_account_id>` is the value returned by `SELECT CURRENT_ACCOUNT()` in the provider phase.

- Apply grants inside the reader account:
  - SQL: `GRANT IMPORTED PRIVILEGES ON DATABASE <reader_db_name> TO ROLE PUBLIC`
  - SQL: `GRANT USAGE ON WAREHOUSE <reader_warehouse_name> TO ROLE PUBLIC`

- Ensure the end user exists in the reader account:
  - Check for existing:
    - SQL: `SHOW USERS LIKE '<end_user_login_name>'`
  - If not found, create user with a temporary password and defaults:
    ```sql
    CREATE USER <end_user_login_name>
      LOGIN_NAME = '<end_user_login_name>'
      PASSWORD = '<temp_password>'
      MUST_CHANGE_PASSWORD = TRUE
      DEFAULT_ROLE = 'PUBLIC'
      DEFAULT_WAREHOUSE = '<reader_warehouse_name>'
      EMAIL = '<end_user_email>';
    ```
  - If found, update metadata without resetting password:
    ```sql
    ALTER USER <end_user_login_name>
      SET EMAIL = '<end_user_email>',
          DEFAULT_WAREHOUSE = '<reader_warehouse_name>',
          DEFAULT_ROLE = 'PUBLIC';
    ```

- Test query to validate access and the view(s):
  - SQL: `USE WAREHOUSE <reader_warehouse_name>`
  - SQL: `USE DATABASE <reader_db_name>`
  - SQL: `USE SCHEMA <shared_schema>`
  - For each shared view: `SELECT COUNT(*) FROM <shared_view_name>`

If SMTP settings are provided and a new reader user was created in this run, the script attempts to send an email with initial login details and the reader account URL.
