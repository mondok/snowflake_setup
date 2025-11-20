#!/usr/bin/env python3
"""
Provision a Snowflake reader account + share end-to-end, idempotently,
and create a login user in the reader account.

See config.yaml for structure.
"""

import sys
import time
import smtplib
from email.message import EmailMessage
import yaml
import snowflake.connector
from snowflake.connector.errors import ProgrammingError


# ---------- simple logging ----------

def log(msg: str) -> None:
    print(f"[provision] {msg}")


# ---------- config loader ----------

def load_config(path: str = "config.yaml") -> dict:
    try:
        with open(path, "r") as f:
            cfg = yaml.safe_load(f)
        return cfg
    except Exception as e:
        log(f"ERROR: Failed to read config file '{path}': {e}")
        sys.exit(1)


# ---------- helper functions ----------

def get_managed_account(cur, account_name: str):
    """
    Look up an existing managed account by name and return its info:
    {
      'account_name': ...,
      'account_locator': ...,
      'account_url': ...
    }
    or None if not found.

    We use SHOW MANAGED ACCOUNTS LIKE '<name>'.
    """
    log(f"SHOW MANAGED ACCOUNTS LIKE '{account_name}' ...")
    cur.execute(f"SHOW MANAGED ACCOUNTS LIKE '{account_name}'")
    rows = cur.fetchall()
    if not rows:
        return None

    row = rows[0]
    # Column order (based on Snowflake docs / typical output):
    # 0: account_name
    # 1: cloud
    # 2: region
    # 3: account_locator
    # 4: created_on
    # 5: account_url
    account_name_val = row[0]
    account_locator_val = row[3]
    account_url_val = row[5]

    log(
        f"Found managed account row: name={account_name_val}, "
        f"locator={account_locator_val}, url={account_url_val}"
    )
    return {
        "account_name": account_name_val,
        "account_locator": account_locator_val,
        "account_url": account_url_val,
    }


def ensure_managed_account(cur, account_name: str, admin_user: str, admin_password: str):
    """
    Idempotently ensure the managed (reader) account exists.
    Returns dict with: account_name, account_locator, account_url.
    """
    # 1) If it's already there, just reuse it.
    info = get_managed_account(cur, account_name)
    if info:
        log(
            f"Managed account '{account_name}' already exists. "
            f"Using locator {info['account_locator']}."
        )
        return info

    # 2) Try to create it.
    log(f"Managed account '{account_name}' not found. Creating...")
    try:
        cur.execute(f"""
            CREATE MANAGED ACCOUNT {account_name}
              TYPE = READER
              ADMIN_NAME = '{admin_user}'
              ADMIN_PASSWORD = '{admin_password}'
              COMMENT = 'Automated reader account for EXCLUDEDLISTS share'
        """)
        log("CREATE MANAGED ACCOUNT executed.")
    except ProgrammingError as e:
        msg = str(e)
        if "already exists" in msg:
            log(
                "CREATE MANAGED ACCOUNT says object already exists; "
                "assuming managed account was created previously. "
                "Re-checking SHOW MANAGED ACCOUNTS..."
            )
            info = get_managed_account(cur, account_name)
            if info:
                log(f"Using existing managed account locator {info['account_locator']}.")
                return info
            log(
                "Name collision: object exists but not visible as managed account. "
                "Either drop/rename that object or use a different "
                "reader.account_name in config.yaml."
            )
            sys.exit(1)
        else:
            log(f"ERROR creating managed account: {e}")
            sys.exit(1)

    # 3) Give Snowflake a moment to register it
    time.sleep(5)

    info = get_managed_account(cur, account_name)
    if not info:
        log("ERROR: Managed account creation succeeded but not found in SHOW MANAGED ACCOUNTS.")
        sys.exit(1)
    return info


def ensure_share_has_account(cur, share_name: str, locator: str) -> None:
    """
    Idempotently ensure the reader account (locator) is added to the share.
    """
    log(f"Ensuring account {locator} is added to share {share_name}...")
    try:
        cur.execute(f"ALTER SHARE {share_name} ADD ACCOUNTS = {locator}")
        log(f"Account {locator} added to share {share_name}.")
    except ProgrammingError as e:
        msg = str(e)
        # If it's already there, Snowflake says "Following accounts cannot be added to this share"
        if "cannot be added to this share" in msg:
            log(f"Account {locator} already present in share {share_name}; continuing.")
        else:
            log(f"ERROR adding account to share: {e}")
            sys.exit(1)


def account_identifier_from_url(account_url: str) -> str:
    """
    Given account_url from SHOW MANAGED ACCOUNTS, e.g.
      https://orgname-accountname.snowflakecomputing.com
    return the Python connector account identifier:
      orgname-accountname
    """
    url = account_url
    if url.startswith("https://"):
        url = url[len("https://") :]
    elif url.startswith("http://"):
        url = url[len("http://") :]

    # Drop any path
    url = url.split("/", 1)[0]
    # Drop the .snowflakecomputing.com suffix
    if ".snowflakecomputing.com" in url:
        acct = url.split(".snowflakecomputing.com", 1)[0]
    else:
        acct = url
    return acct


def ensure_reader_user(cur, user_cfg: dict, default_wh: str):
    """
    Idempotently ensure a user exists inside the reader account with the given
    email and default warehouse.

    If the user already exists, we update EMAIL and DEFAULT_WAREHOUSE but do
    NOT reset the password (to avoid surprising the user).
    """
    if not user_cfg:
        log("No reader_user section in config; skipping user creation.")
        return {"created": False}

    name = user_cfg["name"]
    email = user_cfg["email"]
    temp_pw = user_cfg["temp_password"]

    log(f"Ensuring reader user '{name}' exists...")

    # SHOW USERS LIKE '<name>' will match on user name
    cur.execute(f"SHOW USERS LIKE '{name}'")
    rows = cur.fetchall()

    if not rows:
        # Create fresh user
        log(f"User '{name}' not found. Creating...")
        cur.execute(f"""
            CREATE USER {name}
              LOGIN_NAME = '{name}'
              PASSWORD = '{temp_pw}'
              MUST_CHANGE_PASSWORD = TRUE
              DEFAULT_ROLE = 'PUBLIC'
              DEFAULT_WAREHOUSE = '{default_wh}'
              EMAIL = '{email}'
        """)
        log(f"User '{name}' created with email {email}.")
        return {"created": True, "name": name, "email": email, "temp_password": temp_pw}

    # User exists: update metadata but do not reset password
    log(f"User '{name}' already exists. Updating email and default warehouse...")
    cur.execute(f"""
        ALTER USER {name}
          SET EMAIL = '{email}',
              DEFAULT_WAREHOUSE = '{default_wh}',
              DEFAULT_ROLE = 'PUBLIC'
    """)
    log(f"User '{name}' updated (EMAIL={email}, DEFAULT_WAREHOUSE={default_wh}).")
    return {"created": False, "name": name, "email": email}


def send_credentials_email(user_name: str, user_email: str, temp_password: str, login_url: str, smtp_cfg: dict) -> None:
    """
    Send the reader's initial credentials via email using SMTP settings from config.

    smtp_cfg expected keys (optional defaults in parentheses):
      - host (required)
      - port (587 if use_tls True, else 25; 465 if use_ssl True)
      - user (optional)
      - password (optional)
      - from (optional, falls back to user or 'no-reply@snowflake')
      - use_tls (bool, default True)
      - use_ssl (bool, default False)
    """
    if not smtp_cfg or not smtp_cfg.get("host"):
        log("SMTP config missing or incomplete; skipping email notification.")
        return

    use_ssl = bool(smtp_cfg.get("use_ssl", False))
    use_tls = bool(smtp_cfg.get("use_tls", True)) if not use_ssl else False
    host = smtp_cfg["host"]
    port = smtp_cfg.get("port")
    if port is None:
        port = 465 if use_ssl else (587 if use_tls else 25)
    username = smtp_cfg.get("user")
    password = smtp_cfg.get("password")
    from_addr = smtp_cfg.get("from") or username or "no-reply@snowflake"

    subject = "Your Snowflake Reader Account Credentials"
    body = (
        "Hello,\n\n"
        f"Your Snowflake reader account has been provisioned.\n\n"
        f"Login URL: {login_url}\n"
        f"Username: {user_name}\n"
        f"Temporary Password: {temp_password}\n\n"
        "You will be prompted to change your password on first login.\n\n"
        "If you did not expect this email, please contact the sender.\n"
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = user_email
    msg.set_content(body)

    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host, port)
        else:
            server = smtplib.SMTP(host, port)
        with server:
            server.ehlo()
            if use_tls and not use_ssl:
                server.starttls()
                server.ehlo()
            if username and password:
                server.login(username, password)
            server.send_message(msg)
        log(f"Credentials email sent to {user_email}.")
    except Exception as e:
        log(f"WARNING: Failed to send credentials email to {user_email}: {e}")


# ---------- main ----------

def main():
    cfg = load_config()
    provider_cfg = cfg["provider"]
    reader_cfg = cfg["reader"]
    reader_user_cfg = cfg.get("reader_user")
    share_cfg = cfg["share"]
    data_cfg = cfg["data"]

    provider_account = provider_cfg["account"]
    provider_user = provider_cfg["user"]
    provider_password = provider_cfg["password"]
    provider_role = provider_cfg["role"]

    reader_account_name = reader_cfg["account_name"]
    reader_admin_user = reader_cfg["admin_user"]
    reader_admin_password = reader_cfg["admin_password"]
    reader_wh_name = reader_cfg["warehouse_name"]
    reader_db_name = reader_cfg["db_name"]

    share_name = share_cfg["name"]

    provider_db = data_cfg["provider_database"]
    shared_schema = data_cfg["shared_schema"]
    # Support multiple source tables/views via data.objects list, with backward compatibility
    objects_cfg = data_cfg.get("objects")
    if isinstance(objects_cfg, list) and objects_cfg:
        objects = objects_cfg
    else:
        # Fallback to single keys for backward compatibility
        objects = [
            {
                "shared_view_name": data_cfg["shared_view_name"],
                "source_table": data_cfg["source_table"],
                "view_where": data_cfg.get("view_where"),
            }
        ]

    def normalize_where(where_val):
        where_val = (where_val or "").strip()
        if not where_val:
            return ""
        if not where_val.lower().lstrip().startswith("where "):
            return f"WHERE {where_val}"
        return where_val

    # ------------- connect to provider -------------

    log(
        f"Connecting to provider account '{provider_account}' as {provider_user} "
        f"with role {provider_role}..."
    )
    try:
        provider_conn = snowflake.connector.connect(
            user=provider_user,
            password=provider_password,
            account=provider_account,
            role=provider_role,
        )
    except Exception as e:
        log(f"ERROR: Failed to connect to provider account: {e}")
        sys.exit(1)

    provider_cur = provider_conn.cursor()
    log("Connected to provider.")

    try:
        # Get provider account identifier (locator/ID) for FROM SHARE
        provider_cur.execute("SELECT CURRENT_ACCOUNT()")
        provider_share_account_id = provider_cur.fetchone()[0]
        log(f"Provider share account identifier (CURRENT_ACCOUNT) = {provider_share_account_id}")

        # 1. Create schema + secure views
        log("Ensuring shared schema and secure view(s) exist...")

        provider_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {provider_db}.{shared_schema}")

        for obj in objects:
            sv_name = obj["shared_view_name"]
            src_table = obj["source_table"]
            view_where_sql = normalize_where(obj.get("view_where"))
            provider_cur.execute(
                (
                    f"CREATE OR REPLACE SECURE VIEW {provider_db}.{shared_schema}.{sv_name} AS\n"
                    f"SELECT *\n"
                    f"FROM {provider_db}.PUBLIC.{src_table}\n"
                    + (f"{view_where_sql}\n" if view_where_sql else "")
                )
            )
            log(f"Secure view {provider_db}.{shared_schema}.{sv_name} is ready.")

        # 2. Create share + grants
        log(f"Ensuring share {share_name} exists with proper grants...")
        provider_cur.execute(f"CREATE OR REPLACE SHARE {share_name}")

        provider_cur.execute(f"GRANT USAGE ON DATABASE {provider_db} TO SHARE {share_name}")
        provider_cur.execute(f"GRANT USAGE ON SCHEMA {provider_db}.{shared_schema} TO SHARE {share_name}")
        # Grant SELECT on each view to the share
        for obj in objects:
            sv_name = obj["shared_view_name"]
            provider_cur.execute(
                f"GRANT SELECT ON VIEW {provider_db}.{shared_schema}.{sv_name} TO SHARE {share_name}"
            )
        log("Share and privileges ensured.")

        # 3. Ensure managed account exists + get info (name, locator, url)
        acct_info = ensure_managed_account(
            provider_cur,
            account_name=reader_account_name,
            admin_user=reader_admin_user,
            admin_password=reader_admin_password,
        )
        locator = acct_info["account_locator"]
        account_url = acct_info["account_url"]
        log(f"Using reader managed account: locator={locator}, url={account_url}")

        # 4. Ensure share is granted to reader account
        ensure_share_has_account(provider_cur, share_name, locator)

    finally:
        provider_cur.close()
        provider_conn.close()
        log("Closed provider connection.")

    # ------------- connect to reader -------------

    reader_account_identifier = account_identifier_from_url(account_url)
    log(f"Connecting to reader account '{reader_account_identifier}' as {reader_admin_user}...")
    try:
        reader_conn = snowflake.connector.connect(
            user=reader_admin_user,
            password=reader_admin_password,
            account=reader_account_identifier,
            role="ACCOUNTADMIN",      # reader admin has ACCOUNTADMIN inside reader account
        )
    except Exception as e:
        log(f"ERROR: Failed to connect to reader account: {e}")
        sys.exit(1)

    reader_cur = reader_conn.cursor()
    log("Connected to reader account.")

    try:
        # 5. Create / replace warehouse in reader
        log(f"Ensuring warehouse {reader_wh_name} exists in reader account...")
        reader_cur.execute(f"""
            CREATE OR REPLACE WAREHOUSE {reader_wh_name}
              WAREHOUSE_SIZE = 'XSMALL'
              AUTO_SUSPEND = 60
              AUTO_RESUME = TRUE
              INITIALLY_SUSPENDED = TRUE
        """)
        log("Warehouse ensured.")

        # 6. Create / replace database from share in reader
        log(f"Ensuring database {reader_db_name} FROM SHARE is created in reader account...")
        reader_cur.execute(f"""
            CREATE OR REPLACE DATABASE {reader_db_name}
              FROM SHARE {provider_share_account_id}.{share_name}
        """)
        log("Shared database ensured.")

        # 7. Grants in reader
        log("Applying grants in reader account (idempotent)...")
        reader_cur.execute(
            f"GRANT IMPORTED PRIVILEGES ON DATABASE {reader_db_name} TO ROLE PUBLIC"
        )
        reader_cur.execute(
            f"GRANT USAGE ON WAREHOUSE {reader_wh_name} TO ROLE PUBLIC"
        )

        # 8. Ensure a real user exists in reader account
        user_result = ensure_reader_user(reader_cur, reader_user_cfg, reader_wh_name)

        # 8b. If created, email credentials (if SMTP config provided)
        if user_result and user_result.get("created"):
            smtp_cfg = cfg.get("smtp") or {}
            try:
                send_credentials_email(
                    user_name=user_result["name"],
                    user_email=user_result["email"],
                    temp_password=user_result["temp_password"],
                    login_url=account_url,
                    smtp_cfg=smtp_cfg,
                )
            except Exception as e:
                # Be resilient: email failures should not abort provisioning
                log(f"WARNING: Error during email dispatch: {e}")

        # 9. Test query
        log("Running test query in reader account...")
        reader_cur.execute(f"USE WAREHOUSE {reader_wh_name}")
        reader_cur.execute(f"USE DATABASE {reader_db_name}")
        reader_cur.execute(f"USE SCHEMA {shared_schema}")

        # Run a simple COUNT(*) against each shared view
        for obj in objects:
            sv_name = obj["shared_view_name"]
            reader_cur.execute(f"SELECT COUNT(*) FROM {sv_name}")
            count = reader_cur.fetchone()[0]
            log(f"Test query OK. Row count from view {sv_name}: {count}")

    finally:
        reader_cur.close()
        reader_conn.close()
        log("Closed reader connection.")

    log("✅ DONE. Reader account, share, warehouse, DB, and reader user are fully provisioned and idempotent.")


if __name__ == "__main__":
    main()