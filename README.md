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
