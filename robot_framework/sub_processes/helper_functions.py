"""This module contains helper functions."""

import json
import urllib.parse

from typing import Dict, Any

import pandas as pd

from sqlalchemy import create_engine

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection


def get_forms_data(
    conn_string: str,
    form_type: str,
    target_date: str = "",
    start_date: str = "",
    end_date: str = ""
) -> list[dict]:
    """
    Retrieve form_data['data'] for all matching submissions for the given form type.
    Supports either:
      - exact date (target_date)
      - date range (start_date + end_date)
    Skips entries marked as purged.
    """

    print("inside get_forms_data")

    # Build query depending on which filter type is used
    if start_date and end_date:
        query = """
            SELECT
                form_id,
                form_data,
                CAST(form_submitted_date AS datetime) AS form_submitted_date
            FROM
                [RPA].[journalizing].[Forms]
            WHERE
                form_type = ?
                AND form_data IS NOT NULL
                AND form_submitted_date IS NOT NULL
                AND CAST(form_submitted_date AS date) BETWEEN ? AND ?
            ORDER BY
                form_submitted_date DESC
        """

        query_params = (form_type, start_date, end_date)

    elif target_date:
        query = """
            SELECT
                form_id,
                form_data,
                CAST(form_submitted_date AS datetime) AS form_submitted_date
            FROM
                [RPA].[journalizing].[Forms]
            WHERE
                form_type = ?
                AND form_data IS NOT NULL
                AND form_submitted_date IS NOT NULL
                AND CAST(form_submitted_date AS date) = ?
            ORDER BY
                form_submitted_date DESC
        """

        query_params = (form_type, target_date)

    else:
        raise ValueError("You must provide either a target_date or both start_date and end_date.")

    # Create SQLAlchemy engine
    encoded_conn_str = urllib.parse.quote_plus(conn_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={encoded_conn_str}")

    try:
        df = pd.read_sql(sql=query, con=engine, params=query_params)

    except Exception as e:
        print("Error during pd.read_sql:", e)

        raise

    if df.empty:
        print("No submissions found for the given date(s).")

        return []

    extracted_data = []

    for _, row in df.iterrows():
        try:
            parsed = json.loads(row["form_data"])

            if "purged" not in parsed:
                extracted_data.append(parsed)

        except json.JSONDecodeError:
            print("Invalid JSON in form_data, skipping row.")

    return extracted_data


def get_credentials_and_constants(orchestrator_connection: OrchestratorConnection) -> Dict[str, Any]:
    """Retrieve necessary credentials and constants from the orchestrator connection."""
    try:
        credentials = {
            "go_api_endpoint": orchestrator_connection.get_constant('go_api_endpoint').value,
            "go_api_username": orchestrator_connection.get_credential('go_api').username,
            "go_api_password": orchestrator_connection.get_credential('go_api').password,
            "os2_api_key": orchestrator_connection.get_credential('os2_api').password,
            "sql_conn_string": orchestrator_connection.get_constant('DbConnectionString').value,
            "journalizing_tmp_path": orchestrator_connection.get_constant('journalizing_tmp_path').value,
        }
        return credentials
    except AttributeError as e:
        raise SystemExit(e) from e


def format_html_table(table_att: dict) -> str:
    """
    Create an HTML table from a dictionary of attributes.
    """

    html = '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">\n'

    for key, value in table_att.items():
        html += f'  <tr><td><strong>{key}</strong></td><td>{value}</td></tr>\n'

    html += '</table>'

    return html
