"""This module contains the main process of the robot."""

import json

import traceback

from io import BytesIO

import pandas as pd

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from itk_dev_shared_components.smtp import smtp_util

from mbu_dev_shared_components.database import constants

from mbu_dev_shared_components.msoffice365.sharepoint_api.files import Sharepoint

from robot_framework.sub_processes import helper_functions
from robot_framework.sub_processes import formular_mappings


def process(orchestrator_connection: OrchestratorConnection) -> None:
    """Do the primary process of the robot."""

    orchestrator_connection.log_trace("Running process.")
    print("Running process.")

    sql_server_connection_string = orchestrator_connection.get_constant("DbConnectionString").value

    os2_webform_id = json.loads(orchestrator_connection.process_arguments)["os2_webform_id"]

    date_today = pd.Timestamp.now().date()

    current_day_of_month = str(pd.Timestamp.now().day)
    if current_day_of_month == "1":
        print("Today is the first of the month - we will update the Excel files with new submissions.")
        orchestrator_connection.log_trace("Today is the first of the month - we will update the Excel files with new submissions.")

        # Last + first day of last month
        end_date = date_today.replace(day=1) - pd.Timedelta(days=1)
        start_date = end_date.replace(day=1)

        username = orchestrator_connection.get_credential("SvcRpaMBU002").username
        password = orchestrator_connection.get_credential("SvcRpaMBU002").password

        sharepoint_api = Sharepoint(
            username=username,
            password=password,
            site_url="https://aarhuskommune.sharepoint.com",
            site_name="MBURPA",  # CHANGE TO CENTER FOR TRIVSEL WHEN DEPLOYED
            document_library="Delte dokumenter"
        )

        folder_name = "Misc"  # CHANGE TO "Center for trivsel" WHEN DEPLOYED
        unge_excel_file_name = "center_for_trivsel_esq_unge.xlsx"  # CHANGE WHEN DEPLOYED
        foraeldre_excel_file_name = "center_for_trivsel_esq_foraeldre.xlsx"  # CHANGE WHEN DEPLOYED

        files_in_sharepoint = sharepoint_api.fetch_files_list(folder_name=folder_name)

        # Fetch all submissions once for the whole period
        all_submissions = helper_functions.get_forms_data(sql_server_connection_string, os2_webform_id)

        # Split and transform into two DataFrames
        df_unge = helper_functions.build_df(all_submissions, "Ung/selvbesvarelse", formular_mappings.center_for_trivsel_esq_barn_mapping)
        df_foraeldre = helper_functions.build_df(all_submissions, "Forælder (inklusiv plejeforældre)", formular_mappings.center_for_trivsel_esq_foraelder_mapping)

        file_map = {
            unge_excel_file_name: df_unge,
            foraeldre_excel_file_name: df_foraeldre
        }

        for excel_file_name, df in file_map.items():
            if excel_file_name not in files_in_sharepoint:
                print(f"Excel file '{excel_file_name}' not found - creating new.")
                orchestrator_connection.log_trace(f"Excel file '{excel_file_name}' not found - creating new.")

                excel_stream = BytesIO()
                df.to_excel(excel_stream, index=False, engine="openpyxl", sheet_name="Submissions")
                excel_stream.seek(0)

                sharepoint_api.upload_file_from_bytes(
                    binary_content=excel_stream.getvalue(),
                    file_name=excel_file_name,
                    folder_name=folder_name
                )

            else:
                print(f"Fetching forms from {start_date} to {end_date} for '{excel_file_name}'.")
                ranged_submissions = helper_functions.get_forms_data(
                    sql_server_connection_string,
                    os2_webform_id,
                    start_date=start_date,
                    end_date=end_date
                )

                # Filter/transform for just this file
                if excel_file_name == unge_excel_file_name:
                    new_rows = helper_functions.build_df(ranged_submissions, "Ung/selvbesvarelse", formular_mappings.center_for_trivsel_esq_barn_mapping)

                else:
                    new_rows = helper_functions.build_df(ranged_submissions, "Forælder (inklusiv plejeforældre)", formular_mappings.center_for_trivsel_esq_foraelder_mapping)

                if not new_rows.empty:
                    sharepoint_api.append_row_to_sharepoint_excel(
                        folder_name=folder_name,
                        excel_file_name=excel_file_name,
                        sheet_name="Submissions",
                        new_rows=new_rows.to_dict(orient="records")
                    )

            # Format after create/append
            sharepoint_api.format_and_sort_excel_file(
                folder_name=folder_name,
                excel_file_name=excel_file_name,
                sheet_name="Submissions",
                sorting_keys=[{"key": "A", "ascending": False, "type": "str"}],
                bold_rows=[1],
                align_horizontal="left",
                align_vertical="top",
                italic_rows=None,
                font_config=None,
                column_widths=100,
                freeze_panes="A2"
            )

            print()
            print()

    # ALWAYS RUN DAILY EMAIL SUBMISSION FLOW
    orchestrator_connection.log_trace("Running daily email submission flow.")
    print("Running daily email submission flow.")

    date_yesterday = (pd.Timestamp.now() - pd.Timedelta(days=1)).date()
    all_yesterdays_forms = helper_functions.get_forms_data(sql_server_connection_string, os2_webform_id, target_date=date_yesterday)

    forms_by_cpr = {}

    for form in all_yesterdays_forms:
        try:
            serial = form["entity"]["serial"][0]["value"]

            udfylder_rolle = form["data"]["hvem_udfylder_spoergeskemaet"]

            if udfylder_rolle == "Ung/selvbesvarelse":
                mapping = formular_mappings.center_for_trivsel_esq_barn_mapping

            elif udfylder_rolle == "Forælder (inklusiv plejeforældre)":
                mapping = formular_mappings.center_for_trivsel_esq_foraelder_mapping

            else:
                continue

            transformed_row = formular_mappings.transform_form_submission(serial, form, mapping)

            cpr = transformed_row["Barnets/Den unges CPR-nummer"]

            if cpr not in forms_by_cpr:
                forms_by_cpr[cpr] = []

            forms_by_cpr[cpr].append({
                "form": form,
                "transformed": transformed_row,
                "role": udfylder_rolle,
                "mapping": mapping
            })

        except Exception as e:
            print(f"Error processing form: {e}")

            continue

    for cpr, entries in forms_by_cpr.items():
        sections = []

        for entry in entries:
            transformed_row = entry["transformed"]
            role = entry["role"]
            mapping = entry["mapping"]

            table_att = {
                "Udfyldt": transformed_row["Gennemført"],
                "Behandling": transformed_row["Behandling"],
                "Barnets/Den unges navn": transformed_row["Barnets/Den unges navn"],
                "Barnets/Den unges CPR-nummer": transformed_row["Barnets/Den unges CPR-nummer"],
                "Barnets/Den unges alder": transformed_row["Barnets/Den unges alder"],
            }

            if role == "Forælder (inklusiv plejeforældre)":
                table_att["Forælder navn"] = transformed_row["Navn"]
                table_att["Forælder cpr-Nummer"] = transformed_row["CPR-nummer"]

                for _, spg in mapping["spoergsmaal_foraelder_tabel"].items():
                    table_att[spg] = transformed_row.get(spg)

            else:
                for _, spg in mapping["spoergsmaal_barn_tabel"].items():
                    table_att[spg] = transformed_row.get(spg)

            table_att["Average answer score"] = transformed_row["Average answer score"]

            html_table = helper_functions.format_html_table(table_att)

            sections.append(
                f"<p><strong>Udfylder rolle:</strong> {role}</p><br>{html_table}<br><br>"
            )

        email_body = (
            f"<p>Ny(e) besvarelse(r) til ESQ formular for barn med CPR: <strong>{cpr}</strong></p>"
            + "<hr>".join(sections)
        )

        try:
            smtp_util.send_email(
                receiver=transformed_row["Tilkoblet email"],
                sender=constants.get_constant("e-mail_noreply")["value"],
                subject="Ny(e) ESQ besvarelse(r)",
                body=email_body,
                html_body=email_body,
                smtp_server=constants.get_constant("smtp_server", db_env="PROD")["value"],
                smtp_port=constants.get_constant("smtp_port", db_env="PROD")["value"],
                attachments=None,
            )

        except Exception as e:
            print("❌ Failed to send email")

            print(f"➡️ Error: {e}")

            traceback.print_exc()

    orchestrator_connection.log_trace("Process completed successfully.")
    print("Process completed successfully.")

    return "Process completed successfully."
