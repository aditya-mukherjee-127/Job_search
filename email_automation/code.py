import base64
import mimetypes
import os
from email.message import EmailMessage
import datetime
import pandas as pd
import random

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from email_config import EMAIL_TEMPLATES, FOLLOWUP_TEMPLATES, EMAIL_MINE, PHONE, LINKEDIN, GITHUB, SPREADSHEET_ID

SCOPES = ["https://mail.google.com/", "https://www.googleapis.com/auth/spreadsheets"]

class Mailing:
    def __init__(self):
        creds = None
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
                creds = flow.run_local_server(port=0)
            with open("token.json", "w") as token:
                token.write(creds.to_json())
        self.creds = creds
        self.service_gmail = build("gmail", "v1", credentials=creds)
        self.service_sheets = build("sheets", "v4", credentials=creds)

    
    def __send_email(self, to, subject, body, attachments=[]):
        message = EmailMessage()
        message.set_content(body)
        message["To"] = to
        message["From"] = EMAIL_MINE
        message["Subject"] = subject
        message.set_content(body, subtype='html')

        if attachments:
            for attachment in attachments:
                type_subtype, _ = mimetypes.guess_type(attachment)
                maintype, subtype = type_subtype.split('/')
                print(f"Attaching {attachment} as {maintype}/{subtype}")
                if not os.path.exists(attachment):
                    print(f"Attachment {attachment} does not exist.")
                    continue
                with open(attachment, "rb") as f:
                    attachment_data = f.read()
                message.add_attachment(
                    attachment_data,
                    maintype=maintype,
                    subtype=subtype,
                    filename=os.path.basename(attachment)
                )

        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        try:
            self.service_gmail.users().messages().send(userId="me", body={"message": {"raw": encoded_message}}).execute()
            print("Email sent successfully.")
        except HttpError as error:
            print(f"An error occurred: {error}")
    
    def send_initial_mail(self, to, company_name, email_template_idx, location="India", attachments=[]):
        subject = EMAIL_TEMPLATES[email_template_idx]["subject"].format(company_name=company_name)
        body = EMAIL_TEMPLATES[email_template_idx]["body"].format(
            recruiter=to,
            company_name=company_name,
            location=location,
            EMAIL_MINE=EMAIL_MINE,
            PHONE=PHONE,
            LINKEDIN=LINKEDIN,
            GITHUB=GITHUB,
            email_time=datetime.datetime.now().hour
        )
        self.__send_email(to, subject, body, attachments)

    def send_followup_email(self, to, company_name, email_template_idx, location="India"):
        subject = FOLLOWUP_TEMPLATES[email_template_idx]["subject"].format(company_name=company_name)
        body = FOLLOWUP_TEMPLATES[email_template_idx]["body"].format(
            recruiter=to,
            company_name=company_name,
            location=location,
            EMAIL_MINE=EMAIL_MINE,
            PHONE=PHONE,
            LINKEDIN=LINKEDIN,
            GITHUB=GITHUB
        )
        self.__send_email(to, subject, body)
    
    def read_spreadsheet(self):
        try:
            result = self.service_sheets.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID,
                range="Email_list"
            ).execute()
            values = result.get("values", [])
            data = pd.DataFrame(data=values[1:], columns=values[0])
            return data
        except HttpError as error:
            print(f"An error occurred: {error}")
            return pd.DataFrame()
    
    def write_to_spreadsheet(self, data):
        try:
            body = {
                "values": data.values.tolist()
            }
            self.service_sheets.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range="Email_list",
                valueInputOption="RAW",
                body=body
            ).execute()
            print("Spreadsheet updated successfully.")
        except HttpError as error:
            print(f"An error occurred: {error}")
    

def main():
    mailing = Mailing()
    data = mailing.read_spreadsheet()
    if data.empty:
        print("No data found in the spreadsheet.")
        return
    
    data.fillna("", inplace=True)
    data.replace("NA", "", inplace=True)
    print(data.head())
    
    for index, row in data.iterrows():
        template_idx = random.randint(0, len(EMAIL_TEMPLATES) - 1)
        if not row["initial_mail_sent"] or (datetime.datetime.now() - datetime.datetime.strptime(row["initial_mail_sent"], "%Y-%m-%d %H:%M:%S")) > datetime.timedelta(days=21):
            mailing.send_initial_mail(
                to=row["email"],
                company_name=row["company"],
                email_template_idx=template_idx,
                location=row["location"],
                attachments=["AdityaMukherjee_Analyst.pdf"]
            )
            data.at[index, "initial_mail_sent"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elif (datetime.datetime.now() - datetime.datetime.strptime(row["initial_mail_sent"], "%Y-%m-%d %H:%M:%S")) > datetime.timedelta(days=7):
            mailing.send_followup_email(
                to=row["email"],
                company_name=row["company"],
                email_template_idx=template_idx,
                location=row["location"]
            )
            data.at[index, "followup_mail_sent"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if __name__ == "__main__":
    main()