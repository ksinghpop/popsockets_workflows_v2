from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import base64
import os
from popsockets_etl.modules.transformation.generic import validate_mime_type

# ['https://mail.google.com/']

class GmailApi:
    def __init__(self,credential_file_path:str,email_account:str,scopes:list) -> None:
        self.credential_file_path=credential_file_path
        self.email_account=email_account
        self.scopes=scopes


    def get_credentials(self):
        credentials = service_account.Credentials.from_service_account_file(
                                    filename=self.credential_file_path,
                                    scopes=self.scopes,
                                    subject=self.email_account
                                )
        return credentials
    
    def get_gmail_service(self):
        gmail_service = build('gmail','v1',credentials=self.get_credentials())
        return gmail_service
    
    def get_message_list(self,query:str=None):
        final_response = None
        if query is not None:
            response = self.get_gmail_service().users().messages().list(userId='me',q=query).execute()
            if 'messages' in response.keys():
                final_response = response['messages']
        else:
            response = self.get_gmail_service().users().messages().list(userId='me').execute()
            if 'messages' in response.keys():
                final_response = response['messages']
        
        return final_response
    
    def get_message_detail(self,message_id:str):
        response = self.get_gmail_service().users().messages().get(userId='me', id=message_id).execute()
        return response
    
    def get_attachments(self,message_list:list):
        attachment_ids_list = []
        if message_list is not None:
            try:
                for message in message_list:
                    message_response = self.get_message_detail(message_id=message['id'])
                    for header in message_response['payload']['headers']:
                        if header['name']=='Date':
                            date = header['value']
                    for part in message_response['payload']['parts']:
                        try:                    
                            temp_dict = {}
                            temp_dict.update({"date":date})
                            temp_dict.update({"messageId":message['id']})
                            temp_dict.update({"threadId":message['threadId']})
                            temp_dict.update({"filename":part['filename']})
                            temp_dict.update({"attachmentId":part['body']['attachmentId']})
                            attachment_ids_list.append(temp_dict)
                        except:
                            pass
            except:
                pass
            
        return attachment_ids_list
    
    def get_attachment_files(self,query:str,output_dir:str=None):
        final_attachment_list = []
        gmail_service = self.get_gmail_service()
        message_list = self.get_message_list(query=query)
        attachment_lists = self.get_attachments(message_list)
        attachment_pos = 1
        for attachment in attachment_lists:
            attachment_response = gmail_service.users().messages().attachments().get(userId='me', messageId=attachment['messageId'], id=attachment['attachmentId']).execute()
            file_data = base64.urlsafe_b64decode(attachment_response.get('data').encode('UTF-8'))
            final_data_dict = {
                "date":attachment['date'],
                "messageId":attachment['messageId'],
                "threadId":attachment['threadId'],
                "filename":attachment['filename'],
                "attachmentId":attachment['attachmentId'],
                "data":file_data,
                "pos":attachment_pos
            }
            attachment_ext = validate_mime_type(bytes_data=final_data_dict['data'],get_ext=True)
            splitted_filename = os.path.splitext(attachment['filename'])
            final_data_dict.update({'filename':f"{splitted_filename[0]}{attachment_ext}"})
            final_attachment_list.append(final_data_dict)
            if output_dir is not None:
                new_filename = f"{splitted_filename[0]}_{attachment_pos}{attachment_ext}"
                os.makedirs(output_dir,exist_ok=True)
                with open(f"{output_dir}/{new_filename}",'wb') as f:
                    f.write(file_data)
            attachment_pos += 1


        return final_attachment_list