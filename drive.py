import os
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
import json
from typing import Any
from apiclient.http import MediaFileUpload
from pathlib import Path
import datetime as dt, time

# install pre-requisites
# pip3 install httplib2
# pip3 install google-api-python-client
# pip3 install oauth2client

# setup / download client secret file here
# https://console.developers.google.com/apis/credentials

flags: None = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret_mediaserver.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'


class drive(object):
    driveservice: Any = None

    def __init__(self) -> None:
        self.__connectToService()

    ###########
    # PRIVATE methods
    ###########
    def __connectToService(self):
        """Shows basic usage of the Sheets API.

        Creates a Sheets API service object and prints the names and majors of
        students in a sample spreadsheet:
        https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit
        """
        credentials = self.__get_credentials()
        # v3 is not working !!!!
        self.driveservice = discovery.build('drive', 'v3', credentials=credentials)

    def __get_credentials(self):
        """Gets valid user credentials from storage.

        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.

        Returns:
            Credentials, the obtained credential.
        """
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir,
                                       'mediaserver.json')

        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
            flow.user_agent = APPLICATION_NAME
            credentials = tools.run_flow(flow, store, flags)
            # if flags:
            #     credentials = tools.run_flow(flow, store, flags)
            # else:  # Needed only for compatibility with Python 2.6
            #     credentials = tools.run(flow, store)
            print('Storing credentials to ' + credential_path)
        return credentials

    ###########
    # PUBLIC methods
    ###########
    def findItem(self, name="", mime="application/vnd.google-apps.folder") -> str:
        id = []
        page_token = None
        while True:
            response = self.driveservice.files().list(
                q=f"name='{name}' and trashed=false and mimeType='{mime}'",
                spaces='drive',
                fields='nextPageToken, files(id, name, mimeType)',
                pageToken=page_token).execute()
            for file in response.get('files', []):
                id.append(file["id"])
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

        if id == []:  # not found
            id.append("")

        return id[0]

    def upload(self, pf):
        # google doesn't like a single quote in name...
        name = pf.name.replace("'", "")
        id = self.findItem(name, f'video/{pf.suffix.strip(".")}')
        if len(id) > 0:
            return id
        file_metadata = {'name': name,
                         "parents": [self.findItem("media")],}
        media = MediaFileUpload(pf, mimetype=f'video/{pf.suffix.strip(".")}', resumable=True)
        gf = self.driveservice.files().create(body=file_metadata,
                                                 media_body=media,
                                                 fields="id").execute()

        return gf["id"]


if __name__ == '__main__':
    from pathlib import Path

    mydrive = drive()
    f = mydrive.findItem("mum's medication".replace("'","\'"))
    print(f, "done")
    # http://mcs.local/media/movies/Mike%20and%20Dave%20Need%20Wedding%20Dates%20(2016)/Mike%20and%20Dave%20Need%20Wedding%20Dates%20%282016%29%20Bluray-720p.mp4
    # p = Path("/Volumes/WD4TB/mcs").joinpath("complete")
    # pf = p.joinpath(
    #     "movies/Mike and Dave Need Wedding Dates (2016)/Mike and Dave Need Wedding Dates (2016) Bluray-720p.mp4")

    # pf = p.joinpath("tv/24/Season 09/24 - S09E01 - Day 9 - 11.00 A.M. - 12.00 P.M.eng.srt")
    # start = time.time()
    # fileid = mydrive.upload(pf)

    # print(f"uploadtime:{(time.time()-start):.2f} https://drive.google.com/file/d/{fileid}/view?usp=sharing")
