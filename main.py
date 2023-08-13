"""

@RAPHABIZ

"""

import time
from telethon import TelegramClient
from telethon.errors.rpcerrorlist import SessionExpiredError

from telethon.errors.rpcerrorlist import ChatAdminRequiredError
import csv
import gspread_dataframe as gd
import gspread
import pandas as pd
import os
from dotenv import load_dotenv
from telethon.sessions import StringSession
import asyncio
from datetime import datetime
from telethon import functions, types
from telethon.tl.types import ChannelParticipantCreator , ChannelParticipantAdmin,ChannelParticipantsAdmins

from telethon.tl.types import MessageActionChatAddUser

load_dotenv()

api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')

class Participants:
  def __init__(self):
        """ self.participants = pd.DataFrame(columns = range(7))   """

  async def get_telegram_client(session: str = None) -> TelegramClient:
    return TelegramClient(StringSession(session), api_id, api_hash)
   
  """   def init_saving_sheet(self):
      gc = gspread.service_account(filename='pytel-394611-2285f15c6973.json')
      sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1lQnw54ekn3R8f6cYbueMHy40dUHX6iq2tuJmUQT6XMM/edit?usp=sharing')
      wks= sh.get_worksheet(0)
      return wks """

  """   def verify_duplicate_in_sheet(self,p):
      wks=Participants.init_saving_sheet(self)
      id_list = wks.col_values(1)
      print("_______")
      print(p)
      print("_______")
      for i in range(len(p)):
        print(i)
        if str(p.loc[i,'id']) in id_list:
          print("true")
          
          p.drop([i], inplace=True)
          
        else:
          print("false")
        
      return p """
  
  """   def save_to_sheet(self,part):
        wks=Participants.init_saving_sheet(self)
        data=self.verify_duplicate_in_sheet(part)
        wks.append_rows(data.values.tolist())
        print(wks) """
  
  """ def verify_duplicate_in_df(self,id,df):
      if id in df.values :
        print("true")
        return True
      else:
        print("false")
        return False """
      
  def verify_duplicate_in_csv(self, p,csvfilename):
    with open(f"{csvfilename}.csv",encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=",", quotechar='"')
        # Skip the headers
        next(reader, None)
        data_read = [row for row in reader]

    # Convert id int in string
    p_id = str(p.get('id'))

    # Check for duplicate IDs in the data_read list
    for row in data_read:
        if row[0] == p_id:
            print(f"Duplicate ID found: {p_id}")
            return True  # Duplicate found

    print("No duplicate found.")
    return False  # No duplicate found


  def create_csv_if_not_exists(self,filepath, header):

    if not os.path.exists(filepath+'.csv'):
        with open(filepath+'.csv', 'w', newline='') as csv_file:
            if header:
                csv_writer = csv.writer(csv_file)
                csv_writer.writerow(header)
        print(f"CSV file '{filepath}' created.")
    else:
        print(f"CSV file '{filepath}' already exists.")



  def save_to_csv(self, part,columns,csvfilename):

    # Verify if csv file exists
    self.create_csv_if_not_exists(csvfilename,columns)
    
    # Verify if participant is alredy in file
    if self.verify_duplicate_in_csv(part,csvfilename=csvfilename) == False:
       # Append data to the CSV file
       with open(f'{csvfilename}.csv', 'a', newline='', encoding='UTF-8') as f:
          w = csv.DictWriter(f, fieldnames=columns)
          print(part)
          w.writerow(part)
     

  def get_last_entry_date(self,filepath):
        try:
            with open(filepath+'.csv', "r", newline='', encoding='UTF-8') as f:
                reader = csv.reader(f)
                last_row = None
                for row in reader:
                    last_row = row
                if last_row:
                    last_date_str = last_row[5] 
                    last_date = datetime.strptime(last_date_str, '%Y-%m-%d %H:%M:%S%z')
                    return last_date
        except FileNotFoundError:
            return None
        
  
  def get_member_type(self,member):

    user_type = 'participant'
    if (type(member.participant) == ChannelParticipantAdmin):
        user_type = 'admin'
    if (type(member.participant) == ChannelParticipantCreator):
        user_type = 'owner'
    return user_type
  
  async def process_joining_messages(self, client, entity):
        
        # fetch all admins users 
        await self.process_users(client, entity)

        # recup chat entity from username 
        columns = ['id', 'first_name', 'last_name', 'username', 'about', 'date', 'collectDate','role']
        async for message in client.iter_messages(entity):
            if isinstance(message.action, MessageActionChatAddUser):

                full = await client(functions.users.GetFullUserRequest(id=message.action.users[0]))

                participant = {
                    "id": message.action.users[0],
                    "first_name": full.users[0].first_name,
                    "last_name": full.users[0].last_name,
                    "username": full.users[0].username,
                    "about": full.full_user.about,
                    "collectDate": datetime.now().strftime('%Y-%m-%d %H:%M:%S%z'),
                    "role":'participant'
                } 
                self.save_to_csv(part=participant,columns=columns,csvfilename=entity.username)



  async def process_messages(self, client, entity):
        columns = ['id', 'first_name', 'last_name', 'username', 'about', 'date', 'collectDate']
        async for message in client.iter_messages(entity,offset_date=self.get_last_entry_date(filepath=entity.username)):
            if hasattr(message.from_id, "user_id"):
                result = await client(functions.users.GetFullUserRequest(id=message.from_id.user_id))
                participant = {
                    "id": message.from_id.user_id,
                    "first_name": result.users[0].first_name,
                    "last_name": result.users[0].last_name,
                    "username": result.users[0].username,
                    "about": result.full_user.about,
                    "date": message.date,
                    "collectDate": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            self.save_to_csv(part=participant,columns=columns,csvfilename=entity.username) 

  async def process_users(self, client, entity):
        columns = ['id', 'first_name', 'last_name', 'username', 'about', 'collectDate','role']
        async for participant in client.iter_participants(entity):
                # recup full user to get about variable
                full = await client(functions.users.GetFullUserRequest(id=participant.id))
                participant = {
                    "id": participant.id,
                    "first_name": participant.first_name,
                    "last_name": participant.last_name,
                    "username": participant.username,
                    "about": full.full_user.about,
                    "collectDate": datetime.now().strftime('%Y-%m-%d %H:%M:%S%z'),
                    "role": self.get_member_type(member=participant)
                }
                self.save_to_csv(part=participant,columns=columns,csvfilename=entity.username)


  async def can_read_members(self,client,chat):
      
      admin=[]
      all=[]

      # Get all admins
      async for user in client.iter_participants(chat,filter=ChannelParticipantsAdmins):
              admin.append(user)

      # Get all users
      async for user in client.iter_participants(chat):
              all.append(user)

      # Compare the length of admin and user and admin arrays
      if len(admin) == len(all) :
            return "Private"
      else:
            return "Public"
      
      
  async def get_group_participants(self,url):

        # connect to telegram
        client = await Participants.get_telegram_client("1BJWap1wBu6NWVWV9ByYBIP0jQDTQyahJnyhwjjwj28MNdbDdsK8wHjAqg7CgQksX4TsJAIh6IZ0fB7-LOfRXzQHYiVbmE0tRbFyWQCKLYzByzDnTZ5X0YpIPmqI7YluElfzdOpuvP9j846kfhlmcr4UAxgu_pradhEuvRj-d2s36bAoFSfCDgHeZ5Rfzb5DbnqnFk8kFJXNg2I6hPTrIEvq6o9tvKKdwCuFFwibNekJ5S6Vct9S2d6TFVeIJUYUPQduhaoI1tSXh6FfBFdEBfZILLZHpVQ0T_DOg_WOOuNi-_XmfZyiGJLnsHhLrunNBy20urEUX57eVfaM3m7B6LI5ZHAOiY7s=")
        await client.connect()

        # extract username group from url
        spliturl = url.split("/")[-1]
        username = spliturl[2:]
        print(username)

        # recup chat entity from username 
        chat = await client.get_entity(username)

        #check group type
        grouptype=await Participants.can_read_members(self,client=client,chat=chat)

        print(grouptype)

        if grouptype == "Public":
            await self.process_users(client, chat)
        else :
            await self.process_joining_messages(client, chat)

participants =Participants()
asyncio.run(participants.get_group_participants("https://web.telegram.org/k/#@supermooncamp"))