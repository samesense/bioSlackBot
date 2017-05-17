import os
from slackclient import SlackClient
import dotenv

dotenv.load()

BOT_NAME = 'genome-seq'
BOT_NAME_BLOBEL = 'seqbot'

slack_client = SlackClient(os.environ.get('SLACK_BOT_TOKEN_BLOBEL'))

if __name__ == "__main__":
    api_call = slack_client.api_call("users.list")
    if api_call.get('ok'):
        # retrieve all users so we can find our bot
        users = api_call.get('members')
        for user in users:
            if 'name' in user and user.get('name') == BOT_NAME_BLOBEL:
                print("Bot ID for '" + user['name'] + "' is " + user.get('id'))
    else:
        print("could not find bot user with the name " + BOT_NAME)
