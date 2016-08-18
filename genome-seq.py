import os
import time
from slackclient import SlackClient
import dotenv, twobitreader

#dotenv_path = '.env'
dotenv.load()

# starterbot's ID as an environment variable

BOT_ID = os.environ.get("BOT_ID")

# constants
AT_BOT = "<@" + BOT_ID + ">:"
#EXAMPLE_COMMAND = "do"

# instantiate Slack & Twilio clients
slack_client = SlackClient(os.environ.get('SLACK_BOT_TOKEN'))

def get_seq(tbFile, chrom, st, end):
    genome = twobitreader.TwoBitFile(tbFile)['chr' + chrom]
    return genome[st-1:end]

def handle_command(command, channel):
    """
        Receives commands directed at the bot and determines if they
        are valid commands. If so, then acts on the commands. If not,
        returns back what it needs for clarification.
    """
    #if command.startswith(EXAMPLE_COMMAND):
    genome, chrom, st, end = command.split(':')
    st, end = int(st), int(end)
    tbFile = 'data/%s.2bit' % (genome, )
    seq = get_seq(tbFile, chrom, st, end)
    slack_client.api_call("chat.postMessage", channel=channel,
                          text=seq, as_user=True)
    r = slack_client.api_call("files.upload", channels=channel,
                              content="ho ho ho", filename="test3.txt",
                              as_user=True)
    print('uploaded file')
    print(r)

def parse_slack_output(slack_rtm_output):
    """
        The Slack Real Time Messaging API is an events firehose.
        this parsing function returns None unless a message is
        directed at the Bot, based on its ID.
    """
    output_list = slack_rtm_output
    if output_list and len(output_list) > 0:
        for output in output_list:
            if output and 'text' in output and AT_BOT in output['text']:
                # return text after the @ mention, whitespace removed
                return output['text'].split(AT_BOT)[1].strip().lower(), \
                       output['channel']
    return None, None

if __name__ == "__main__":
    READ_WEBSOCKET_DELAY = 1 # 1 second delay between reading from firehose
    if slack_client.rtm_connect():
        print("StarterBot connected and running!")
        #mm9TwoBitFile = 'data/mm9.2bit'
        while True:
            command, channel = parse_slack_output(slack_client.rtm_read())
            if command and channel:
                handle_command(command, channel)
            time.sleep(READ_WEBSOCKET_DELAY)
    else:
        print("Connection failed. Invalid Slack token or bot ID?")
