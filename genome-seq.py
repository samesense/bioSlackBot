import os
import time
from slackclient import SlackClient
import dotenv, twobitreader, tables

#dotenv_path = '.env'
dotenv.load()

# starterbot's ID as an environment variable

BOT_ID = os.environ.get("BOT_ID_BLOBEL")

# constants
AT_BOT = "<@" + BOT_ID + ">:"
#EXAMPLE_COMMAND = "do"

# instantiate Slack & Twilio clients
slack_client = SlackClient(os.environ.get('SLACK_BOT_TOKEN_BLOBEL'))

def loadCalls(table, chrom, st, end):
    """Load jc4 data.
       Don't use chr
    """
    header = ('pos', 'ref', 'calls', 'depth', 'altFrac', 'zygosity')
    resLs = [ '\t'.join([str(x['pos']),
                         x['ref'].decode('UTF-8'),
                         x['calls'].decode('UTF-8'),
                         str(x['depth']),
                         str(x['altFrac']),
                         x['zygosity'].decode('UTF-8'),
                         ]) for x in
              table.where('(chrom == b"%s") & (pos >= %d) & (pos <= %d)'
                          % (chrom, st, end)) ]
    return '\n'.join(['\t'.join(header)] + resLs)

def get_seq(tbFile, chrom, st, end):
    genome = twobitreader.TwoBitFile(tbFile)['chr' + chrom]
    return genome[st-1:end]

def handle_command(table, command, channel):
    """
        Receives commands directed at the bot and determines if they
        are valid commands. If so, then acts on the commands. If not,
        returns back what it needs for clarification.
    """
    print('yo')
    #if command.startswith(EXAMPLE_COMMAND):
    try:
        genome, chrom, st, end = command.split(':')
        if 'chr' in chrom or \
           not genome in ('jc4', 'mm9', 'hg19') or \
           ',' in command or '-' in command:
            slack_client.api_call("chat.postMessage", channel=channel,
                              text='Format query like jc4:10:3002950:3012990',
                              as_user=True)
            return
    except:
        slack_client.api_call("chat.postMessage", channel=channel,
                              text='Format query like jc4:10:3002950:3012990',
                              as_user=True)
        return
    st, end = int(st), int(end)
    tbFile = 'data/%s.2bit' % (genome, )
    print(end-st)
    
    seqLen = end-st
    # crashes @seqbot: jc4:10:3002950:301290940
    print(end-st, 'done')
    if seqLen <= 0:
        slack_client.api_call("chat.postMessage", channel=channel,
                              text='Enter larger region.', as_user=True)
    elif seqLen < 100:
        seq = get_seq(tbFile, chrom, st, end)
        slack_client.api_call("chat.postMessage", channel=channel,
                              text=seq, as_user=True)
    else:
        seq = get_seq(tbFile, chrom, st, st+100)
        slack_client.api_call("chat.postMessage", channel=channel,
                              text='Region is too large to print.\nUsing first 100 bases.\njc4 variants will span the whole region.', as_user=True)
        slack_client.api_call("chat.postMessage", channel=channel,
                              text=seq, as_user=True)
#    print('here')
    if genome == 'jc4':
        slack_client.api_call("chat.postMessage", channel=channel,
                              text='Looking up variants ...', as_user=True)
        calls = loadCalls(table, chrom, st, end)
        if calls:
            content = calls
        else:
            content = 'no variants'
        #print(content)
        r = slack_client.api_call("files.upload", channels=channel,
                                  content=content, filename="test%s.txt" % (str(st),),
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
    h5file = tables.open_file('/nas/is1/perry/projects/me/encode_genomes/data/vars/jc4.db', mode = "r")
    table = h5file.root.posCollection.posLs
    if slack_client.rtm_connect():
        print("StarterBot connected and running!")
        #mm9TwoBitFile = 'data/mm9.2bit'
        while True:
            command, channel = parse_slack_output(slack_client.rtm_read())
            print('ok', command, channel)
            if command and channel:
                handle_command(table, command, channel)
            time.sleep(READ_WEBSOCKET_DELAY)
    else:
        print("Connection failed. Invalid Slack token or bot ID?")

    h5file.close()
