import datetime, getopt, signal, sys, time, traceback
import json, sleekxmpp

class SourceXMPP(sleekxmpp.ClientXMPP):

  '''
  CONSTANTS
  '''
  MAPPER_JID = 'comp-mapper@localhost/console'
  MUC_JID = 'conference.localhost'

  def __init__(self, jid, pwd):
    sleekxmpp.ClientXMPP.__init__(self, jid, pwd)
    self.register_plugin('xep_0004') # Data forms
    self.register_plugin('xep_0030') # Service discovery
    self.register_plugin('xep_0045') # Multi-user chat
    self.register_plugin('xep_0060') # Publish subscribe
    self.register_plugin('xep_0199') # XMPP ping
    self.register_plugin('xep_0203') # Delayed delivery
    self.add_event_handler("session_start", self.handle_start)
    self.add_event_handler("message", self.handle_message)
    self.auto_authorize = True

    self.JID = jid
    self.pwd = pwd
    self.muc_nick = jid.split("@")[0]
    
    self.twitcasting_id = None
    
    self.stream_ID = None
    self.group_JID = None
    self.hashtags = None
    self.latlng = None
    self.media = None
    
    self.source = None
    self.stream = None
        
    self.stream_status_required = True  # should change back to True whenever the stream is paused (including when the entire app is paused - sent to background)
    
    print 'source ' , self.JID

  def handle_start(self, event):
    print "connected"
    self.get_roster()
    self.send_presence(pstatus="")

  def handle_message(self, message):
    # print message
    delay = message["delay"].get_stamp()
    if delay is None: # process only if it isn't an offline message
      try:
        jbody = json.loads(message["body"])
        func = getattr(self, "hnd_" + jbody["func"]) # gets reference for the application function
        args = jbody["args"] # application arguments
        args['from'] = str(message['from'])
        args['to'] = str(message['to'])
        func(args)
      except:
        traceback.print_exc()
    # self.make_message(mto=message["from"], mbody=message["body"]).send()

  def handle_func(self, str_func):
    arr_func = str_func.split(" ")
    func = getattr(self, arr_func[0])
    if len(arr_func) == 1:
      func()
    else:
      func(arr_func[1])
    
  def set_hashtags(self, hashtags):
    self.hashtags = hashtags
    print "set hashtags " + self.hashtags

  def print_hashtags(self):
    print self.hashtags

  def set_latlng(self, latlng):
    self.latlng = latlng
    print "set latlgn " + self.latlng

  def print_latlng(self):
    print self.latlng
    
  def set_media(self, media):
    self.media = media
    print "set media " + self.media

  def print_media(self):
    print self.media
    
  def set_twitcasting_id(self, twitcasting_id):
    self.twitcasting_id = twitcasting_id
    print "set twitcasting_id" + self.twitcasting_id

  def print_twitcasting_id(self):
    print self.twitcasting_id

  ##################################################
  ##################################################
  ##################################################
  '''
  REPLIES
  '''
  ##################################################
  ##################################################
  ##################################################
    
  def hnd_message_exception_reply(self, args):
    print 'bad message reply'

  ##################################################

  def hnd_stream_status_reply(self, args):
    print 'stream status reply'
    print args
    try:
      self.source = args['source']
      if self.twitcasting_id is None:
        self.twitcasting_id = args['twitcasting_id']
      if args.has_key('stream'):
        self.stream = args['stream']
      self.stream_status_required = False
    except:
      traceback.print_exc()

  ##################################################
 
  def hnd_group_match_reply(self, args):
    print 'group match reply'

  ##################################################
  ##################################################
  ##################################################
  '''
  MESSAGES
  '''
  ##################################################
  ##################################################
  ##################################################
  
  def stream_status(self):
    try:
      msg = {
        'func': 'stream_status',
        'args': {}
      }
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()
  
  ##################################################
  
  def stream_init(self, group_jid=None):
    print "initiating stream"
    
    try:
      if self.stream_status_required == True:
        raise Exception('stream_status message is required')
      if self.stream_ID is not None:
        raise Exception('there is already an active or pending stream')
      if self.media == None:
        raise Exception('current_media missing')
      if self.twitcasting_id == None:
        raise Exception('twitcasting_id missing')
      
      now = datetime.datetime.now()
      stamp = now.strftime('%Y%m%d%H%M%S')
      self.stream_ID = self.muc_nick + "__" + stamp
    
      if group_jid is None:
        self.group_jid = self.stream_ID + "__" + stamp + "@" + self.MUC_JID
      else:
        self.group_jid = group_jid

      msg = {
        'func':'stream_init',
        'args': {
          'stream_id': self.stream_ID,
          'group_jid': self.group_jid,
          'hashtags': self.hashtags, # self.hashtags.replace("#",";")
          'latlng': self.latlng,
          'media': self.media,
          'twitcasting_id': self.twitcasting_id,
          'stamp': stamp
        }
      }
      
      pto_jid = str(self.group_jid+"/"+self.muc_nick)
      self.make_presence(pto=pto_jid).send()
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
      print "stream_initiated"
      
    except:
      traceback.print_exc()

  ##################################################

  def stream_pause(self):
    try:
      if self.stream is None:
        raise Exception('there is no active stream')
    
      msg = {
        'func':'stream_pause',
        'args': {
          'stream_id':self.stream['stream_id']
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
      self.stream_status_required = True
    except:
      traceback.print_exc()

  ##################################################

  def stream_resume(self):
    try:
      if self.stream_status_required == True:
        raise Exception('stream_status message is required')
      if self.stream is None:
        raise Exception('there is no pending stream')
      
      msg = {
        'func':'stream_resume',
        'args': {
          'stream_id':self.stream['stream_id']
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()

  ##################################################

  def stream_close(self):
    # leave current group
    
    try:
      if self.stream_ID is None:
        raise Exception('there is neither an active or pending stream')
    
      msg = {
        'func':'stream_close',
        'args': {
          'stream_id':self.stream_ID
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
      
      self.stream_ID = None
      self.group_jid = None
      self.current_latlng = None
      self.hashtags = None
      self.stream_status_required = True
    except:
      traceback.print_exc()
      
  ##################################################

  def group_join(self, group_id):
    # leave current group and join selected/existing group
    
    try:
      if group_id == self.group_id:
        raise Exception('stream is already in that group')
    
      msg = {
        'func':'group_join',
        'args': {
          'stream_id':self.stream_ID,
          'group_jid':group_id
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()

  ##################################################

  def group_leave(self):
    # leave current group and create group
    
    try:
      now = datetime.datetime.now()
      stamp = now.strftime('%Y%m%d%H%M%S')
    
      self.group_jid = self.stream_ID + '_' + stamp + '@' + self.MUC_JID
    
      msg = {
        'func':'group_leave',
        'args': {
          'stream_id':self.stream_ID,
          'group_jid':self.group_jid
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()

  ##################################################

  def group_match(self):
    # find groups matching geolocation + hashtags

    try:
      msg = {
        'func':'group_match',
        'args': {
          'hashtags': self.hashtags,
          'latlng': self.latlng
        }
      }
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()

  ##################################################

  def update_stream_latlng(self, latlng):
    # publish current geolocation + sensor data + battery level
    
    try:
      msg = {
        'func':'update_stream_latlng',
        'args': {
          'latlng': latlng
        }
      }
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
      print "latlng sent"
    except:
      traceback.print_exc()

  ##################################################

  def update_stream_hashtags(self, hashtags):
    # publish hashtag update

    try:
      msg = {
        'func':'update_stream_hashtags',
        'args': {
          'hashtags': hashtags
        }
      }
      self.make_message(mto=self.MAPPER_JID)
      print "hashtags sent"
    except:
      traceback.print_exc()

'''
CONSOLE
'''

def signal_handler(signal, frame):
  client.disconnect(wait=True)
  sys.exit(0)

client = None

def main(argv):
  signal.signal(signal.SIGINT, signal_handler)
  
  global client
  
  try:
  
    '''
    CONSOLE ARGS
    '''
    opts, args = getopt.getopt(argv, "j:p:h:", ['jid=', 'pwd=', 'hts='])
    if len(opts) < 2 :
      raise Exception("missing arguments")
    
    for opt, arg in opts:
      if opt in ('-j', '--jid'):
        jid = arg + "/lprm"
      if opt in ('-p', '--pwd'):
        pwd = arg

    '''
    RUN
    '''
    
    client = SourceXMPP(jid, pwd)
    client.connect()
    client.process()
    time.sleep(1)
     
    while True:
      try:
        msg_in = raw_input("> ")
        client.handle_func(msg_in)
        pass
      except KeyboardInterrupt, EOFError:
        client.disconnect(wait=True)
        break
    
  except Exception as e:
    traceback.print_exc()
    sys.exit(0)

if __name__ == '__main__':
  main(sys.argv[1:])
