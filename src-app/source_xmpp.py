import datetime, getopt, signal, sys, time, traceback
import json, sleekxmpp

class SourceXMPP(sleekxmpp.ClientXMPP):

  '''
  CONSTANTS
  '''
  DOMAIN = 'localhost'
  MAPPER_JID = 'mapper@localhost/console'
  MUC_JID = 'conference.localhost'

  def __init__(self, jid, pwd):
    self.JID = jid + '/device'
    self.pwd = pwd  
    sleekxmpp.ClientXMPP.__init__(self, self.JID, self.pwd)
    self.register_plugin('xep_0004') # Data forms
    self.register_plugin('xep_0030') # Service discovery
    self.register_plugin('xep_0045') # Multi-user chat
    self.register_plugin('xep_0060') # Publish subscribe
    self.register_plugin('xep_0199') # XMPP ping
    self.register_plugin('xep_0203') # Delayed delivery
    self.add_event_handler("session_start", self.handle_start)
    self.add_event_handler("message", self.handle_message)
    # self.add_event_handler("groupchat_message", self.handle_muc_message)
    self.auto_authorize = True
    
    jid_tuple = jid.split("@")
    self.muc_nick = jid_tuple[0]
    self.DOMAIN = jid_tuple[1]
    self.MUC_JID = 'conference.' + self.DOMAIN
    self.MAPPER_JID = 'mapper@' + self.DOMAIN + '/console'
    
    print self.JID
    print self.DOMAIN
    print self.MUC_JID
    print self.MAPPER_JID
    
    self.twitcasting_id = None
    
    self.stream_ID = None
    self.group_JID = None
    self.hashtags = None
    self.latlng = None
    self.media = None
    self.device_status = None
    self.general_status = None
    
    self.delta_over_time = None
    self.matched_groups = None
    self.members = None
    
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
    
  def handle_muc_message(self, message):
    print 'muc_message handler'
    print message

  def handle_func(self, str_func):
    try:
      arr_func = str_func.split(" ")
      func = getattr(self, arr_func[0])
      arr_func.pop(0)
      func(*arr_func)
    except:
      traceback.print_exc()

  def print_source(self):
    print self.source
    
  def print_stream(self):
    print self.stream

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
    print 'message_exception_reply'
    print args['exception']

  ##################################################

  def hnd_stream_status_reply(self, args):
    print 'stream status reply'
    print args
    try:
      self.source = args['source']
      if self.twitcasting_id is None:
        self.twitcasting_id = self.source['twitcasting_id']
      if args.has_key('stream'):
        self.stream = args['stream']
        print self.stream['stream_id'] , ' is ', self.stream['status']
        
        self.stream_ID = self.stream['stream_id']
        self.group_JID = self.stream['group_jid']
        self.media = self.stream['media']
        if self.hashtags is None:
          self.hashtags = self.stream['hashtags']
        if self.latlng is None:
          self.latlng = self.stream['latlng']
      else:
        print 'no active or pending stream'
      self.stream_status_required = False
    except:
      traceback.print_exc()

  ##################################################
  
  def hnd_stream_init_reply(self, args):
    print 'stream_init_reply'
    print args
    try:
      self.stream = args['stream']
      print 'streaming'
      
      pto_jid = str(self.group_JID+"@"+self.MUC_JID+"/"+self.muc_nick)
      self.make_presence(pto=pto_jid).send()
    except:
      traceback.print_exc()

  ##################################################
  
  def hnd_stream_pause_reply(self, args):
    print 'stream_pause_reply'
    print args
    try:
      self.stream = args['stream']
      print 'paused'
      if args.has_key('delta'):
        self.delta_over_time = args['delta']
        print 'automatically closing after ' , self.delta_over_time , ' seconds'
    except:
      traceback.print_exc()

  ##################################################
  
  def hnd_stream_resume_reply(self, args):
    print 'stream_resume_reply'
    print args
    try:
      self.stream = args['stream']
      print 'streaming'
    except:
      traceback.print_exc()

  ##################################################
      
  def hnd_stream_close_reply(self, args):
    print 'stream_close_reply'
    print args
    try:
      self.stream = args['stream']
      if self.stream is None:
        self.stream_ID = None
        self.group_JID = None
        self.latlng = None
        self.hashtags = None
        self.media = None
        self.delta_over_time = None
        self.stream_status_required = True
        print 'over'
    except:
      traceback.print_exc()
      
  ##################################################
   
  def hnd_group_join_reply(self, args):
    print 'group_join_reply'
    old_group = self.group_JID
    self.stream['group_jid'] = args['stream']['group_jid']
    self.group_JID = args['stream']['group_jid']
    print 'joined ' , self.group_JID
    
    pto_jid = str(self.group_JID+"@"+self.MUC_JID+"/"+self.muc_nick)
    self.make_presence(pto=pto_jid).send()
    self.make_presence(ptype='unavailable', pto=str(old_group+"@"+self.MUC_JID)).send()
    
  ##################################################
   
  def hnd_group_leave_reply(self, args):
    print 'group_leave_reply'
    old_group = self.group_JID
    old_group_jid = self.group_JID
    self.stream['group_jid'] = args['stream']['group_jid']
    self.group_JID = args['stream']['group_jid']
    print 'left ' , old_group_jid
    print 'joined ' , self.group_JID
    
    pto_jid = str(self.group_JID+"@"+self.MUC_JID+"/"+self.muc_nick)
    self.make_presence(pto=pto_jid).send()
    self.make_presence(ptype='unavailable', pto=str(old_group+"@"+self.MUC_JID)).send()

  ##################################################
   
  def hnd_group_match_reply(self, args):
    print 'group_match_reply'
    self.matched_groups = args['matched_groups']
    print 'correlated groups'
    print self.matched_groups
    
  ##################################################
   
  def hnd_group_fetch_members_reply(self, args):
    print 'group_fetch_members_reply'
    self.members = args['members']
    print 'members'
    print self.members
    
  ##################################################
   
  def hnd_ping_target(self, args):
    print 'ping_target'
    print args
    
  ##################################################
   
  def hnd_ping_danger(self, args):
    print 'ping_danger'
    print args
    
  ##################################################
   
  def hnd_ping_assist(self, args):
    print 'ping_assist'
    print args
    
  ##################################################
   
  def hnd_ping_onmyway(self, args):
    print 'ping_onmyway'
    print args

  ##################################################
  ##################################################
  ##################################################
  '''
  STREAM_LIFECYCLE MESSAGES
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
  
  def stream_init(self, media, group_jid=None):
    print "initiating stream"
    
    try:
      if self.stream_status_required == True:
        raise Exception('stream_status message is required')
      if self.stream is not None:
        raise Exception('there is already an active or pending stream')
      if self.twitcasting_id is None:
        raise Exception('twitcasting_id missing')
      if self.hashtags is None:
        raise Exception('hashtags missing')
      if self.latlng is None:
        raise Exception('latlng missing')
        
      self.media = media
      
      now = datetime.datetime.now()
      stamp = now.strftime('%Y%m%d%H%M%S')
      self.stream_ID = self.muc_nick + "__" + stamp
    
      if group_jid is None:
        self.group_JID = self.stream_ID + "__" + stamp
      else:
        self.group_JID = group_jid

      msg = {
        'func':'stream_init',
        'args': {
          'stream_id': self.stream_ID,
          'group_jid': self.group_JID,
          'hashtags': self.hashtags, # self.hashtags.replace("#",";")
          'latlng': self.latlng,
          'media': self.media,
          'stamp': stamp
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()      
      print "stream_initiated"
      
    except:
      traceback.print_exc()

  ##################################################

  def stream_pause(self):
    try:
      if self.stream is None:
        raise Exception('there is no active stream')
      if self.stream['status'] == 'paused':
        raise Exception('already paused')
    
      msg = {
        'func':'stream_pause',
        'args': {
          'stream_id':self.stream['stream_id']
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
      self.make_presence(ptype='unavailable', pto=(self.group_JID+"@"+self.MUC_JID)).send()
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
      if self.stream['status'] == 'streaming':
        raise Exception('already streaming')
      
      msg = {
        'func':'stream_resume',
        'args': {
          'stream_id':self.stream['stream_id']
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
      
      pto_jid = str(self.group_JID+"@"+self.MUC_JID+"/"+self.muc_nick)
      self.make_presence(pto=pto_jid).send()
    except:
      traceback.print_exc()

  ##################################################

  def stream_close(self):
    # leave current group
    
    try:
      if self.stream is None:
        raise Exception('there is neither an active or pending stream')
    
      msg = {
        'func':'stream_close',
        'args': {
          'stream_id':self.stream['stream_id']
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
      self.make_presence(ptype='unavailable', pto=(self.group_JID+"@"+self.MUC_JID)).send()
      self.stream_status_required = True
    except:
      traceback.print_exc()
      
  ##################################################
  ##################################################
  ##################################################
  '''
  GROUP_LIFECYCLE MESSAGES
  '''
  ##################################################
  ##################################################
  ##################################################

  def group_join(self, group_JID):
    # leave current group and join selected/existing group
    
    try:
      if self.stream is None:
        raise Exception('there is no active stream')
      if group_JID == self.stream['group_jid']:
        raise Exception('stream is already in that group')
    
      msg = {
        'func':'group_join',
        'args': {
          'stream_id':self.stream['stream_id'],
          'group_jid':group_JID
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()

  ##################################################

  def group_leave(self):
    # leave current group and create group
    
    try:
      if self.stream is None:
        raise Exception('there is no active stream')
    
      now = datetime.datetime.now()
      stamp = now.strftime('%Y%m%d%H%M%S')
    
      group_jid = self.stream['stream_id'] + '_' + stamp
    
      msg = {
        'func':'group_leave',
        'args': {
          'stream_id':self.stream['stream_id'],
          'group_jid':group_jid
        }
      }
      
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()

  ##################################################

  def group_match(self, radius):
    # find groups matching geolocation + hashtags
    
    try:
      if self.hashtags is None:
        raise Exception('hashtags missing')
      if self.latlng is None:
        raise Exception('latlng missing')
    
      msg = {
        'func':'group_match',
        'args': {
          'hashtags': self.hashtags,
          'latlng': self.latlng,
          'radius': radius
        }
      }
	  if self.stream is not None:
	    msg['group_jid'] = self.stream['group_jid']
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()
      
  ##################################################

  def group_fetch_members(self):
    # fetches information about a given group, specially its members

    try:
      if self.stream is None:
        raise Exception('there is no active stream')
    
      msg = {
        'func':'group_fetch_members',
        'args': {
          'stream_id':self.stream['stream_id'],
          'group_jid': self.stream['group_jid']
        }
      }
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()

  ##################################################
  ##################################################
  ##################################################
  '''
  STREAM_UPDATE MESSAGES
  '''
  ##################################################
  ##################################################
  ##################################################

  def update_latlng(self, latlng):
    # publish current geolocation + sensor data + battery level
    
    latlng_array = latlng.split(',')
    lat = float(latlng_array[0])
    lng = float(latlng_array[1])
    self.latlng = [lng, lat]
    
    if self.stream is None:
      print 'latlng locally updated'
    else:
      try:
        msg = {
          'func':'update_latlng',
          'args': {
            'stream_id': self.stream['stream_id'],
            'latlng': self.latlng
          }
        }
        self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
        self.stream['latlng'] = latlng
        print 'latlng remotely updated'
      except:
        traceback.print_exc()

  ##################################################

  def update_hashtags(self, hashtags):
    # publish hashtag update

    self.hashtags = hashtags
    
    if self.stream is None:
      print 'hashtags locally updated'
    else:
      try:
        msg = {
          'func':'update_hashtags',
          'args': {
            'stream_id':self.stream['stream_id'],
            'hashtags': hashtags
          }
        }
        self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
        self.stream['hashtags'] = hashtags
        print 'hashtags remotely updated'
      except:
        traceback.print_exc()
        
  ##################################################

  def update_device_status(self, status, details = None):
    # publish device_status update

    try:
      if self.stream is None:
        raise Exception('there is no active stream')
    
      msg = {
        'func':'update_device_status',
        'args': {
          'stream_id':self.stream['stream_id'],
          'status': status,
          'details': None
        }
      }
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()
      
  ##################################################

  def update_general_status(self, status, details = None):
    # publish device_status update

    try:
      if self.stream is None:
        raise Exception('there is no active stream')
    
      msg = {
        'func':'update_general_status',
        'args': {
          'stream_id':self.stream['stream_id'],
          'status': status,
          'details': None
        }
      }
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()     
      
  ##################################################

  def update_twitcasting_id(self, twitcasting_id):
    self.twitcasting_id = twitcasting_id

    try:
      msg = {
        'func':'update_twitcasting_id',
        'args': {
          'twitcasting_id': twitcasting_id
        }
      }
      self.make_message(mto=self.MAPPER_JID, mbody=json.dumps(msg)).send()
      print "twitcasting_id remotely updated"
    except:
      traceback.print_exc()
      
  ##################################################
  ##################################################
  ##################################################
  '''
  PING MESSAGES
  '''
  ##################################################
  ##################################################
  ##################################################
  
  def ping_target(self, latlng, details = None):
    # publish current geolocation + sensor data + battery level

    try:
      if self.stream is None:
        raise Exception('there is no active stream')
        
      latlng_array = latlng.split(',')
      lat = float(latlng_array[0])
      lng = float(latlng_array[1])
      target_latlng = [lng, lat]

      msg = {
        'func':'ping_target',
        'args': {
          'stream_id': self.stream['stream_id'],
          'target_latlng': target_latlng,
          'details': details
        }
      }
      group_jid = self.stream['group_jid'] + '@' + self.MUC_JID
      print 'ping_target'
      print group_jid
      print msg
      self.make_message(mto=group_jid, mbody=json.dumps(msg), mtype='groupchat').send()
    except:
      traceback.print_exc()
      
  ##################################################
  
  def ping_danger(self, latlng, details = None):
    # publish current geolocation + sensor data + battery level

    try:
      if self.stream is None:
        raise Exception('there is no active stream')
        
      latlng_array = latlng.split(',')
      lat = float(latlng_array[0])
      lng = float(latlng_array[1])
      danger_latlng = [lng, lat]

      msg = {
        'func':'ping_danger',
        'args': {
          'stream_id': self.stream['stream_id'],
          'danger_latlng': danger_latlng,
          'details': details
        }
      }
      group_jid = self.stream['group_jid'] + '@' + self.MUC_JID
      self.make_message(mto=group_jid, mbody=json.dumps(msg), mtype='groupchat').send()
    except:
      traceback.print_exc()
      
  ##################################################
  
  def ping_assist(self, latlng, details = None):
    # publish current geolocation + sensor data + battery level

    try:
      if self.stream is None:
        raise Exception('there is no active stream')
        
      latlng_array = latlng.split(',')
      lat = float(latlng_array[0])
      lng = float(latlng_array[1])
      assist_latlng = [lng, lat]

      msg = {
        'func':'ping_assist',
        'args': {
          'stream_id': self.stream['stream_id'],
          'assist_latlng': assist_latlng,
          'details': details
        }
      }
      group_jid = self.stream['group_jid'] + '@' + self.MUC_JID
      self.make_message(mto=group_jid, mbody=json.dumps(msg), mtype='groupchat').send()
    except:
      traceback.print_exc()
      
  ##################################################
  
  def ping_onmyway(self, latlng, details = None):
    # publish current geolocation + sensor data + battery level

    try:
      if self.stream is None:
        raise Exception('there is no active stream')
        
      latlng_array = latlng.split(',')
      lat = float(latlng_array[0])
      lng = float(latlng_array[1])
      onmyway_latlng = [lng, lat]

      msg = {
        'func':'ping_onmyway',
        'args': {
          'stream_id': self.stream['stream_id'],
          'onmyway_latlng': onmyway_latlng,
          'details': details
        }
      }
      group_jid = self.stream['group_jid'] + '@' + self.MUC_JID
      self.make_message(mto=group_jid, mbody=json.dumps(msg), mtype='groupchat').send()
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
        jid = arg
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
