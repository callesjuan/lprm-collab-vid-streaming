import datetime, dateutil.parser, signal, sys, threading, time, traceback
import bson.son as son, json, pymongo, sleekxmpp

class MapperXMPP(sleekxmpp.ClientXMPP):

  '''
  CONSTANTS
  '''
  MUC_JID = 'conference.localhost'

  def __init__(self, jid, password, mongo_uri):
    sleekxmpp.ClientXMPP.__init__(self, jid, password)
    self.register_plugin('xep_0004') # Data forms
    self.register_plugin('xep_0030') # Service discovery
    self.register_plugin('xep_0045') # Multi-user chat
    self.register_plugin('xep_0060') # Publish subscribe
    self.register_plugin('xep_0199') # XMPP ping
    self.register_plugin('xep_0203') # Delayed delivery
    self.add_event_handler("session_start", self.handle_start)
    self.add_event_handler("message", self.handle_message)
    # self.add_event_handler('got_offline', self.got_offline)
    # self.add_event_handler('got_online', self.got_online)

    self.local_sources = {}
    self.local_groups = {}

    self.delta_over_timer = 60 * 30 # 30 minutes
    self.over_timer = {}
    
    try:
      self.mongo = pymongo.MongoClient(mongo_uri)
      self.db = self.mongo['lprm']

      self.sources = self.db['sources']
      self.sources.ensure_index([('jid', pymongo.ASCENDING)], unique=True)

      self.streams = self.db['streams']
      self.streams.ensure_index([('stream_id', pymongo.ASCENDING)], unique=True)
      self.streams.ensure_index([('current_latlng', pymongo.GEO2D)])

      self.groups = self.db['groups']
      self.groups.ensure_index([('group_jid', pymongo.TEXT)], unique=True)
      self.groups.ensure_index([('current_centroid', pymongo.GEO2D)])

    except:
      traceback.print_exc()
      sys.exit(0)

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

  ##################################################

  def got_offline(self, presence):
    try:
      jid = str(presence['from'])
      stream = self.streams.find_one({'jid':jid, 'status': {'$ne':'over'}})
      
      if stream is not None:
        if stream['status'] == 'streaming': 
          self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'status':'paused'}})
          
        self.over_timer[jid] = threading.Timer(self.delta_over_timer, self.autoclose, [jid], {})
        self.over_timer[jid].start()
        
    except:
      traceback.print_exc()

  ##################################################
  
  def got_online(self, presence):
    try:
      jid = str(presence['from'])
      
      if self.over_timer.has_key(jid):
        self.over_timer[jid].cancel()
        self.over_timer.pop(jid, None)
        
        print 'timer unset'
            
    except:
      traceback.print_exc()

  ##################################################

  def autoclose(self, stream_id):
    try:
      stream = self.streams.find_one({'jid':jid, 'status': {'$ne':'over'}})

      if stream is not None:
        self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'status':'over'}})
        
        print 'stream closed'
    except:
      traceback.print_exc()

  ##################################################
  ##################################################
  ##################################################
  '''
  LISTENERS 
  '''
  ##################################################  
  ##################################################  
  ##################################################
  
  def hnd_stream_status(self, args):
    print "stream_status"
    
    try:
      msg = {
        'func':'stream_status_reply',
        'args': {}
      }
      
      source = self.sources.find_one({'jid':args['from']})        
      if source is None:
        source = {
          'jid': args['from'],
          'pwd': None,
          'twitter_id': None,
          'twitcasting_id': None,
          'livestream_id': None,
          'ustream_id': None,
          'bambuser_id': None,
          'current_stream': None,
          'streams': []
        }
        
        print "creating source " + args['from']
        self.sources.insert(source)
        print args['from'] , " created"
      msg['args']['source'] = source
        
      if source['current_stream'] is not None:
        stream = self.streams.find_one({'stream_id': source['current_stream'], 'status': {'$ne':'over'}})
        
        if stream is not None:
          msg['args']['stream'] = stream
   
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
    except:
      # print sys.exc_info()
      traceback.print_exc()
  
  ##################################################
 
  def hnd_stream_init(self, args):
    print "stream_init"
    
    try:
      stamp = datetime.datetime.now().isoformat()

      if self.streams.find_one({'stream_id':args['stream_id']}) is None:
        stream_data = {
          'stream_id': args['stream_id'],
          'jid': args['from'],
          'group_jid': args['group_jid'],
          'status': 'streaming',
          'resources_status': 'ok',
          'physical_status': 'ok',
          'positive_ratings': 0,
          'negative_ratings': 0,
          'latlng': args['latlng'],
          'hashtags': args['hashtags'],
          'init_stamp': args['stamp'],
          'media': args['media'],
          'stamp': stamp,
          'init_stamp': stamp,
          'close_stamp': None
        }
        print "initializing stream "  + args['stream_id']
        self.streams.insert(stream_data)
        self.make_presence(pto=source, ptype='subscribe').send()
        print args['stream_id'] , " initiated"
        
        print "updating source " + args['from']
        self.sources.update({'jid':args['from']}, {'$set':{'current_stream':args['stream_id'], 'twitcasting_id':args['twitcasting_id']}})
        self.sources.update({'jid':args['from']}, {'$addToSet':{'streams':args['stream_id']}})
        print args['from'] , " updated"
   
        group = self.groups.find_one({'group_jid':args['group_jid']})
        if group is None:
          group_data = {
            'group_jid':args['group_jid'],
            'status':'populated',
            'centroid':args['latlng'],
            'hashtags':args['hashtags'],
            'stamp':stamp,
            'init_stamp':stamp,
            'close_stamp':None,
            'members':[args['stream_id']],
            'banned':[]
          }
          print "creating group " + args['group_jid']
          self.groups.insert(group_data)
          print args['group_jid'] , " created"
        else:
          # group_hashtags = group['hashtags'] + args['hashtags']
          self.groups.update({'group_jid':args['group_jid']}, {'$set':{'stamp':stamp}})
          self.groups.update({'group_jid':args['group_jid']}, {'$addToSet':{'members':args['stream_id']}}) # $push/$addToSet
          print args['group_jid'] , " updated"
        
    except:
      traceback.print_exc()

  ##################################################

  def hnd_stream_pause(self, args):
    print "stream_pause"
    
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id']})
    
      self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'status':'paused'}})
      
      # if self.over_timer.has_key(stream['jid']):
      #     self.over_timer[stream['jid']].cancel()
      # self.over_timer[stream['jid']] = threading.Timer(self.delta_over_timer, self.autoclose, [stream['jid']], {})
      # self.over_timer[stream['jid']].start()
      # print stream['jid'] , ' timer set'
      
      msg = {
        'func':'stream_pause_reply',
        'args': {
          'delta': self.delta_over_timer
         }
      }
      
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
    
    except:
      traceback.print_exc()

  ##################################################

  def hnd_stream_resume(self, args):
    print "stream_resume"
    
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id']})
      
      self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'status':stream['last_status']}})
      
    except:
      traceback.print_exc()

  ##################################################

  def hnd_stream_close(self, args):
    print "stream_close"
    # leave current group
    
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id']})
      
      self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'status':'ended', 'last_status':stream['status']}})
      self.sources.update({'jid':args['from']}, {'$set':{'current_stream':None}})
      
    except:
      traceback.print_exc()

  ##################################################

  def hnd_group_join(self, args):
    print "group_join"
    # leave current group and join selected/existing group
    
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id']})
      
      if stream['group_jid'] == args['group_jid']:
        raise Exception('stream is already in that group')
      
      current = self.groups.find_one({'group_jid':stream['group_jid']})
      next = self.groups.find_one({'group_jid':args['stream_id']})
      
      if next is None:
        raise Exception('target group does not exists')
      
      # leaving old group
      self.groups.update({'group_jid':stream['group_jid']}, {'$pull':{'members':stream['stream_id']}})
      
      # joining new group
      self.groups.update({'group_jid':args['group_jid']}, {'$addToSet':{'members':stream['stream_id']}})
      self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'group_jid':args['group_jid']}})
      
    except:
      traceback.print_exc()

  ##################################################

  def hnd_group_leave(self, args):
    print "group_leave"
    # leave current group and create group
    
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id']})
      self.groups.update({'group_jid':stream['group_jid']}, {'$pull':{'members':stream['stream_id']}})
      
      current = self.groups.find_one({'group_jid':stream['group_jid']})      
      if len(current['members']) == 1:
        self.groups.update({'group_jid':stream['group_jid']}, {'$set':{'status':'empty'}})
        
      stamp = datetime.datetime.now().isoformat()
      
      group_data = {
        'group_jid':args['group_jid'],
        'members':[args['stream_id']], 
        'banned':[],
        'insert_stamp':stamp,
        'hashtags':stream['current_hashtags']
      }

      self.groups.insert(group_data)
      
    except:
      traceback.print_exc()

  ##################################################

  def hnd_group_match(self, args):
    print "group_match"
    # find groups matching geolocation + hashtags
    
    try:
      latlng = args['latlng'].split(',')
      lat = int(latlng[0])
      lng = int(latlng[1])

      # match latlng
      match = self.groups.find( { 'status':'active', 'latlng': son.SON([('$near', [lng, lat]), ('$maxDistance', 2)]) })
      # print match.count()
      groups = []
      for g in match:
        d = {
          'group_jid': g['group_jid'],
          'hashtags': g['hashtags']
        }
        groups.append(d)
      
      # print groups

      # match hashtags
      msg = {
        'func':'group_match_reply',
        'args': {
          'count': match.count(),
          'groups': groups
        }
      }

      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
    except:
      traceback.print_exc()

  ##################################################

  def hnd_update_latlng(self, args):
    print "update_latlng"

    try:
      # publish current geolocation + sensor data + battery level
      jid = args['from']
      latlng = args['latlng'].split(',')
      lat = float(latlng[0])
      lng = float(latlng[1])
      
      stamp = datetime.datetime.now().isoformat()

      data = {
        'latlng': [lng, lat],
        'update_stamp': stamp
      }
      self.streams.update({'jid':jid}, {'$set': data})
      print "location updated"

      # calculate centroids
      group = self.groups.find_one({'members' : {'$in' : [jid]}})
      # src = self.streams.find_one({'jid':jid})
      # group = self.groups.find_one({'group_jid':src['group']})
      members = self.streams.find({'group' : group['group_jid']})
  
      sum_lat = 0
      sum_lng = 0
      for member in members:
        latlng = member['latlng']
        sum_lat += latlng[1]
        sum_lng += latlng[0]
      
      lat = sum_lat/members.count()
      lng = sum_lng/members.count()
      self.groups.update({'group_jid':group['group_jid']}, {'$set' : {'latlng' : [lng, lat]}})
      print "centroid calculated"

    except:
      traceback.print_exc()
      
  ##################################################

  def hnd_update_hashtags(self, args):
    print "update_hashtags"
    # publish hashtag update

  ##################################################
  ##################################################
  ##################################################
  '''
  MESSAGES
  '''
  ##################################################
  ##################################################
  ##################################################
    
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

  jid = "comp-mapper@localhost/console"
  pwd = "123"
  mongo_uri = "mongodb://localhost:27017/"

  client = MapperXMPP(jid, pwd, mongo_uri)
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

if __name__ == '__main__':
  main(sys.argv[1:])
