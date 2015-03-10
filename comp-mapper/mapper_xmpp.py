import datetime, dateutil.parser, getopt, signal, sys, threading, time, traceback
import bson.son as son, json, pymongo, sleekxmpp

class MapperXMPP(sleekxmpp.ClientXMPP):

  '''
  CONSTANTS
  '''
  AUTOCLOSE = False
  DOMAIN = 'localhost'
  MUC_JID = 'conference.localhost'
  
  rooms = {}

  def __init__(self, jid, pwd, mongo_uri):
    self.JID = jid + '/console'
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
    self.add_event_handler('got_offline', self.got_offline)
    # stream_pause also has some of the got_offline business logic
    # actually got_offline listener should force an stream_pause
    
    jid_tuple = jid.split("@")
    self.DOMAIN = jid_tuple[1]
    self.MUC_JID = 'conference.' + self.DOMAIN

    self.local_sources = {}
    self.local_groups = {}

    # self.delta_over_timer = 60 * 30 # 30 minutes
    self.delta_over_timer = 60 * 1 # 30 minutes
    self.over_timer = {}
    
    try:
      self.mongo = pymongo.MongoClient(mongo_uri)
      self.db = self.mongo['lprm']

      self.sources = self.db['sources']
      self.sources.ensure_index([('jid', pymongo.ASCENDING)], unique=True)

      self.streams = self.db['streams']
      self.streams.ensure_index([('stream_id', pymongo.ASCENDING)], unique=True)
      self.streams.ensure_index([('latlng', pymongo.GEO2D)])

      self.groups = self.db['groups']
      self.groups.ensure_index([('group_jid', pymongo.TEXT)], unique=True)
      self.groups.ensure_index([('centroid', pymongo.GEO2D)])
	  
      self.pings = self.db['pings']

    except:
      traceback.print_exc()
      sys.exit(0)

  def handle_start(self, event):
    print "connected"
    self.send_presence(pstatus="")
    self.get_roster()
  
  def handle_message(self, message):
    # print message
    delay = message["delay"].get_stamp()
    if delay is None: # process only if it isn't an offline message
      try:
        jbody = json.loads(message["body"])
        func = getattr(self, "hnd_" + jbody["func"]) # gets reference for the application function
        args = jbody["args"] # application arguments
        args['from'] = str(message['from'])
        args['jid'] = args['from'].split('@')[0]
        args['to'] = str(message['to'])
        func(args)
      except:
        traceback.print_exc()

  ##################################################

  def got_offline(self, presence):
    try:
      jid = str(presence['from']).split('@')[0]
      stream = self.streams.find_one({'jid':jid, 'status': {'$ne':'over'}})
      
      if stream is not None and stream['status'] == 'streaming':
        self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'status':'paused'}})
        print 'got_offline stream_pause'
        
        if self.AUTOCLOSE and not self.over_timer.has_key(stream['jid']):
          self.over_timer[jid] = threading.Timer(self.delta_over_timer, self.autoclose, [jid], {})
          self.over_timer[jid].start()
        
    except:
      traceback.print_exc()

  ##################################################

  def autoclose(self, stream_id):
    try:
      stream = self.streams.find_one({'jid':jid, 'status': {'$ne':'over'}})

      if stream is not None and stream['status'] == 'paused':
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
      
      source = self.sources.find_one({'jid':args['jid']}, {'_id': 0})        
      if source is None:
        source = {
          'jid': args['jid'],
          'pwd': None,
          'twitter_id': None,
          'twitcasting_id': None,
          'livestream_id': None,
          'ustream_id': None,
          'bambuser_id': None,
          'current_stream': None,
          'streams': []
        }
        
        print "creating source " + args['jid']
        self.sources.insert(source, manipulate=False)
        roster_entry = self.client_roster[args['from']]
        if not roster_entry['to']:
          self.make_presence(pto=args['from'], ptype='subscribe').send()
          self.get_roster()
        print args['jid'] , " created"
      msg['args']['source'] = source
        
      if source['current_stream'] is not None:
        stream = self.streams.find_one({'stream_id': source['current_stream'], 'status': {'$ne':'over'}}, {'_id': 0})
        
        if stream is not None:
          msg['args']['stream'] = stream
      
      print msg
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
    except Exception as e:
      # print sys.exc_info()
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
  
  ##################################################
 
  def hnd_stream_init(self, args):
    print "stream_init"
    
    try:
      if self.streams.find_one({'stream_id':args['stream_id']}) is not None:
        raise Exception('there is a stream with the same stream_id')
   
      if self.sources.find_one({'current_stream':args['stream_id']}) is not None:
        raise Exception('source already has an active stream')
    
      stamp = datetime.datetime.now().isoformat()
      
      stream_data = {
        'stream_id': args['stream_id'],
        'jid': args['jid'],
        'group_jid': args['group_jid'],
        'status': 'streaming',
        'device_status': 'ok',
        'general_status': 'ok',
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
      self.streams.insert(stream_data, manipulate=False)
      print args['stream_id'] , " initiated"
      
      print "updating source " + args['jid']
      self.sources.update({'jid':args['jid']}, {'$set':{'current_stream':args['stream_id']}})
      self.sources.update({'jid':args['jid']}, {'$addToSet':{'streams':args['stream_id']}})
      print args['jid'] , " updated"
 
      members = None
      group = self.groups.find_one({'group_jid':args['group_jid'], 'status':{'$ne':'over'}})
      if group is None:
        group_data = {
          'group_jid':args['group_jid'],
          'status':'active',
          'centroid':args['latlng'],
          'hashtags':args['hashtags'],
          'stamp':stamp,
          'init_stamp':stamp,
          'close_stamp':None,
          'members':[args['stream_id']],
          'banned':[]
        }
        print "creating group " + args['group_jid']
        self.groups.insert(group_data, manipulate=False)
        print args['group_jid'] , " created"
      else:
        # group_hashtags = group['hashtags'] + args['hashtags']
        self.groups.update({'group_jid':args['group_jid']}, {'$set':{'stamp':stamp}})
        self.groups.update({'group_jid':args['group_jid']}, {'$addToSet':{'members':args['stream_id']}}) # $push/$addToSet
        print args['group_jid'] , " updated"
        
        # group_members = self.streams.find({'group_jid':args['group_jid'], 'status':{'$ne':'over'}, 'stream_id':{'$ne':stream['stream_id']}})
        group_members = self.streams.find({'group_jid':args['group_jid'], 'status':{'$eq':'streaming'}})
        if group_members.count() == 0:
          raise Exception('group appears to be active but has no members, therefore it should be idle')
        members = []      
        for m in group_members:
          if m['stream_id'] == stream_data['stream_id']:
            continue
          member = {
            'jid': m['jid'],
            'stream_id': m['stream_id'],
            'status': m['status'],
            'device_status': m['device_status'],
            'general_status': m['general_status'],
            'positive_ratings': m['positive_ratings'],
            'negative_ratings': m['negative_ratings'],
            'latlng': m['latlng'],
            'hashtags': m['hashtags'],
            'stamp': m['stamp']
          }
          members.append(member)
        
      msg = {
        'func':'stream_init_reply',
        'args': {
          'stream': stream_data
        }
      }
      if members is not None:
        msg['args']['members'] = members
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
      
      t = threading.Thread(target=self.join_room, args=(stream_data['group_jid'],))
      t.start()
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()

  ##################################################

  def hnd_stream_pause(self, args):
    print "stream_pause"
    
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id']}, {'_id': 0})
      
      if stream['status'] == 'over':
        raise Exception('stream is over')
      if stream['status'] == 'paused':
        raise Exception('stream is already paused')
    
      self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'status':'paused'}})
      stream['status'] = 'paused'
      
      if self.AUTOCLOSE and not self.over_timer.has_key(stream['jid']):
        self.over_timer[stream['jid']] = threading.Timer(self.delta_over_timer, self.autoclose, [stream['jid']], {})
        self.over_timer[stream['jid']].start()
        print stream['jid'] , ' timer set'
      
      msg = {
        'func':'stream_pause_reply',
        'args': {
          'stream': stream
         }
      }
      if self.AUTOCLOSE:
        msg['args']['delta'] = self.delta_over_time
      
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()

  ##################################################

  def hnd_stream_resume(self, args):
    print "stream_resume"
    print args
    
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id']}, {'_id': 0})
      
      if stream['status'] == 'over':
        raise Exception('stream is over')
      if stream['status'] == 'streaming':
        raise Exception('stream is already streaming')
      
      self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'status':'streaming'}})
      stream['status'] = 'streaming'
      
      group_members = self.streams.find({'group_jid':stream['group_jid'], 'status':{'$eq':'streaming'}})
      if group_members.count() == 0:
        raise Exception('group appears to be active but has no members, therefore it should be idle')
      members = []
      for m in group_members:
        if m['stream_id'] == stream['stream_id']:
          continue
        member = {
          'jid': m['jid'],
          'stream_id': m['stream_id'],
          'status': m['status'],
          'device_status': m['device_status'],
          'general_status': m['general_status'],
          'positive_ratings': m['positive_ratings'],
          'negative_ratings': m['negative_ratings'],
          'latlng': m['latlng'],
          'hashtags': m['hashtags'],
          'stamp': m['stamp']
        }
        members.append(member)
      
      msg = {
        'func':'stream_resume_reply',
        'args': {
          'stream': stream,
          'members': members
        }
      }
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
      
      t = threading.Thread(target=self.join_room, args=(stream['group_jid'],))
      t.start()
      
      if self.AUTOCLOSE and self.over_timer.has_key(jid):
        self.over_timer[jid].cancel()
        self.over_timer.pop(jid, None) 
        print 'timer unset'
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()

  ##################################################

  def hnd_stream_close(self, args):
    print "stream_close"
    # leave current group
    
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id']})
      
      if stream['status'] == 'over':
        raise Exception('stream is already over')
      
      self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'status':'over'}})
      self.sources.update({'jid':args['jid']}, {'$set':{'current_stream':None}})
      
      # leaving old group
      self.groups.update({'group_jid':stream['group_jid']}, {'$pull':{'members':stream['stream_id']}})
      group = self.groups.find_one({'group_jid':stream['group_jid']})
      if len(group['members']) == 0:
        self.groups.update({'group_jid':stream['group_jid']}, {'$set':{'status':'idle'}})
      
      msg = {
        'func':'stream_close_reply',
        'args': { 
          'stream': None
        }
      }
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()

  ##################################################

  def hnd_group_join(self, args):
    print "group_join"
    # leave current group and join selected/existing group
    
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id'], 'status':{'$ne':'over'}}, {'_id': 0})
      
      if stream is None:
        raise Exception('stream is over or does not exist')
      if stream['group_jid'] == args['group_jid']:
        raise Exception('stream is already in that group')
      
      current = self.groups.find_one({'group_jid':stream['group_jid']})
      next = self.groups.find_one({'group_jid':args['group_jid'], 'status':{'$ne':'idle'}})
      if current is None:
        raise Exception('source group does not exists')
      if next is None:
        raise Exception('target group is idle or does not exist')
      
      # joining new group
      self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'group_jid':next['group_jid']}})
      stream['group_jid'] = next['group_jid']
      self.groups.update({'group_jid':next['group_jid']}, {'$addToSet':{'members':stream['stream_id']}})
      
      group_members = self.streams.find({'group_jid':stream['group_jid'], 'status':{'$eq':'streaming'}})
      if group_members.count() == 0:
        raise Exception('group appears to be active but has no members, therefore it should be idle')
      members = []
      for m in group_members:
        if m['stream_id'] == stream['stream_id']:
          continue
        member = {
          'jid': m['jid'],
          'stream_id': m['stream_id'],
          'status': m['status'],
          'device_status': m['device_status'],
          'general_status': m['general_status'],
          'positive_ratings': m['positive_ratings'],
          'negative_ratings': m['negative_ratings'],
          'latlng': m['latlng'],
          'hashtags': m['hashtags'],
          'stamp': m['stamp']
        }
        members.append(member)
      
      # leaving old group
      self.groups.update({'group_jid':current['group_jid']}, {'$pull':{'members':stream['stream_id']}})
      current['members'].remove(stream['stream_id'])
      if len(current['members']) == 0:
        self.groups.update({'group_jid':current['group_jid']}, {'$set':{'status':'idle'}})
      
      msg = {
        'func':'group_join_reply',
        'args': { 
          'stream': stream,
          'members': members
        }
      }
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
      
      t = threading.Thread(target=self.join_room, args=(stream['group_jid'],))
      t.start()
      
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()

  ##################################################

  def hnd_group_leave(self, args):
    print "group_leave"
    # leave current group and create group
    
    try:    
      stream = self.streams.find_one({'stream_id':args['stream_id'], 'status':{'$ne':'over'}}, {'_id': 0})
      if stream is None:
        raise Exception('stream is over or does not exist')

      current = self.groups.find_one({'group_jid':stream['group_jid'], 'status':{'$ne':'idle'}})
      next = self.groups.find_one({'group_jid':args['group_jid'], 'status':{'$ne':'idle'}})      
      if current is None:
        raise Exception('source group does not exist')
      if next is not None:
        raise Exception('target group already exist')
        
      self.streams.update({'stream_id':stream['stream_id']}, {'$set':{'group_jid':args['group_jid']}})
      stream['group_jid'] = args['group_jid']

      stamp = datetime.datetime.now().isoformat()      
      group_data = {
        'group_jid':args['group_jid'],
        'status':'active',
        'centroid':stream['latlng'],
        'hashtags':stream['hashtags'],
        'stamp':stamp,
        'init_stamp':stamp,
        'close_stamp':None,
        'members':[stream['stream_id']],
        'banned':[]
      }
      self.groups.insert(group_data, manipulate=False)

      self.groups.update({'group_jid':current['group_jid']}, {'$pull':{'members':stream['stream_id']}})
      current['members'].remove(stream['stream_id'])
      if len(current['members']) == 0:
        self.groups.update({'group_jid':current['group_jid']}, {'$set':{'status':'idle'}})
      
      msg = {
        'func':'group_leave_reply',
        'args': { 
          'stream': stream
        }
      }
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
      
      t = threading.Thread(target=self.join_room, args=(args['group_jid'],))
      t.start()
      
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()

  ##################################################

  def hnd_group_match(self, args):
    print "group_match"
    # find groups matching geolocation + hashtags
    
    try:

      # match latlng
      if args.has_key("group_jid"):
        match = self.groups.find( { 'status':'active', 'centroid': son.SON([('$near', args['latlng']), ('$maxDistance', float(args['radius']))]), 'group_jid': {'$ne':args['group_jid']} })
      else:
        match = self.groups.find( { 'status':'active', 'centroid': son.SON([('$near', args['latlng']), ('$maxDistance', float(args['radius']))]) })
      groups = []
      for g in match:
        d = {
          'group_jid': g['group_jid'],
          'hashtags': g['hashtags'],
          'centroid': g['centroid'],
          'num_members': len(g['members'])
        }
        groups.append(d)
      print groups
      
      # print groups
      # match hashtags
      msg = {
        'func':'group_match_reply',
        'args': {
          'matched_groups': groups
        }
      }
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
      
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
      
  ##################################################

  def hnd_group_fetch_members(self, args):
    print "group_fetch_members"
    # fetches information about a given group, specially its members
    
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id'], 'status':{'$ne':'over'}}, {'_id': 0})
      if stream is None:
        raise Exception('stream is over or does not exist')
    
      group = self.groups.find_one({'group_jid':args['group_jid'], 'status':{'$ne':'idle'}})
      if group is None:
        raise Exception('group is idle or does not exist')
      
      group_members = self.streams.find({'group_jid':args['group_jid'], 'status':{'$eq':'streaming'}})
      if group_members.count() == 0:
        raise Exception('group appears to be active but has no members, therefore it should be idle')
      members = []
      for m in group_members:
        if m['stream_id'] == stream['stream_id']:
          continue
        member = {
          'jid': m['jid'],
          'stream_id': m['stream_id'],
          'status': m['status'],
          'device_status': m['device_status'],
          'general_status': m['general_status'],
          'positive_ratings': m['positive_ratings'],
          'negative_ratings': m['negative_ratings'],
          'latlng': m['latlng'],
          'hashtags': m['hashtags'],
          'stamp': m['stamp']
        }
        members.append(member)
      
      msg = {
        'func':'group_fetch_members_reply',
        'args': {
          'stream': stream,
          'members': members
        }
      }
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
	  
  ##################################################

  def hnd_group_fetch_pings(self, args):
    print "group_fetch_pings"
    # fetches information about a given group, specially its members
    
    try:
      pings = self.pings.find({'group_jid':args['group_jid'], 'stamp':{'$gte':args['since']}}, {'_id': 0})
	  
      result = []
      for p in pings:
		result.append(p)
		
      msg = {
        'func':'group_fetch_pings_reply',
        'args': {
          'pings': result
        }
      }
	  
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()

  ##################################################

  def hnd_update_latlng(self, args):
    print "update_latlng"

    try:
      stream = self.streams.find_one({'stream_id':args['stream_id'], 'status':{'$ne':'over'}})
      if stream is None:
        raise Exception('stream is over or does not exist')
        
      # publish current geolocation + sensor data + battery level      
      stamp = datetime.datetime.now().isoformat()
      data = {
        'latlng': args['latlng'],
        'stamp': stamp
      }
      self.streams.update({'stream_id':stream['stream_id']}, {'$set': data})
      print "location updated"

      '''
      # calculate centroids
      members = self.streams.find({'group_jid':stream['group_jid'], 'status':{'$ne':'over'}})  
      sum_lat = 0
      sum_lng = 0
      for member in members:
        latlng = member['latlng']
        sum_lat += latlng[1]
        sum_lng += latlng[0]    
      lat = sum_lat/members.count()
      lng = sum_lng/members.count()
      self.groups.update({'group_jid':stream['group_jid']}, {'$set' : {'centroid' : [lng, lat]}})
      print "centroid calculated"
      '''

    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
      
  ##################################################

  def hnd_update_hashtags(self, args):
    print "update_hashtags"
    # publish hashtag update
    try:
      stream = self.streams.find_one({'stream_id':args['stream_id'], 'status':{'$ne':'over'}})
      if stream is None:
        raise Exception('stream is over or does not exist')

      stamp = datetime.datetime.now().isoformat()
      data = {
        'hashtags':args['hashtags'],
        'stamp':stamp
      }
      self.streams.update({'stream_id':stream['stream_id']}, {'$set':data})
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()

  ##################################################
  
  def hnd_update_twitcasting_id(self, args):
    print "update_source_twitcasting_id"
    try:
      self.sources.update({'jid':args['jid']}, {'$set': {'twitcasting_id':args['twitcasting_id']}})
    except Exception as e:
      traceback.print_exc()
      msg = {'func':'message_exception_reply', 'args':{'exception':e.args}}
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
      
  def hnd_ping_target(self, args):
    print "ping_target"
    try:
      group_jid = args['from'].split('@')[0]
      ping = {
        'group_jid':group_jid,
        'type':'TARGET',
        'stream_id':args['stream_id'],
        'latlng':args['target_latlng'],
        'details':args['details'],
        'author':args['author'],
        'stamp':args['stamp']
      }
      self.pings.insert(ping, manipulate=False)
    except Exception as e:
      traceback.print_exc()
	  
  def hnd_ping_assist(self, args):
    print "ping_assist"
    try:
      group_jid = args['from'].split('@')[0]
      ping = {
        'group_jid':group_jid,
        'type':'ASSIST',
        'stream_id':args['stream_id'],
        'latlng':args['assist_latlng'],
        'details':args['details'],
        'author':args['author'],
        'stamp':args['stamp']
      }
      self.pings.insert(ping, manipulate=False)
    except Exception as e:
      traceback.print_exc()
	  
  def hnd_ping_danger(self, args):
    print "ping_danger"
    try:
      group_jid = args['from'].split('@')[0]
      ping = {
        'group_jid':group_jid,
        'type':'DANGER',
        'stream_id':args['stream_id'],
        'latlng':args['danger_latlng'],
        'details':args['details'],
        'author':args['author'],
        'stamp':args['stamp']
      }
      self.pings.insert(ping, manipulate=False)
    except Exception as e:
      traceback.print_exc()
	  
  ##################################################
  ##################################################
  ##################################################
  '''
  MESSAGES
  '''
  ##################################################
  ##################################################
  ##################################################
  
  ##################################################
  ##################################################
  ##################################################
  '''
  ASYNC
  '''
  ##################################################
  ##################################################
  ##################################################
  
  def join_room(self, group_JID):
    if not self.rooms.has_key(group_JID):
      print group_JID , "@" , self.MUC_JID , "/mapper"
      pto_jid = str(group_JID+"@"+self.MUC_JID+"/mapper")
      self.make_presence(pto=pto_jid).send()
      
      self.rooms[group_JID] = True
  
  # def leave_room(group_JID):
  
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

    mongo_uri = "mongodb://localhost:27017/"
    
    '''
    RUN
    '''

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
  except Exception as e:
    traceback.print_exc()
    sys.exit(0)

if __name__ == '__main__':
  main(sys.argv[1:])
