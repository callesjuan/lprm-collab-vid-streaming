import datetime, dateutil.parser, logging, signal, sys, threading, time
import bson.son as son, json, matplotlib.pyplot as plt, pymongo, sleekxmpp

class MapperXMPP(sleekxmpp.ClientXMPP):

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
    self.add_event_handler('got_online', self.got_online)
    self.add_event_handler('got_offline', self.got_offline)

    self.local_sources = {}
    self.local_groups = {}

    self.delta_check_latlng_update = 10.0
    self.timer_check_latlng_update = {}
    
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
      print sys.exc_info()
      sys.exit(0)

  def handle_start(self, event):
    logging.info("connected")
    print "connected"
    self.get_roster()
    self.send_presence(pstatus="")
  
  def handle_message(self, message):
    logging.info(message)
    # print message
    delay = message["delay"].get_stamp()
    if delay is None: # process only if it isn't an offline message
      try:
        jbody = json.loads(message["body"])
        func = getattr(self, "hnd_" + jbody["func"]) # gets reference for the application function
        args = jbody["args"] # application arguments
        args['from'] = str(message['from'])
        args['to'] = str(message['to'])
        func(head, args)
      except:
        logging.warning("message error")
        print sys.exc_info()

  ##################################################

  def got_online(self, presence):
    self.streams.update({'jid':presence['from']}, {'$set':{'status':'on'}})

  ##################################################

  def got_offline(self, presence):
    self.streams.update({'jid':presence['from']}, {'$set':{'status':'off'}})

  ##################################################

  def check_latlng_update(self, stream_id):
    try:
      stream = self.streams.find_one({'stream_id': jid, 'status'})

      now = datetime.datetime.now()
      update = dateutil.parser.parse(stream['current_stamp'])

      time_lapsed = now - update
      # seconds_lapsed = time_lapsed.days * 24 * 60 * 60 + time_lapsed.seconds
      if time_lapsed.seconds > 60 or time_lapsed.days != 0:
        self.streams.update({'jid': jid}, {'$set':{'state':'off'}})
    except:
      print sys.exc_info()

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
    logging.info("stream_status")
    try:
      source = self.sources.find_one({'jid':args['jid']})
      stream_id = source['current_stream']
      
      msg = {
          'func':'stream_status_reply',
          'args': {}
        }

      if stream_id is not None:
        stream = self.streams.find_one({'stream_id': stream_id})
        msg['args']['stream'] = {
          'stream_id': stream['stream_id']
          'group_jid': stream['group_jid'],
          'current_latlng': stream['current_latlng'],
          'current_hashtags': stream['current_hashtags']
        }
      else:
        msg['args']['stream'] = None
   
      self.make_message(mto=args['from'], mbody=json.dumps(msg)).send()
    except:
      print sys.exc_info()
  
  ##################################################
 
  def hnd_stream_init(self, args):
    logging.info("stream_init")
    try:
      if args["group_jid"] is not None:
        source = args['from']
        group_jid = args['group_jid']
        hashtags = args['hashtags']
        
        stamp = datetime.datetime.now().isoformat()

        data = {
          'group_jid': group_jid,
          'hashtags': hashtags,
          'current_stamp': stamp
        }

        if self.streams.find_one({'jid':source}) is None:
          data['jid'] = source
          data['insert_stamp'] = stamp
          data['state'] = 'on'
          self.streams.insert(data)
          self.make_presence(pto=source, ptype='subscribe').send()
        else:
          self.streams.update({'jid':source}, {'$set':data})

        print source , " initiated"
     
        if self.groups.find_one({'group_jid':group}) is None:
          print "TRYING TO CREATE GROUP " + group + " AND SOURCE " + source
          self.groups.insert({'group_jid':group, 'members':[source], 'banned':[], 'insert_stamp':stamp, 'hashtags':hashtags})
        else:
          self.groups.update({'group_jid':group}, {'$addToSet':{'members':source}}) # $push/$addToSet

        print group , " created"

        if self.timer_check_latlng_update.has_key(source):
          self.timer_check_latlng_update[source].cancel()

        self.timer_check_status[source] = threading.Timer(self.delta_check_status, self.check_status, [source], {})
        self.timer_check_status[source].start()
        
        print 'timer set'
    except:
      logging.error("stream_init error")
      logging.error(sys.exc_info())
      print sys.exc_info()

  ##################################################

  def hnd_stream_pause(self, args):
    logging.info("stream_pause")

  ##################################################

  def hnd_stream_resume(self, args):
    logging.info("stream_resume")

  ##################################################

  def hnd_stream_close(self, args):
    logging.info("stream_close")
    # leave current group

  ##################################################

  def hnd_stream_exists(self, args):
    logging.info("stream_exists")

  ##################################################

  def hnd_group_join(self, args):
    logging.info("group_join")
    # leave current group and join selected/existing group

  ##################################################

  def hnd_group_leave(self, args):
    logging.info("group_leave")
    # leave current group and create group

  ##################################################

  def hnd_group_match(self, args):
    logging.info("group_match")
    # find groups matching geolocation + hashtags
    
    try:
      source = args['from']
      latlng = args['latlng'].split(',')
      lat = int(latlng[0])
      lng = int(latlng[1])
      hashtags = args['hashtags']

      # match latlng
      match = self.groups.find( {'latlng': son.SON([('$near', [lng, lat]), ('$maxDistance', 2)]) })
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

      self.make_message(mto=source, mbody=json.dumps(msg)).send()
    except:
      print sys.exc_info()

  ##################################################

  def hnd_update_latlng(self, args):
    logging.info("update_latlng")

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
      print sys.exc_info()
      
  ##################################################

  def hnd_update_hashtags(self, args):
    logging.info("update_hashtags")
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
  
  logging.basicConfig(filename='mapper_xmpp.log',level=logging.DEBUG)
  logging.info("mapper started")
  
  global client

  jid = "comp-mapper@localhost/console"
  pwd = "123"
  mongo_uri = "mongodb://localhost:27017/"

  client = MapperXMPP(jid, pwd, mongo_uri)
  client.connect()
  client.process()

  fig = plt.figure()
  plt.ion()
  plt.show()

  while True:
    time.sleep(5)
    
    plot_x = []
    plot_y = []

    sources = client.sources.find({'status':'on'})
    for source in sources:
      if source.has_key('latlng'):
        latlng = source['latlng']
        plot_x.append(latlng[0])
        plot_y.append(latlng[1])

    plt.clf()
    plt.axis([-20,20,-20,20])
    plt.plot(plot_x, plot_y, 'ro')
    plt.draw()
    pass

if __name__ == '__main__':
  main(sys.argv[1:])
