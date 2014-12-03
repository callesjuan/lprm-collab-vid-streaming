import datetime, logging, signal, sys, time
import json, matplotlib.pyplot as plt, pymongo, sleekxmpp

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

    try:
      self.mongo = pymongo.MongoClient(mongo_uri)
      self.db = self.mongo['lprm']

      self.sources = self.db['sources']
      # if len(self.sources.index_information()) == 0:
      #   self.sources.create_index([('jid', pymongo.TEXT), ('latlng', pymongo.GEO2D)])
      self.sources.ensure_index([('jid', pymongo.ASCENDING)], unique=True)
      self.sources.ensure_index([('latlng', pymongo.GEO2D)])

      self.groups = self.db['groups']
      self.groups.ensure_index([('jid', pymongo.TEXT)], unique=True)
      self.groups.ensure_index([('latlng', pymongo.GEO2D)])

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
    print message
    delay = message["delay"].get_stamp()
    if delay is None: # process only if it isn't an offline message
      try:
        jbody = json.loads(message["body"])
        func = getattr(self, "hnd_" + jbody["func"]) # gets reference for the application function
        args = jbody["args"] # application arguments
        head = {'from':str(message["from"]), 'to':str(message["to"])} # header arguments
        func(head, args)
      except:
        logging.warning("message error")
        print sys.exc_info()
    # self.make_message(mto=message["from"], mbody=message["body"]).send()

  def got_online(self, presence):
    print 'online'
    print presence

  def got_offline(self, presence):
    print 'offline'
    print presence

  '''
  LISTENERS 
  '''
 
  def hnd_stream_init(self, head, args):
    logging.info("stream_init")
    try:
      if args["group_jid"] is not None:
        # source = head['from'].split("@")[0]
        source = head['from']
        group = args['group_jid']
        hashtags = args['hashtags']
        
        data = {
          'hashtags': hashtags,
          'group': group
        }

        stamp = datetime.datetime.now().isoformat()

        if self.sources.find_one({'jid':source}) is None:
          data['jid'] = source
          data['stamp'] = stamp
          self.sources.insert(data)
          self.makePresence(pto=source, ptype='subscribe').send()
        else:
          self.sources.update({'jid':source}, {'$set':data})

        print source , " initiated"
     
        if self.groups.find_one({'group_jid':group}) is None:
          print "TRYING TO CREATE GROUP " + group + " AND SOURCE " + source
          self.groups.insert({'group_jid':group, 'members':[source], 'stamp':stamp, 'hashtags':hashtags})
        else:
          self.groups.update({'group_jid':group}, {'$addToSet':{'members':source}}) # $push/$addToSet

        print group , " created"
    except:
      print sys.exc_info()

  def hnd_stream_pause(self, head, args):
    logging.info("stream_pause")

  def hnd_stream_resume(self, head, args):
    logging.info("stream_resume")

  def hnd_stream_close(self, head, args):
    logging.info("stream_close")
    # leave current group

  def hnd_stream_exists(self, head, args):
    logging.info("stream_exists")

  def hnd_group_join(self, head, args):
    logging.info("group_join")
    # leave current group and join selected/existing group

  def hnd_group_leave(self, head, args):
    logging.info("group_leave")
    # leave current group and create group

  def hnd_group_match(self, head, args):
    logging.info("group_match")
    # find groups matching geolocation + hashtags
    
    try:
      source = head['from']
      latlng = args['latlng'].split(',')
      lat = int(latlng[0])
      lng = int(latlng[1])
      hashtags = args['hashtags']
      print source + " - " + str(latlng) + " - " + hashtags

      # match latlng
      match = self.groups.find({'latlng': {'$near': [lng, lat]}})
      print match.count()
      groups = []
      for g in match:
        d = {
          'group_jid': g['group_jid'],
          'hashtags': g['hashtags']
        }
        groups.append(d)
      
      print groups

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


  def hnd_update_location(self, head, args):
    logging.info("update_location")
    # publish current geolocation + sensor data + battery level

    try:
      jid = head['from']
      latlng = args['latlng'].split(',')
      lat = float(latlng[0])
      lng = float(latlng[1])
      
      data = {
        'latlng': [lng, lat]
      }
      self.sources.update({'jid':jid}, {'$set': data})
      print "location updated"
    except:
      print sys.exc_info()

    # calculate centroids
    try:
      group = self.groups.find_one({'members' : {'$in' : [jid]}})
      # src = self.sources.find_one({'jid':jid})
      # group = self.groups.find_one({'group_jid':src['group']})
      members = self.sources.find({'group' : group['group_jid']})
  
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

  def hnd_update_hashtags(self, head, args):
    logging.info("update_hashtags")
    # publish hashtag update

  '''
  MESSAGES
  '''
  
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

  jid = "comp-mapper@localhost"
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

    sources = client.sources.find()
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
