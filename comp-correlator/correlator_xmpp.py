import datetime, dateutil.parser, signal, sys, time
import json, numpy as np, pymongo, sklearn.cluster as skc, sleekxmpp

class CorrelatorXMPP(sleekxmpp.ClientXMPP):

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
    
    jid_tuple = jid.split("@")
    self.DOMAIN = jid_tuple[1]
    self.MUC_JID = 'conference.' + self.DOMAIN

    self.local_sources = {}
    self.local_groups = {}

    try:
      self.mongo = pymongo.MongoClient(mongo_uri)
      self.db = self.mongo['lprm']
    except:
      print sys.exc_info()
      sys.exit(0)

  def handle_start(self, event):
    print "connected"
    self.send_presence(pstatus="")
    self.get_roster()

  def handle_message(self, message):
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
        print "message error"
    # self.make_message(mto=message["from"], mbody=message["body"]).send()

  def handle_func(self, str_func):
    arr_func = str_func.split(" ")
    func = getattr(self, arr_func[0])
    if len(arr_func) == 1:
      func()
    else:
      func(arr_func[1])

  '''
  LISTENERS 
  '''
  
  '''
  MESSAGES
  '''

  '''
  ROUTINES/TASKS
  '''
  def calculate_centroids(self):
    print "calculating centroids"
    # calculate group centroids
    groups = self.db['groups'].find({'status':'active'})
    for group in groups:
      print group['group_jid']
      members = self.db['sources'].find({'status':'streaming', 'group':group['group_jid']})
      if members.count() > 0:
        sum_lng = 0
        sum_lat = 0
        count = len(group['members'])
        print "count " + str(count) + " " + str(members.count())
        for member in members:
          print member['latlng']
          latlng = member['latlng']
          sum_lng += latlng[0]
          sum_lat += latlng[1]
        centroid_lng = sum_lng/count
        centroid_lat = sum_lat/count
        group_latlng = str(centroid_lat) + "," + str(centroid_lng)
        self.db['groups'].update({'group_jid':group['group_jid']}, {'$set' : {'centroid' : group_latlng}})
    print "calculated centroids"
  
  def send_suggestions(self):
    print "sending suggestions"
    # cluster groups (group location, centroid, should be updated after its sources send update_location)
    # if it finds clusters then checks which group is more crowded and ancient, the pivot/master, finally it suggests members from the remaining groups to join it
    groups_iter = self.db['groups'].find({'status':'active'})
    groups_arr = []
    X_arr = []
    
    print 'clustering with ' + str(groups_iter.count()) + ' groups'

    i = 0
    for group in groups_iter:
      latlng = group['centroid']
      groups_arr.append(group)
      new_row = [i, float(latlng[0]), float(latlng[1])]
      X_arr.append(new_row)
      # print new_row
      i += 1 

    X = np.array(X_arr)
    EPS = 2
    dbs = skc.DBSCAN(eps=2, min_samples=1).fit(X[:,1:])
    
    labels = dbs.labels_
    # print labels

    super_groups = {}
    
    for i in range(0, len(labels)):
      sg_key = str(labels[i])
      if not super_groups.has_key(sg_key):
        super_groups[sg_key] = {}
        super_groups[sg_key]['members'] = []
        super_groups[sg_key]['pivot'] = i
      else:
        members_pivot = len(groups_arr[super_groups[sg_key]['pivot']]['members'])
        members_curr = len(groups_arr[i]['members'])
        if members_curr > members_pivot:
          super_groups[sg_key]['pivot'] = i
        elif members_curr == members_pivot:      
          stamp_pivot = dateutil.parser.parse(groups_arr[super_groups[sg_key]['pivot']]['stamp'])
          stamp_curr = dateutil.parser.parse(groups_arr[i]['stamp'])
          if stamp_curr < stamp_pivot:
            super_groups[sg_key]['pivot'] = i
      super_groups[sg_key]['members'].append(X_arr[i])
    
    # print super_groups

    for k,sg in super_groups.items():
      for i in range(0, len(sg['members'])):
        g = sg['members'][i][0]
        if sg['pivot'] != g:
          notify_jid = (groups_arr[g]['group_jid']+"@"+self.MUC_JID)
          pivot = groups_arr[sg['pivot']]['group_jid']

          print "suggest sources at [" + notify_jid + "] to join [" + pivot + "]"
          
          msg = {
            'func':'notify_correlation',
            'args': {
              'group_jid':pivot,
              'hashtags':groups_arr[sg['pivot']]['hashtags'],
              'num_members':len(groups_arr[sg['pivot']]['members'])
            }
          }

          try:
            self.make_presence(pto=str(notify_jid+"/correlator")).send()
            self.make_message(mto=notify_jid, mbody=json.dumps(msg), mtype='groupchat').send()
            self.make_presence(ptype='unavailable', pto=notify_jid).send()
          except:
            print sys.exc_info()

    print "sended suggestions"
      

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

  jid = "correlator@git.lprm.inf.ufes.br"
  pwd = "123"
  mongo_uri = "mongodb://localhost:27017"

  client = CorrelatorXMPP(jid, pwd, mongo_uri)
  client.connect()
  client.process()
  
  while True:
    try:
      # msg_in = raw_input("> ")
      # client.handle_func(msg_in)
      time.sleep(10)
      print "firing correlator"
      client.calculate_centroids()
      client.send_suggestions()
      
      pass
    except Exception:
      print sys.exc_info()
      client.disconnect(wait=True)
      break

if __name__ == '__main__':
  main(sys.argv[1:])
