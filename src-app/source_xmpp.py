import getopt, logging, signal, sys, time
import json, sleekxmpp

class SourceXMPP(sleekxmpp.ClientXMPP):

  '''
  CONSTANTS
  '''
  MAPPER_JID = 'comp-mapper@localhost'

  def __init__(self, jid, password):
    sleekxmpp.ClientXMPP.__init__(self, jid, password)
    self.register_plugin('xep_0004') # Data forms
    self.register_plugin('xep_0030') # Service discovery
    self.register_plugin('xep_0045') # Multi-user chat
    self.register_plugin('xep_0060') # Publish subscribe
    self.register_plugin('xep_0199') # XMPP ping
    self.register_plugin('xep_0203') # Delayed delivery
    self.add_event_handler("session_start", self.handle_start)
    self.add_event_handler("message", self.handle_message)

  def handle_start(self, event):
    logging.info("connected")
    print "connected"
    self.get_roster()
    self.send_presence(pstatus="")

  def handle_message(self, message):
    print message
    delay = message["delay"].get_stamp()
    if delay is None: # process only if it isn't an offline message
      try:
        jbody = json.loads(message["body"])
        func = getattr(self, "hnd_" + jbody["func"]) # gets reference for the application function
        args = jbody["args"] # application arguments
        head = {'from':message["from"], 'to':message["to"]} # header arguments
        func(head, args)
      except:
        logging.warning("message error")
    # self.make_message(mto=message["from"], mbody=message["body"]).send()

  def handle_func(self, str_func):
    func = getattr(self, str_func)
    func()
    
  '''
  LISTENERS
  '''

  '''
  MESSAGES
  '''
  def stream_init(self, to=None, group=None):
    logging.info("stream_init")
    
    if to is None:
      to = self.MAPPER_JID
    
    try:
      msg = {
        'func':'stream_init',
        'args': {}
      }
      if group is not None:
        msg['args']['group'] = group
      self.make_message(mto=to, mbody=json.dumps(msg)).send()
    except:
      logging.error("error")

  def stream_pause(self, head, args):
    logging.info("stream_pause")

  def stream_resume(self, head, args):
    logging.info("stream_resume")

  def stream_close(self, head, args):
    logging.info("stream_close")
    # leave current group

  def stream_exists(self, head, args):
    logging.info("stream_exists")

  def group_join(self, head, args):
    logging.info("group_join")
    # leave current group and join selected/existing group

  def group_leave(self, head, args):
    logging.info("group_leave")
    # leave current group and create group

  def group_match(self, head, args):
    logging.info("group_match")
    # find groups matching geolocation + hashtags

  def update_context(self, head, args):
    logging.info("update_context")
    # publish current geolocation + sensor data + battery level

  def update_hashtags(self, head, args):
    logging.info("update_hashtags")
    # publish hashtag update
    
'''
CONSOLE
'''

def signal_handler(signal, frame):
  client.disconnect(wait=True)
  sys.exit(0)

client = None

def main(argv):
  signal.signal(signal.SIGINT, signal_handler)

  logging.basicConfig(filename='source_xmpp.log',level=logging.DEBUG)
  
  
  root = logging.getLogger()
  ch = logging.StreamHandler(sys.stdout)
  ch.setLevel(logging.INFO)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  ch.setFormatter(formatter)
  root.addHandler(ch)
  
  logging.info("source started")
  
  global client
  
  try:
  
    '''
    CONSOLE ARGS
    '''
    
    opts, args = getopt.getopt(argv, "j:p:", ['jid=', 'pwd='])
    
    if len(opts) != 2:
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
        msg_in = raw_input("msg:")
        client.handle_func(msg_in)
        pass
      except KeyboardInterrupt, EOFError:
        client.disconnect(wait=True)
        break
    
  except Exception as e:
    logging.error("error")
    sys.exit(0)

if __name__ == '__main__':
  main(sys.argv[1:])
