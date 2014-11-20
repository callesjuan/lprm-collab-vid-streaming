import logging, signal, sys, time
import json, sleekxmpp

class CorrelatorXMPP(sleekxmpp.ClientXMPP):

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

    self.sources = {}
    self.groups = {}

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
    # self.make_message(mto=message["from"], mbody=message["body"]).send()

  def handle_func(self, str_func):
    arr_func = str_func.split(" ")
    func = getattr(self, arr_func[0])
    if len(arr_func) == 1:
      func()
    else
      func(arr_func[1])

  '''
  LISTENERS 
  '''
  
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
  
  logging.basicConfig(filename='correlator_xmpp.log',level=logging.DEBUG)
  logging.info("correlator started")
  
  global client

  jid = "comp-correlator@localhost"
  pwd = "123"

  client = CorrelatorXMPP(jid, pwd)
  client.connect()
  client.process()

  while True:
    try:
      msg_in = raw_input("> ")
      client.handle_func(msg_in)
      pass
    except KeyboardInterrupt, EOFError, Exception:
      print sys.exc_info()
      client.disconnect(wait=True)
      break

if __name__ == '__main__':
  main(sys.argv[1:])
