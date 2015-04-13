import datetime, json, matplotlib.pyplot as plt, pymongo, signal, sys, threading, time, traceback

plotter = None
t = None

def thread_plot():
  fig = plt.figure()
  plt.ion()
  plt.show()

  while True:
    try:
      time.sleep(5)
      
      global plotter      
      if plotter.group_jid is None:
        pass
        
      plot_x = []
      plot_y = []

      online_streams = plotter.streams.find({'status':'streaming', 'group_jid':plotter.group_jid})
      
      if online_streams is not None:
        for stream in online_streams:
          if stream['latlng'] is not None:
            latlng = stream['latlng']
            plot_x.append(latlng[0])
            plot_y.append(latlng[1])

      plt.clf()
      plt.axis([-20,20,-20,20])
      plt.plot(plot_x, plot_y, 'ro')
      plt.draw()
    except:
      traceback.print_exc()

class Plotter():
  streams = None
  groups = None
  group_jid = None
  
  def __init__(self):
    mongo_uri = "mongodb://localhost:27017/"
    
    mongo = pymongo.MongoClient(mongo_uri)
    db = mongo['lprm']

    self.streams = db['streams']
    self.groups = db['groups']
    
  def handle_func(self, str_func):
      try:
        arr_func = str_func.split(" ")
        func = getattr(self, str('hnd_' + arr_func[0]))
        arr_func.pop(0)
        func(*arr_func)
      except:
        traceback.print_exc()
        
  def hnd_active_groups(self):
    active_groups = self.groups.find({'status':'active'})
    for g in active_groups:
      print g['group_jid']  
      
  def hnd_select_group(self, jid):
    self.group_jid = jid
    print self.group_jid + ' selected'
  
def signal_handler(signal, frame):
  sys.exit(0)

def main(argv):
  signal.signal(signal.SIGINT, signal_handler)
  
  global plotter
  plotter = Plotter()
  
  global t
  # t = threading.Thread(target=plotter.thread_plot, args=(,))
  t = threading.Thread(target=thread_plot)
  t.daemon = True
  t.start()
  
  while True:
    try:
      t.join(1)
      msg_in = raw_input("> ")
      plotter.handle_func(msg_in)
      pass
    except KeyboardInterrupt, EOFError:
      break
  
if __name__ == '__main__':
  main(sys.argv[1:])
