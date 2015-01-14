import matplotlib.pyplot as plt, pymongo, sys, time

def main(argv):
  mongo_uri = "mongodb://localhost:27017/"
  
  mongo = pymongo.MongoClient(mongo_uri)
  db = mongo['lprm']

  sources = db['sources']

  fig = plt.figure()
  plt.ion()
  plt.show()

  while True:
    time.sleep(5)
    
    plot_x = []
    plot_y = []

    online_sources = sources.find({'status':'on'})
    
    if online_sources is not None:
      for source in online_sources:
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
