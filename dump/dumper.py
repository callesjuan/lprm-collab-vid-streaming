import datetime, json, pymongo, signal, sys, threading, time, traceback

db = None

def dump_db():
  filename = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
  filename_sources = filename + "_sources"
  filename_streams = filename + "_streams"
  filename_groups = filename + "_groups"
  filename_pings = filename + "_pings"
  for doc in db['sources'].find({},{'_id':0}):
    print doc
  with open(filename_sources, 'w') as outfile:
    json.dump([doc for doc in db['sources'].find({},{'_id':0})], outfile)
  with open(filename_streams, 'w') as outfile:
    json.dump([doc for doc in db['streams'].find({},{'_id':0})], outfile)
  with open(filename_groups, 'w') as outfile:
    json.dump([doc for doc in db['groups'].find({},{'_id':0})], outfile)
  with open(filename_pings, 'w') as outfile:
    json.dump([doc for doc in db['pings'].find({},{'_id':0})], outfile)
  
def signal_handler(signal, frame):
  sys.exit(0)

def main(argv):
  signal.signal(signal.SIGINT, signal_handler)
  
  mongo_uri = "mongodb://localhost:27017/"    
  mongo = pymongo.MongoClient(mongo_uri)
  
  global db
  db = mongo['lprm']
  
  while True:
    try:
      time.sleep(5)
      dump_db()
      pass
    except KeyboardInterrupt, EOFError:
      break
  
if __name__ == '__main__':
  main(sys.argv[1:])
