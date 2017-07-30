#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.02

Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value and ttl associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => {"value": Binary, "ttl": 1000}
      print rv["value"].data => "value"
  put(base64 key, base64 value, int ttl)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"), 1000)
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable
"""
"""Project submitted by Srivalli Boddupalli & Shivangi Singh
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest, shelve
from datetime import datetime, timedelta
from xmlrpclib import Binary


# Presents a HT interface
class SimpleHT:
  def __init__(self):
    self.data = {} 
    
  def count(self):
    return len(self.data)

  # Retrieve something from the HT
  def get(self, key):
    # Default return value
    rv = {}
    # If the key is in the data structure, return properly formated results    
    shelve_datastore=shelve.open(datastore)
    key = key.data    
    if key in self.data:
       rv = Binary(shelve_datastore[key])
    shelve_datastore.close()
    return rv

  # Insert something into the HT
  def put(self, key, value):
    self.data[key.data] = value.data    
    shelve_datastore=shelve.open(datastore)
    shelve_datastore.update(self.data)
    shelve_datastore.close()
    return True
  
  def corrupt(self, key):        
    shelve_datastore=shelve.open(datastore)
    if key in shelve_datastore.keys():
       self.data[key.data] = Binary(pickle.dumps("abc12345\nabcdefghijklmnop1234567891234567")) 
       print ("corrupted path :")
       print (key[2:])
       print ("Corrupted block no: ")
       print (key[1])
       shelve_datastore.update(self.data)
       shelve_datastore.close()
       return True
    else: 
       print ("Path not found on this dataserver")   
       return False

  # Establish Connection with adjacent servers
  def connect(self) :
     print("Established connection")
     return True 

  
    
  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Clear all contents
  def clear(self):
    self.data.clear()
    return True

  # Delete a file
  def remove(self, key):
    if key.data in self.data:
      del self.data[key.data]
      return True
    else:
      return False

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

def main(argv):
  optlist, args = getopt.getopt(sys.argv[1:], "", ["port=", "test"])
  ol={}
  argv= sys.argv
  for k,v in optlist:
    ol[k] = v  
  global i
  i=int(argv[1])
  global self_port 
  self_port = int(argv[i+2])
  global datastore
  datastore = "datastore" +str(i)+ ".db"
  if (i== len(sys.argv)-3):
     global next_port
     next_port = int(argv[2])
     global datastore_next 
     datastore_next = "datastore" + str(0) + ".db"
  else :
     next_port = int(argv[i+3])
     datastore_next = "datastore" + str(i+1) + ".db"
  if (i== 0) :
     global prev_port
     prev_port = int(argv[-1])        
     global datastore_prev 
     datastore_prev = "datastore" + str(len(sys.argv)-3) + ".db"
  else :
     datastore_prev = "datastore" + str(i-1) + ".db"
     prev_port = int(argv[i+1])

  status_next = False
  nextserv = xmlrpclib.ServerProxy("http://localhost:" + str(next_port))
  try:
     status_next = nextserv.connect()  
      
  except Exception :
     print ("unable to establish connection with next server with Port no:")
     print (next_port)
 

  status_prev = False
  prevserv = xmlrpclib.ServerProxy("http://localhost:" + str(prev_port))
  try:
     status_prev = prevserv.connect()
      
  except Exception :
     print ("unable to establish connection with previous server with Port no: ")
     print (prev_port)
  
   
  
  new_dict = {}
  shelve_obj = shelve.open(datastore)
  shelve_obj.update(new_dict)
  if status_next == False and status_prev== False:
      print("Servers are starting up")
  else :
     if( status_next == True) :
        print ("Connection established with next server. Fetching data...")
        shelve_next = shelve.open(datastore_next)
        for j in shelve_next.keys():
           if ( j[0] == i) :
              new_dict[j] = shelve_next[j]
        shelve_obj.update(new_dict)

     if (status_prev == True) :
        print ("Connection established with previous server. Fetching data...")
        shelve_prev = shelve.open(datastore_prev)
        for j in shelve_prev.keys():
           if ( j[0] == i) :
              new_dict[j] = shelve_prev[j]
        shelve_obj.update(new_dict)
  
  
  port = self_port
  if "--port" in ol:
    port = int(ol["--port"])  
  if "--test" in ol:
    sys.argv.remove("--test")
    unittest.main()
    return
  serve(port)
  

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.clear)
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.register_function(sht.remove)
  file_server.register_function(sht.connect)
  file_server.register_function(sht.corrupt)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))
  
  def corrupt(self, key):
    return self.caller.corrupt(Binary(key))
  
  def connect(self):
    return self.caller.connect(Binary())

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(51234, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
  main(sys.argv[1:])
