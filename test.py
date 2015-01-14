class Test:

  def wibble(self, arg1, arg2=None):
    print arg1
    print arg2
    
  def zibble(self):
    print 'asdf'

if __name__ == '__main__':
  function_name = 'zibble'
  args = ['fdsa']

  test = Test()
  func = getattr(test, function_name)
  func(*args)
