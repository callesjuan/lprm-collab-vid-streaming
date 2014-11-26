import matplotlib.pyplot as plt
import numpy as np
import time

'''
plt.plot([1,2,3,4], [1,2,3,4], 'ro')
plt.axis([-10,10,-10,10])
plt.show()
'''

fig = plt.figure()

plt.ion()
plt.show()

plt.clf()
plt.axis([-10,10,-10,10])
plt.plot([1,2,3,4], [1,2,3,4], 'ro')
#plt.scatter(1,2)
plt.draw()
time.sleep(3)

plt.clf()
plt.axis([-10,10,-10,10])
plt.plot([1,2,3,4], [1,4,9,16], 'ro')
#plt.scatter(1,4)
plt.draw()

while True:
  pass
