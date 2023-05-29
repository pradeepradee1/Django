#print(80/100)
print("\U0001F618")

import sys
a=[1,2,3,4,5,6]
print(sys.getsizeof(a))
print(sys.getsizeof(a[2]))
# a={"a":1,"b":2,"c":4}
# print(sys.getsizeof(a))
# print(sys.getsizeof(a["b"]))

for i in a:
    print(i,id(i))