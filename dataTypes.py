
#Python Tutorial:  https://www.w3schools.com/python/

"""Python has the following data types built-in by default, in these categories:

Text Type:	str
Numeric Types:	int, float, complex
Sequence Types:	list, tuple, range
Mapping Type:	dict
Set Types:	set, frozenset
Boolean Type:	bool
Binary Types:	bytes, bytearray, memoryview"""


'''Getting the Data Type
You can get the data type of any object by using the type() function:

Example
Print the data type of the variable x:'''

# integer
x = 5
print("integer value: ",x)
print(type(x))

# float
x = 20.5
print("float value: ",x)
print(type(x))

#complex
x = 1j
print("complex value: ",x)
print(type(x))

#string
x = "Hello World"
print("string value: ",x)
print(type(x))

#list
x = ["apple", "banana", "cherry"]
print("list value: ",x)
print(type(x))

#set
x = {"apple", "apple","banana", "cherry"}
print("set value: ",x)
print(type(x))

#tuple
x = ("apple", "banana", "cherry")
print("tuple value: ",x)
print(type(x))

#dict
x = {"name" : "John", "age" : 36,"salary" : 50000, "desination":"softwareEngineer"}
print("Dict value: ",x)
print(type(x))

#Boolean
x = True
print("Boolean value: ",x)
print(type(x))





'''Example	Data Type	
x = "Hello World"	str	
x = 20	int	
x = 20.5	float	
x = 1j	complex	
x = ["apple", "banana", "cherry"]	list	
x = ("apple", "banana", "cherry")	tuple	
x = range(6)	range	
x = {"name" : "John", "age" : 36}	dict	
x = {"apple", "banana", "cherry"}	set	
x = True	bool	
x = b"Hello"	bytes	
x = bytearray(5)	bytearray'''



'''Setting the Specific Data Type
If you want to specify the data type, you can use the following constructor functions:

Example	Data Type	Try it
x = str("Hello World")	str	
x = int(20)	int	
x = float(20.5)	float	
x = complex(1j)	complex	
x = list(("apple", "banana", "cherry"))	list	
x = tuple(("apple", "banana", "cherry"))	tuple	
x = range(6)	range	
x = dict(name="John", age=36)	dict	
x = set(("apple", "banana", "cherry"))	set	
	x = bool(5)	bool	
x = bytes(5)	bytes	
x = bytearray(5)	bytearray	
x = memoryview(bytes(5))	memoryview'''



