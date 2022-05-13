#differnt representation of strings

name1='viswanath'
name2="viswanath"
name3='viswan\'ath'
name4="viswan\"ath"
name5='viswa"nath'
name6="viswa'nath"
name7='puvvada "kali viswanath"'
name8="puvvada 'kali viswanath'"

print("names: ",name1+"\n")
print("names: ",name2+"\n")
print("names: ",name3+"\n")
print("names: ",name4+"\n")
print("names: ",name5+"\n")
print("names: ",name6+"\n")
print("names: ",name7+"\n")
print("names: ",name8+"\n")


#representation of multiline string
a = """Lorem ipsum dolor sit amet,
consectetur adipiscing elit,
sed do eiusmod tempor incididunt
ut labore et dolore magna aliqua."""
print(a)

a = '''Lorem ipsum dolor sit amet,
consectetur adipiscing elit,
sed do eiusmod tempor incididunt
ut labore et dolore magna aliqua.'''
print(a)


#String Operations(Slicing:You can return a range of characters by using the slice syntax.)

name="viswanath"

print("indexing to string:",name[0])
print("part of string: ",name[2:5]) #include index 2 but exclude index 5
print("indexing with starting index of string: ",name[2:]) #include index 2

#Negative Indexing(Use negative indexes to start the slice from the end of the string)
print("indexing in reverse of string:",name[-1])

#String Methods

#lenght of the string
a = "Hello, World!"
print("length of string: ",len(a))

#The strip() method removes any whitespace from the beginning or the end:
a = " Hello, World! "
print("removes the spaces in beginning and ending:",a.strip()) # returns "Hello, World!"

#The lower() method returns the string in lower case:
a = "Hello, World!"
print("lower case of string: ",a.lower())

#The upper() method returns the string in upper case:
a = "Hello, World!"
print("upper case of string: ",a.upper())

#The replace() method replaces a string with another string:
a = "Hello, World!"
print("replace of character: ",a.replace("H", "J"))

#The split() method splits the string into substrings if it finds instances of the separator:
a = "Hello, World!"
print("split of string: ",a.split(",")) # returns ['Hello', ' World!']


"""To check if a certain phrase or character is present in a string, we can use the keywords in or not in.

Example
Check if the phrase "ain" is present in the following text:"""

txt = "The rain in Spain stays mainly in the plain"
x = "ain" in txt
print("check if a certain phrase or character is present in a string: ",x)

#Check if the phrase "ain" is NOT present in the following text:

txt = "The rain in Spain stays mainly in the plain"
x = "aina" not in txt
print("Check if the phrase is NOT present in the string: ",x)

'''To concatenate, or combine, two strings you can use the + operator.

Example
Merge variable a with variable b into variable c:'''

a = "Hello"
b = "World"
c = a + b
print("Concatenation of string: ",c)

# To add a space between them, add a " ":

a = "Hello"
b = "World"
c = a + " " + b
print("adding the spaces: ",c)


'''As we learned in the Python Variables chapter, we cannot combine strings and numbers like this:

Example
age = 36
txt = "My name is John, I am " + age
print(txt)

But we can combine strings and numbers by using the format() method!

The format() method takes the passed arguments, formats them, and places them in the string where the placeholders {} are:

Example
Use the format() method to insert numbers into strings:'''

age = 36
txt = "My name is John, and I am {}"
print(txt.format(age))

"""The format() method takes unlimited number of arguments, and are placed into the respective placeholders:

Example"""
quantity = 3
itemno = 567
price = 49.95
myorder = "I want {} pieces of item {} for {} dollars."
print(myorder.format(quantity, itemno, price))

'''You can use index numbers {0} to be sure the arguments are placed in the correct placeholders:

Example'''
quantity = 3
itemno = 567
price = 49.95
myorder = "I want to pay {2} dollars for {0} pieces of item {1}."
print(myorder.format(quantity, itemno, price))



