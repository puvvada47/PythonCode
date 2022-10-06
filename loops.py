
#----------------------------------
#1st variety: looping via range of numbers
#----------------------------------


for x in range(0,6):
  print(x)

print("===============================")
#------------------------------------------
#2nd variety: looping through list and break the loop if condition statisfied
#------------------------------------------
fruits = ["apple", "banana", "cherry"]
for fruit in fruits:
  if fruit == "banana":
    break
  print(fruit)

print("===============================")

#--------------------------------------------
#3rd variety looping through the list and continue the loop if condition satisified
# and using "and" condition
#---------------------------------------------
fruits = ["apple", "banana", "cherry"]
for x in fruits:
  if x == "banana" and x.__contains__("viswa") and len(x)>0:
     continue
  print(x)

print("===============================")

#--------------------------------------------
#4th variety: using the "or" condition
fruits = ["mango", "grapes", "pineapple"]
for x in fruits:
  if x == "mango" or x.__contains__("go") or len(x)>0:
    print("condition satisfied: so donot print any fruit")
    continue
  print(x)

print("===============================")


#--------------------------------------------
#5th variety: using equal
vegetables = ["tomateos", "potaeos", "ladiesfinger"]
for x in vegetables:
  if x == "tomateos":
    print(x)

print("===============================")



#--------------------------------------------
#6th variety: using not equal to
vegetables = ["tomateos", "potaeos", "ladiesfinger"]
for x in vegetables:
  if x != "tomateos":
    print(x)

print("===============================")