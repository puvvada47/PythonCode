dict = {"name" : "John", "age" : 36,"salary" : 50000, "desination":"softwareEngineer"}
print("Dict value: ",dict)
print("based on key:"+dict["name"])
print("based on key:"+str(dict["age"]))
print("based on key:"+str(dict["salary"]))


schema="name age salary"
dict = {"fname" : ("table",schema), "lname" : ("tables",schema)}
print("tuple first item:",dict["lname"].__getitem__(0)
      )
print("tuple second item:",dict["lname"].__getitem__(1)
      )

