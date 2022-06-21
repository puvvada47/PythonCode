list = ["apple", "banana", "cherry"]  #list
# x.__add__("mangoes")
list.append("pomegrante")
list.insert(1, "mangaoes")
print(list.__getitem__(0))
print(len(list))
print(list.count("apple"))
print(list)
print(list[0])



set = {"apple", "apple","banana", "cherry"}
print(len(set))
print(set.add("guva"))
print(set.update(["grapes","oranges"]))
print(set)


dictionary = {"name" : "John", "age" : 36,"salary" : 50000, "desination":"softwareEngineer"}
print(dictionary["age"])
print(len(dictionary))
a=dictionary.__getitem__("salary")
print(dictionary.__getitem__("salary"))
i=dictionary.items()
print(dictionary.items())
print(dictionary.keys())
print(dictionary.values())



