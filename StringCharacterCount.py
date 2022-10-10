input = 'aaabbbccaadddeeeeefffllllllaaaa'
# output=3a3b2c2a3d5e3f6l4a


appendOutput = ''
c = list(input)
print("character array: ", c)
size = len(c)
i = 0
while (i < size):
    count = 1
    j = i + 1
    while (j < size):
        if c[i] == c[j]:
            count = count + 1
            if (j == (size - 1)):
                appendOutput = appendOutput + str(count) + c[i]
        else:
            appendOutput = appendOutput + str(count) + c[i]
            print(" append value: ", appendOutput)
            break
        j = j + 1
    i = i + count
print("expected Appended Output: ",appendOutput)
