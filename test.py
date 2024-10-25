# Get user input, separated by new lines, until an empty line is entered
print("Enter prices (type an empty line to finish):")
prices = []
while True:
    line = input()
    if line == "":
        break
    prices.append(line.strip())

# Join the list items with commas and print the result
result = " ".join(prices)
print(result)
