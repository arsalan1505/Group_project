value2 = "Large Latte - 2.45, Large Flavoured latte - Hazelnut - 2.85, Large Flavoured latte - Vanilla - 2.85"
basketitems = []
splitvalue = value2.split(",")

for item in splitvalue:
    print(item)
    basket = {}
    if item.count("-") == 1:
        splitval = item.split("-")
        if "Large" in item:
            basket["item_size"] = "Large"
            name = splitval[0].replace("Large", "").strip()
            basket["item_name"] = name
        
        else:
            basket["item_size"] = "Regular"
            name = splitval[0].replace("Regular", "").strip()
            basket["item_name"] = name
        
        basket["item_price"] = float(splitval[1])

    elif item.count("-") == 2:
        splitval = item.rsplit("-", 1)
        basket["item_price"] = float(splitval[1])
        if "Large" in item:
            basket["item_size"] = "Large"
            name = splitval[0].replace("Large", "").strip()
            basket["item_name"] = name
        else:
            basket["item_size"] = "Regular"
            name = splitval[0].replace("Regular", "").strip()
            basket["item_name"] = name
    basketitems.append(basket)

for item in basketitems:
    print(item)

