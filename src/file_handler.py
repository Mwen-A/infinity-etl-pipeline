import pandas as pd

# EXTRACT
def extract_csv(file_path: str):
    headers = ['datetime', 'location', 'name', 'items', 'payment-type', 'price', 'payment-details']
    df = pd.read_csv(file_path, names=headers)
    return df

df = extract_csv("data/isle-of-wight.csv")


# TRANSFORM
to_drop = ["name","payment-details"]
df.drop(columns=to_drop, inplace=True)
df.drop_duplicates()
df.dropna()

for i in range(len(df["items"])):
    df["items"][i] = df["items"][i].split(",")

for basket in df["items"]:
    for position in range(len(basket)):
        if position %3 == 0:
            # then we know it relates to the size of the item
            if basket[position] == "":
                # if it is a null, replace with NaN
                basket[position] = "regular"

# create another dataframe for the unique products



print(df.head())

# LOAD

# take the unique dataframe and push directly into the database


# go through each row
## it would read each location, check if it is already in the database
## take the price and payment type, and add them to the puchase table
## do a loop through items:
### take the size, name, price check if they already exist in the database