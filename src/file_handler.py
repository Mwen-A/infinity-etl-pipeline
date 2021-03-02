import pandas as pd

headers = ['datetime', 'location', 'name', 'items', 'payment-type', 'price', 'payment-details']
df = pd.read_csv("data/isle-of-wight.csv", names=headers)