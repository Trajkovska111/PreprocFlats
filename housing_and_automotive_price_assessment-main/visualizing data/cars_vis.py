import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

# Read the data

cars = pd.read_csv("crawler/data/cars.csv")
cars_po_dogovor = pd.read_csv("crawler/data/test_cars.csv")
cars_mkd = pd.read_csv("crawler/data/cars_mkd.csv")

# Prices in euros and macedonian denars
cars['price'].plot(label='euros', color='blue', figsize=(15, 7))
cars_mkd['price'].plot(label='mkd', color='red')
plt.title('Prices in euros and macedonian denars')
plt.legend()

# Diverging text bars of car price by make in euros
lala = cars.groupby('make')['price'].agg([np.mean])
lala.columns = ['mean_price']

print(lala)

x = lala.loc[:, ['mean_price']]
lala['price_n'] = (x - x.mean()) / x.std()
lala.sort_values('price_n', inplace=True)
lala.reset_index(inplace=True)

plt.figure(figsize=(14, 18))
plt.hlines(y=lala.index, xmin=0, xmax=lala.price_n)
for x, y, tex in zip(lala.price_n, lala.index, lala.price_n):
    t = plt.text(x, y, round(tex, 2), horizontalalignment='right' if x < 0 else 'left', verticalalignment='center',
                 fontdict={'color': 'blue' if x < 0 else 'red', 'size': 12})
plt.yticks(lala.index, lala.make, fontsize=12)
plt.title("Diverging text bars of car price by make", fontdict={'color': 'indigo', "size": 20})
plt.grid(linestyle='dashdot', alpha=0.5)
plt.show()

#  Diverging text bars of car price by make in denar

lala_mkd = cars_mkd.groupby('make')['price'].agg([np.mean])
lala_mkd.columns = ['mean_price']

print(lala_mkd)

x = lala_mkd.loc[:, ['mean_price']]
lala_mkd['price_n'] = (x - x.mean()) / x.std()
lala_mkd.sort_values('price_n', inplace=True)
lala_mkd.reset_index(inplace=True)

plt.figure(figsize=(14, 18))
plt.hlines(y=lala_mkd.index, xmin=0, xmax=lala_mkd.price_n)
for x, y, tex in zip(lala_mkd.price_n, lala_mkd.index, lala_mkd.price_n):
    t = plt.text(x, y, round(tex, 2), horizontalalignment='right' if x < 0 else 'left', verticalalignment='center',
                 fontdict={'color': 'blue' if x < 0 else 'red', 'size': 12})
plt.yticks(lala_mkd.index, lala_mkd.make, fontsize=12)
plt.title("Diverging text bars of car price by make", fontdict={'color': 'indigo', "size": 20})
plt.grid(linestyle='dashdot', alpha=0.5)
plt.show()

# Boxplot euros

plt.figure(figsize=(12, 6))
sorted_nb = cars.groupby(['make'])['price'].median().sort_values()
sns.boxplot(x=cars['make'], y=cars['price'], order=list(sorted_nb.index))
plt.xticks(rotation=70)

# Boxplot denari

plt.figure(figsize=(12, 6))
sorted_nb = cars_mkd.groupby(['make'])['price'].median().sort_values()
sns.boxplot(x=cars_mkd['make'], y=cars['price'], order=list(sorted_nb.index))
plt.xticks(rotation=70)

#just histograms

cars.hist()
cars_mkd.hist()

#scaterplot
plt.figure(figsize=(12,6))
sns.scatterplot(x=cars['price'], y=cars['distance_in_km'])

plt.figure(figsize=(12,6))
sns.scatterplot(x=cars_mkd['price'], y=cars['distance_in_km'])


#correlation map

cars.corr()
f, ax = plt.subplots(figsize=(10, 10))
sns.heatmap(cars.corr(), annot=True, linewidths=.5, fmt= '.1f',ax=ax)
plt.show()

cars_mkd.corr()
f, ax = plt.subplots(figsize=(10, 10))
sns.heatmap(cars_mkd.corr(), annot=True, linewidths=.5, fmt= '.1f',ax=ax)
plt.show()

# just the price
plt.figure(figsize = (15,8))
sns.distplot(cars['price'])
plt.xlabel('Price', fontsize = 20)
plt.show()

plt.figure(figsize = (15,8))
sns.distplot(cars_mkd['price'])
plt.xlabel('Price', fontsize = 20)
plt.show()