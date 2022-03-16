import pandas as pd
import math

# Read and merge the data

cars_1 = pd.read_csv("C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_1.csv")
cars_2 = pd.read_csv("C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_2.csv")
cars_3 = pd.read_csv("C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_3.csv")
cars_4 = pd.read_csv("C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_4.csv")
cars_5 = pd.read_csv("C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_5.csv")
cars_6 = pd.read_csv("C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_6.csv")
cars_7 = pd.read_csv("C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_7.csv")
cars_8 = pd.read_csv("C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_8.csv")
cars_9 = pd.read_csv("C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_9.csv")
cars_10 = pd.read_csv("C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_10.csv")

cars = cars_1
cars = cars.append(cars_2)
cars = cars.append(cars_3)
cars = cars.append(cars_4)
cars = cars.append(cars_5)
cars = cars.append(cars_6)
cars = cars.append(cars_7)
cars = cars.append(cars_8)
cars = cars.append(cars_9)
cars = cars.append(cars_10)
cars

cars = cars.drop(['Unnamed: 0', "Латитуда", "Лонгитуда"], axis=1)
cars.rename(columns={'Цена': 'price', 'Марка': 'make', 'Модел': 'model', 'Година': 'year', 'Гориво': 'fuel',
                     'Километри': 'distance_in_km', 'Менувач': 'transmission',
                     'Каросерија': 'type', 'Боја': 'color', 'Регистрација': 'registration',
                     'Регистриранадо': 'registered_till', 'Силанамоторот': 'HP',
                     'Kласанаемисија': 'standard'}, inplace=True)
cars.isna().sum()

cars = cars.dropna()

# Годинаa
cars['year'] = cars['year'].astype('int64')

# Километри
cars['distance_in_km'] = cars['distance_in_km'].astype('int64')

# Регистриранадо
cars['registered_till'] = cars['registered_till'].astype('int64')

# KW and HP

cars[['KW', 'HP']] = cars['HP'].str.split("kw/", n=1, expand=True)

cars[['HP', 'drop']] = cars['HP'].str.split("k", n=1, expand=True)

cars = cars.drop({'drop'}, axis=1)

cars

# Цена

cars_cena_dogovor = pd.DataFrame(
    columns={'price', 'make', 'model', 'year', 'fuel', 'distance_in_km', 'transmission', 'type', 'color',
             'registration', 'registered_till', 'HP', 'standard'})
cars_cena_mkd = pd.DataFrame(
    columns={'price', 'make', 'model', 'year', 'fuel', 'distance_in_km', 'transmission', 'type', 'color',
             'registration', 'registered_till', 'HP', 'standard'})

cars_cena_dogovor = cars[cars.price.isin(['По договор']) == True]

cars_cena_mkd = cars[cars.price.str.contains('МКД') == True]

cars = cars[cars.price.str.contains('МКД') == False]
cars = cars[cars.price.isin(['По договор']) == False]

cars_cena_mkd[['price', 'drop']] = cars_cena_mkd['price'].str.split("МКД", n=1, expand = True)
cars_cena_mkd = cars_cena_mkd.drop({'drop'}, axis=1)

cars[['price', 'drop']] = cars['price'].str.split("€", n=1, expand = True)
cars = cars.drop({'drop'}, axis=1)

cars['price'] = cars['price'].astype('int64')
cars_cena_mkd['price'] = cars_cena_mkd['price'].astype('int64')

def convent(value):
    euros = value * 0.01619
    return math.ceil(euros)


cars_cena_mkd['price_euros'] = cars_cena_mkd['price'].apply(convent).values

#drop values under 300 and above 10000

cars_cena_mkd = cars_cena_mkd.query("`price_euros` >= 300 and `price_euros` <= 10000")
cars = cars.query("`price` >= 300 and `price` <= 10000")


cars_cena_dogovor

cars_cena_mkd

cars

cars.to_csv('C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars.csv')
cars_cena_mkd.to_csv('C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cars_mkd.csv')
cars_cena_dogovor.to_csv('C:/Users/tamar/OneDrive/Desktop/FINKI/TIMSKI PROEKT/housing_and_automotive_price_assessment/crawler/data/cena_dogovor.csv')

# %%
