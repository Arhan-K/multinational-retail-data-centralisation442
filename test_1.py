import pandas as pd
import numpy as np
import tabula

def check_numeric(card_number_list):
    string_cards = [str(card_number) for card_number in card_number_list]
    false_card_numbers = []
    for card in string_cards:
        if not card.isdigit():
            false_card_numbers.append(card)
    return false_card_numbers
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
tabular_data = tabula.read_pdf(pdf_link, pages = 'all', multiple_tables = True)
df = pd.concat(tabular_data, ignore_index = True)
print(f"total card numbers = {len(df['card_number'])}")
print(f"unique card numbers before cleaning = {df['card_number'].nunique()}")
#print(df.isnull().sum())
card_number_list = df['card_number'].tolist()
false_cards = check_numeric(card_number_list)
for card in false_cards:
    df['card_number'] = df['card_number'].replace(card, np.nan)
df = df.replace('NULL', np.nan)
df = df.dropna(how = 'any')
print(f"unique card numbers = {df['card_number'].nunique()}")
print(f"valid card numbers = {len(df['card_number'])}")
print(df['card_number'].describe())
print(df['card_number'].value_counts())

#print(df.value_counts())
#df.isna()