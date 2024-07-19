import pandas as pd
import re
import uuid

class DataCleaning:
    def clean_store_data(self, df):
        df = df.copy()
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df = df.dropna(subset=['longitude', 'latitude'])
        return df

    def clean_card_data(self, df):
        df = df.copy()
        df['expiry_date'] = df['expiry_date'].astype(str)
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        return df

    @staticmethod
    def convert_weight(weight):
        if pd.isna(weight):
            return None
        weight = str(weight)
        weight = re.sub(r'[^\d\.]', '', weight)
        if 'kg' in weight:
            return float(weight.replace('kg', '').strip())
        elif 'g' in weight:
            return float(weight.replace('g', '').strip()) / 1000
        elif 'ml' in weight:
            return float(weight.replace('ml', '').strip()) / 1000
        else:
            return float(weight) / 1000  # default assumption grams if no unit

    def convert_product_weights(self, df):
        df = df.copy()
        df['weight'] = df['weight'].apply(self.convert_weight)
        return df

    @staticmethod
    def clean_products_data(df):
        def clean_price(price):
            try:
                return float(price.replace('£', '').replace(',', '').strip())
            except ValueError:
                return None  # or any default value you prefer

        # Convert the product_price column to string before cleaning
        df['product_price'] = df['product_price'].astype(str)
        df['product_price'] = df['product_price'].apply(clean_price)

        # Optionally drop rows where the price could not be cleaned
        df = df.dropna(subset=['product_price'])

        return df

    def clean_orders_data(self, df):
        # Calculate max lengths for VARCHAR fields
        max_card_number_length = df['card_number'].astype(str).str.len().max()
        max_store_code_length = df['store_code'].astype(str).str.len().max()
        max_product_code_length = df['product_code'].astype(str).str.len().max()

        # Cast columns to required data types
        df['date_uuid'] = df['date_uuid'].apply(lambda x: str(uuid.UUID(x)) if pd.notna(x) else None)
        df['user_uuid'] = df['user_uuid'].apply(lambda x: str(uuid.UUID(x)) if pd.notna(x) else None)
        df['card_number'] = df['card_number'].astype(str)
        df['store_code'] = df['store_code'].astype(str)
        df['product_code'] = df['product_code'].astype(str)
        df['product_quantity'] = df['product_quantity'].astype('int16')

        # Drop unnecessary columns
        df.drop(columns=['first_name', 'last_name', '1'], inplace=True, errors='ignore')

        return df

    def clean_date_details(self, date_details_df):
        # Clean the date details data
        return date_details_df

    def clean_users_data(self, df):
        # Converting 'first_name' and 'last_name' to VARCHAR(255)
        df['first_name'] = df['first_name'].astype(str).str[:255]
        df['last_name'] = df['last_name'].astype(str).str[:255]
        
        # Converting 'date_of_birth' and 'join_date' to DATE
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce').dt.date
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').dt.date
        
        # Converting 'country_code' to VARCHAR(3) (assuming a max length of 3)
        df['country_code'] = df['country_code'].astype(str).str[:3]
        
        # Converting 'user_uuid' to UUID
        df['user_uuid'] = df['user_uuid'].apply(lambda x: str(x) if pd.notnull(x) else None)
        
        return df
