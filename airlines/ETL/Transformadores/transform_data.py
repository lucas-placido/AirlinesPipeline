import pandas as pd

class Transform():

    def get_english_version(self, col):
        return col.apply(lambda x: eval(x).get('en'))
    
    def datetime_converter(self, df):
        return pd.to_datetime(df)

