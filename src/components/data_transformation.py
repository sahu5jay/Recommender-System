import os
import sys
import ast
import pandas as pd
from nltk.stem.porter import PorterStemmer
from src.exception import CustomException
from src.logger import logging


class DataTransformation:
    def __init__(self):
        self.ps = PorterStemmer()

    def convert(self, obj):
        return [i['name'] for i in ast.literal_eval(obj)]

    def fetch_director(self, obj):
        L = []
        for i in ast.literal_eval(obj):
            if i['job'] == 'Director':
                L.append(i['name'])
                break
        return L

    def initiate_data_transformation(self, raw_path):
        try:
            logging.info("Starting Data Transformation")

            df = pd.read_csv(raw_path)

            df['genres'] = df['genres'].apply(self.convert)
            df['keywords'] = df['keywords'].apply(self.convert)
            df['cast'] = df['cast'].apply(
                lambda x: [i['name'] for i in ast.literal_eval(x)[:3]]
            )
            df['crew'] = df['crew'].apply(self.fetch_director)

            for col in ['genres', 'keywords', 'cast', 'crew']:
                df[col] = df[col].apply(
                    lambda x: [i.replace(" ", "") for i in x]
                )

            df['overview'] = df['overview'].apply(
                lambda x: x.split() if isinstance(x, str) else []
            )

            df['tags'] = (
                df['overview']
                + df['genres']
                + df['keywords']
                + df['cast']
                + df['crew']
            )

            new_df = df[['movie_id', 'title', 'tags']].copy()

            new_df['tags'] = new_df['tags'].apply(
                lambda x: " ".join(x).lower()
            )

            new_df['tags'] = new_df['tags'].apply(
                lambda x: " ".join(self.ps.stem(word) for word in x.split())
            )

            # ✅ Save to artifacts folder as Excel
            artifacts_path = "Artifacts"
            os.makedirs(artifacts_path, exist_ok=True)

            excel_path = os.path.join(artifacts_path, "processed_data.xlsx")
            new_df.to_excel(excel_path, index=False)

            print(f"File saved successfully at: {excel_path}")
            logging.info("Data Transformation Completed Successfully")

            return new_df, excel_path

        except Exception as e:
            raise CustomException(e, sys)
