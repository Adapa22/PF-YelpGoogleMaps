from fastapi import FastAPI
import pandas as pd
import numpy as np

app = FastAPI()

merged_df=pd.read_csv('merged_df.csv')

@app.get("/best_categories")
def get_best_categories(state: str):
    # Filtrar el dataframe por el estado dado
    state_df = merged_df[merged_df['state'] == state]

    # Calcular el promedio de sentimientos por categoría en cada ciudad
    state_df['sentiment_review'] = state_df['sentiment_review'].map({'positive': 1, 'neutral': 0, 'negative': -1})
    city_category_sentiment = state_df.groupby(['state', 'city', 'categories'])['sentiment_review'].mean().reset_index()

    # Ordenar las categorías por el promedio de sentimientos descendente en cada ciudad
    city_category_sentiment_sorted = city_category_sentiment.sort_values(['state', 'city', 'sentiment_review'], ascending=[True, True, False])
    city_category_sentiment_sorted.drop_duplicates(subset=['state', 'city'], keep='first', inplace=True)

    # Obtener las categorías con mejores reseñas en cada ciudad
    best_categories_by_state = city_category_sentiment_sorted[['city', 'categories']]

    return best_categories_by_state