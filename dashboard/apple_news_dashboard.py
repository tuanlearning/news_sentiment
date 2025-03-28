import streamlit as st
from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd
import datetime

PROJECT_ID = "august-sandbox-425102-m1"
DATASET_ID = "news"
TABLE_ID = "apple_news"
COLUMNS = ['published_date','title','source_name','author','url','sentiment']

# st.text('Hello World')
def init_bq_client():
    load_dotenv()
    bq_client = bigquery.Client()
    return bq_client

def query_apple_news(bq_client, PROJECT_ID, DATASET_ID, TABLE_ID):
    query = f"""select 
            date(published_date) published_date,
            title,
            source_name,
            author,
            url,
            sentiment
            from `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            """
    result = bq_client.query(query).result()
    data = [list(row) for row in result]
    return data

def init_dataframe(columns, data):
    df = pd.DataFrame(columns = columns)
    df = pd.DataFrame(data, columns = columns)
    return df

def aggregate_dataframe(df):
    aggregated_df = df.groupby(['published_date', 'sentiment']).size().reset_index(name='count')
    return aggregated_df

def pivot_dataframe(aggregated_df):
    # Pivot the DataFrame so that each sentiment becomes a separate column
    pivot_df = aggregated_df.pivot(index="published_date", columns="sentiment", values="count").fillna(0)
    return pivot_df

def main():
    bq_client = init_bq_client()
    data = query_apple_news(bq_client, PROJECT_ID, DATASET_ID, TABLE_ID)
    df = init_dataframe(columns = COLUMNS, data = data)
    aggregated_df = aggregate_dataframe(df)
    pivot_df = pivot_dataframe(aggregated_df)

    st.subheader("Sentiment Trend Over Time")
    st.line_chart(pivot_df)
    st.subheader("Details")
    st.dataframe(df)


if __name__ == '__main__':
    main()