FROM python:3.7.17

WORKDIR /opt/news_sentiment

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/apple_news_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]