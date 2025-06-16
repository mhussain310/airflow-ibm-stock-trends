FROM python:3.13-slim

WORKDIR /opt

COPY config ./config
COPY sql ./sql
COPY utils ./utils
COPY README.md .
COPY streamlit-app/requirements.txt .
COPY streamlit-app/app.py .

RUN pip install --no-cache-dir -r requirements.txt


EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]