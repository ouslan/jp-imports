FROM python:3.12-alpine

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY data/ data/

COPY Dash.py .

EXPOSE 8080

CMD ["python", "Dash.py"]