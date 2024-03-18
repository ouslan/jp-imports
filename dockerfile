FROM python:3.12-alpine

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY graphs.py .

COPY imports_data.pkl .

EXPOSE 8080

CMD ["python", "graphs.py"]