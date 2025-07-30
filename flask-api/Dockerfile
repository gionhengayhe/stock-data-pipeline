from python:3.12-slim

WORKDIR ./

COPY requirements.txt .
RUN pip install -r requirements.txt && rm requirements.txt

COPY app.py .

EXPOSE 5000

CMD python app.py