FROM python:3.8-alpine
WORKDIR /usr/src/app

ADD requirements.txt .
COPY . /app

RUN pip install --trusted-host pypi.python.org --no-cache-dir --upgrade -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]