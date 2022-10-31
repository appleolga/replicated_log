FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9
WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

#COPY . ./

#CMD ["uvicorn", "main:app", "--host", "127.0.0.1", "--port", "1208"]
CMD ["uvicorn", "main:app"]

#ENV APP_MODULE="master"