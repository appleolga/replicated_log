FROM python:3.8-alpine
WORKDIR /usr/src/app

ADD requirements.txt .

RUN pip install --trusted-host pypi.python.org --no-cache-dir --upgrade -r requirements.txt

#COPY . ./

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3000"]
#CMD ["uvicorn", "main:app", "--reload", "--host", ]

#ENV APP_MODULE="master"