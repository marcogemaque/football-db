FROM python:3.9
WORKDIR /football_db
COPY . .
RUN pip3 install pipenv
RUN pipenv install --dev --deploy --system --python=`which python3`
CMD ["python3","scripts/app.py"]