FROM python:3.10
EXPOSE 8000
COPY . /app/
WORKDIR /app
RUN pip install --upgrade pip \
     && pip install -r requirements.txt
RUN ["python", "manage.py", "collectstatic"]
CMD ["uwsgi", "--ini", "uwsgi.ini"]
