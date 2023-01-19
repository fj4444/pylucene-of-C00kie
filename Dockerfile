# FROM coady/pylucene:9.1.0
FROM fj44444/pylucene_for_c00kie:1.5

ENV MYSQL_PASSWORD=C00kie
ENV INDEXDIR=./index

WORKDIR /C00kie-pylucene

EXPOSE 8080

COPY . /C00kie-pylucene

RUN pip3 install -r requirements.txt

CMD ["/bin/bash", "./entrypoint.sh"]
