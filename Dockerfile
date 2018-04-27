FROM maven:3.5.3-jdk-10

ADD . /usr/src/river.rdf

ADD solo.sh /usr/bin/solo

RUN chmod +x /usr/bin/solo

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    ca-certificates \
    cron \
    rsyslog \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

ADD crontab /var/spool/cron/crontabs/root
RUN chmod 0600 /var/spool/cron/crontabs/root

RUN cd /usr/src/river.rdf && /usr/bin/mvn clean install

RUN printenv | grep -v "no_proxy" >> /etc/environment
RUN cd /usr/src/river.rdf && /usr/bin/mvn compile

RUN sed -i '/#cron./c\cron.*                          \/proc\/1\/fd\/1'  /etc/rsyslog.conf \
     && sed -i '/cron.*/a local2.*                          \/proc\/1\/fd\/1' /etc/rsyslog.conf

#RUN service cron start && service rsyslog start

#CMD cd /usr/src/river.rdf &&  mvn clean && mvn compile && mvn exec:java -Dexec.mainClass="org.elasticsearch.app.Indexer"

ENTRYPOINT sh /usr/src/river.rdf/startup.sh