FROM maven:3.5.2-jdk-8

ADD . /usr/src/river.rdf
ADD solo.sh /usr/bin/solo
RUN chmod +x /usr/bin/solo

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends apt-utils
RUN apt-get install -y cron > /dev/null
RUN apt-get install -y rsyslog > /dev/null

ADD crontab /var/spool/cron/crontabs/root
RUN chmod 0600 /var/spool/cron/crontabs/root
RUN touch /var/log/cron-ady.log

RUN service cron start
RUN service rsyslog start
RUN cd /usr/src/river.rdf && /usr/bin/mvn clean install

RUN printenv | grep -v "no_proxy" >> /etc/environment
RUN cd /usr/src/river.rdf && /usr/bin/mvn compile
#RUN sed -i '/#cron./c\cron.*                          \/proc\/1\/fd\/1'  /etc/rsyslog.conf \
 #    && sed -i '/#$ModLoad imudp/c\$ModLoad imudp'  /etc/rsyslog.conf \
 #    && sed -i '/#$UDPServerRun/c\$UDPServerRun 514'  /etc/rsyslog.conf \
 #    && sed -i '/$UDPServerRun 514/a $UDPServerAddress 127.0.0.1' /etc/rsyslog.conf \
 #    && sed -i '/cron.*/a local2.*                          \/proc\/1\/fd\/1' /etc/rsyslog.conf

#CMD service cron start && service rsyslog start


#CMD cd /usr/src/river.rdf &&  mvn clean && mvn compile && mvn exec:java -Dexec.mainClass="org.elasticsearch.app.Indexer"