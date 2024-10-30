FROM mysql:8.0.40

ENV MYSQL_USER=my_test
ENV MYSQL_PASSWORD=password
ENV MYSQL_DATABASE=test
ENV MYSQL_ROOT_PASSWORD=rootpassword

CMD ["mysqld", "--default-authentication-plugin=mysql_native_password", "--local-infile=1"]

EXPOSE 3306
