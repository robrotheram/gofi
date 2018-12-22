FROM ubuntu:18.04
RUN mkdir /app 
ADD . /app/
WORKDIR /app
RUN touch /.container
CMD ["/app/injester_test"]
