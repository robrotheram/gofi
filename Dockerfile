FROM ubuntu:18.04
RUN mkdir /app
RUN mkdir /app/data
ADD package /app/
WORKDIR /app
RUN touch /.container
CMD ["/app/gofi"]
