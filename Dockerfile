#FROM golang:onbuild


FROM golang:latest
RUN mkdir /app
ADD . /app/
WORKDIR /app/
#RUN go build -o main .
CMD ["/app/cmd/node/node"]

#Service listens on port 7575.
EXPOSE 7575
