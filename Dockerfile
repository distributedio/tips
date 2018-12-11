#Builder image
FROM golang:1.10.1-alpine as builder

RUN apk add --no-cache \
    make \
    git

COPY . /go/src/github.com/tipsio/tips

WORKDIR /go/src/github.com/tipsio/tips/tipsd

RUN env GOOS=linux CGO_ENABLED=0 go build

# Executable image
#FROM debian:stretch-slim
FROM scratch

COPY --from=builder /go/src/github.com/tipsio/tips/tipsd/tipsd /tips/bin/tipsd
COPY --from=builder /go/src/github.com/tipsio/tips/conf/tips.toml /tips/conf/tips.toml

WORKDIR /tips

EXPOSE 7369

ENTRYPOINT ["./bin/tipsd"]


