FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/straw-relay ./cmd/straw-relay && mkdir /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/straw-relay /straw-relay
COPY --from=build --chown=65532:65532 /out/data /data
ENV STRAW_RELAY_ADDR=:8080 STRAW_RELAY_DB=/data/straw-relay.db
VOLUME /data
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/straw-relay"]
CMD ["serve"]
