FROM golang:1.26-alpine3.24 AS build
WORKDIR /src
COPY go.mod ./
COPY ember/ ./ember/
COPY internal/ ./internal/
COPY cmd/emberd/ ./cmd/emberd/
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -ldflags="-s -w" -o /out/emberd ./cmd/emberd \
    && mkdir -p /rootfs/data

FROM scratch
COPY --from=build /out/emberd /bin/emberd
COPY --from=build --chown=65532:65532 /rootfs/ /
WORKDIR /data
USER 65532:65532
EXPOSE 6380
ENTRYPOINT ["/bin/emberd"]
CMD ["-addr", "0.0.0.0:6380", "-dir", "/data"]
