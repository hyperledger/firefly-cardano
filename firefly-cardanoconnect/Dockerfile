FROM firefly-cardano-builder AS builder

FROM alpine
WORKDIR /app
COPY --from=builder /app/target/release/firefly-cardanoconnect /app/
CMD ["./firefly-cardanoconnect"]
