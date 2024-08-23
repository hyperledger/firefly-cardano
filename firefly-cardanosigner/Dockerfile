FROM firefly-cardano-builder AS builder

FROM alpine
WORKDIR /app
COPY --from=builder /app/target/release/firefly-cardanosigner /app/
CMD ["./firefly-cardanosigner"]
