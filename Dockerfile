FROM rust:1.94.0-alpine3.23 AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache openssl-dev=3.5.6-r0 openssl-libs-static=3.5.6-r0

# Copy manifest and source files
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build the application
RUN cargo build --release

FROM alpine:3.23 AS runtime

RUN apk --no-cache upgrade

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/nox-observer .

# Run the application
ENTRYPOINT ["/app/nox-observer"]
