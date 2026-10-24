FROM rust:1.99-slim-trixie AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked

FROM gcr.io/distroless/cc-debian13:nonroot
LABEL org.opencontainers.image.source=https://github.com/mwpryer/cloud-tasks-emulator
COPY --from=build /src/target/release/cloud-tasks-emulator /usr/local/bin/
# Published ports and other containers reach only all-interfaces address
ENV LISTEN_ADDR=0.0.0.0:8123
EXPOSE 8123
ENTRYPOINT ["cloud-tasks-emulator"]
