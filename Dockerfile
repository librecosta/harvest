FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329
ARG TARGETARCH
COPY --chmod=0555 bin/linux/${TARGETARCH}/harvest /harvest
COPY LICENSE LICENSE.steam-go LICENSE.x-time /licenses/
USER 65532:65532
EXPOSE 8080
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=2 CMD ["/harvest", "healthcheck", "--config", "/etc/harvest/config.yaml"]
ENTRYPOINT ["/harvest"]
CMD ["serve", "--config", "/etc/harvest/config.yaml"]
