FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /out/agent-workspace ./cmd/agent-workspace && \
    CGO_ENABLED=0 go build -trimpath -o /out/demo-agent ./cmd/demo-agent && \
    CGO_ENABLED=0 go build -trimpath -o /out/agent-runtime ./cmd/agent-runtime && \
    CGO_ENABLED=0 go build -trimpath -o /out/fake-llm ./cmd/fake-llm

FROM alpine:3.22 AS demo
RUN adduser -D -u 1000 agent && mkdir /workspace && chown agent /workspace
COPY --from=build /out/demo-agent /usr/local/bin/demo-agent
USER 1000:1000
EXPOSE 8080
ENTRYPOINT ["demo-agent"]

FROM alpine:3.22 AS agent
# CA certificates: the agent calls hosted LLM endpoints over HTTPS.
RUN apk add --no-cache ca-certificates && \
    adduser -D -u 1000 agent && mkdir /workspace && chown agent /workspace
ARG AGENT_VERSION=dev
ENV AGENT_VERSION=$AGENT_VERSION
COPY --from=build /out/agent-runtime /usr/local/bin/agent-runtime
USER 1000:1000
EXPOSE 8080
ENTRYPOINT ["agent-runtime"]

FROM alpine:3.22 AS fakellm
COPY --from=build /out/fake-llm /usr/local/bin/fake-llm
USER 1000:1000
EXPOSE 8081
ENTRYPOINT ["fake-llm"]

FROM alpine:3.22 AS controller
RUN apk add --no-cache ca-certificates && \
    adduser -D -u 1000 controller && mkdir /data && chown controller /data
COPY --from=build /out/agent-workspace /usr/local/bin/agent-workspace
COPY configs/profiles.json /etc/agent-workspace/profiles.json
USER 1000:1000
EXPOSE 8090
ENTRYPOINT ["agent-workspace"]
CMD ["-listen", "0.0.0.0:8090", "-data", "/data", "-profiles", "/etc/agent-workspace/profiles.json"]
