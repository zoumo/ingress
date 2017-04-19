FROM cargo.caicloud.io/caicloud/debian:jessie

# Set the default timezone to Shanghai
RUN echo "Asia/Shanghai" > /etc/timezone
RUN dpkg-reconfigure -f noninteractive tzdata

RUN apt-get update && apt-get install -y --no-install-recommends \
  ca-certificates \
  bash && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

COPY aliyun-ingress-controller /
CMD ["./aliyun-ingress-controller"]
