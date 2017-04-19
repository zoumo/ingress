all: push

BUILDTAGS=

# Use the 0.0 tag for testing, it shouldn't clobber any release builds
RELEASE?=v0.1.0
PREFIX?=cargo.caicloud.io/caicloud/ingress-aliyun
GOOS?=linux
DOCKER?=docker

REPO_INFO=$(shell git config --get remote.origin.url)

ifndef COMMIT
  COMMIT := git-$(shell git rev-parse --short HEAD)
endif

PKG=k8s.io/ingress/controllers/aliyun

build: clean
	CGO_ENABLED=0 GOOS=${GOOS} go build -a -installsuffix cgo \
		-o aliyun-ingress-controller ${PKG}

container: build
	$(DOCKER) build --pull -t $(PREFIX):$(RELEASE) .

push: container
	$(DOCKER) push $(PREFIX):$(RELEASE)

clean:
	rm -f nginx-ingress-controller
