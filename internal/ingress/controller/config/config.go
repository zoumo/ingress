/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"fmt"
	"runtime"
	"strconv"

	"github.com/golang/glog"

	apiv1 "k8s.io/api/core/v1"

	"k8s.io/ingress-nginx/internal/ingress"
	"k8s.io/ingress-nginx/internal/ingress/defaults"
)

const (
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#client_max_body_size
	// Sets the maximum allowed size of the client request body
	bodySize = "1m"

	// http://nginx.org/en/docs/ngx_core_module.html#error_log
	// Configures logging level [debug | info | notice | warn | error | crit | alert | emerg]
	// Log levels above are listed in the order of increasing severity
	errorLevel = "notice"

	// HTTP Strict Transport Security (often abbreviated as HSTS) is a security feature (HTTP header)
	// that tell browsers that it should only be communicated with using HTTPS, instead of using HTTP.
	// https://developer.mozilla.org/en-US/docs/Web/Security/HTTP_strict_transport_security
	// max-age is the time, in seconds, that the browser should remember that this site is only to be accessed using HTTPS.
	hstsMaxAge = "15724800"

	gzipTypes = "application/atom+xml application/javascript application/x-javascript application/json application/rss+xml application/vnd.ms-fontobject application/x-font-ttf application/x-web-app-manifest+json application/xhtml+xml application/xml font/opentype image/svg+xml image/x-icon text/css text/plain text/x-component"

	brotliTypes = "application/xml+rss application/atom+xml application/javascript application/x-javascript application/json application/rss+xml application/vnd.ms-fontobject application/x-font-ttf application/x-web-app-manifest+json application/xhtml+xml application/xml font/opentype image/svg+xml image/x-icon text/css text/plain text/x-component"

	logFormatUpstream = `%v - [$the_real_ip] - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent" $request_length $request_time [$proxy_upstream_name] $upstream_addr $upstream_response_length $upstream_response_time $upstream_status`

	logFormatStream = `[$time_local] $protocol $status $bytes_sent $bytes_received $session_time`

	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_buffer_size
	// Sets the size of the buffer used for sending data.
	// 4k helps NGINX to improve TLS Time To First Byte (TTTFB)
	// https://www.igvita.com/2013/12/16/optimizing-nginx-tls-time-to-first-byte/
	sslBufferSize = "4k"

	// Enabled ciphers list to enabled. The ciphers are specified in the format understood by the OpenSSL library
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_ciphers
	sslCiphers = "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-ECDSA-AES128-SHA256:ECDHE-RSA-AES128-SHA256"

	// SSL enabled protocols to use
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_protocols
	sslProtocols = "TLSv1.2"

	// Time during which a client may reuse the session parameters stored in a cache.
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_session_timeout
	sslSessionTimeout = "10m"

	// Size of the SSL shared cache between all worker processes.
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_session_cache
	sslSessionCacheSize = "10m"

	// Default setting for load balancer algorithm
	defaultLoadBalancerAlgorithm = "least_conn"

	// Parameters for a shared memory zone that will keep states for various keys.
	// http://nginx.org/en/docs/http/ngx_http_limit_conn_module.html#limit_conn_zone
	defaultLimitConnZoneVariable = "$binary_remote_addr"
)

// Configuration represents the content of nginx.conf file
type Configuration struct {
	defaults.Backend `json:",squash"`

	// Sets the name of the configmap that contains the headers to pass to the client
	AddHeaders string `json:"add-headers,omitempty"`

	// AllowBackendServerHeader enables the return of the header Server from the backend
	// instead of the generic nginx string.
	// http://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_hide_header
	// By default this is disabled
	AllowBackendServerHeader bool `json:"allow-backend-server-header"`

	// AccessLogPath sets the path of the access logs if enabled
	// http://nginx.org/en/docs/http/ngx_http_log_module.html#access_log
	// By default access logs go to /var/log/nginx/access.log
	AccessLogPath string `json:"access-log-path,omitempty"`

	// WorkerCpuAffinity bind nginx worker processes to CPUs this will improve response latency
	// http://nginx.org/en/docs/ngx_core_module.html#worker_cpu_affinity
	// By default this is disabled
	WorkerCpuAffinity string `json:"worker-cpu-affinity,omitempty"`
	// ErrorLogPath sets the path of the error logs
	// http://nginx.org/en/docs/ngx_core_module.html#error_log
	// By default error logs go to /var/log/nginx/error.log
	ErrorLogPath string `json:"error-log-path,omitempty"`

	// EnableDynamicTLSRecords enables dynamic TLS record sizes
	// https://blog.cloudflare.com/optimizing-tls-over-tcp-to-reduce-latency
	// By default this is enabled
	EnableDynamicTLSRecords bool `json:"enable-dynamic-tls-records"`

	// EnableModsecurity enables the modsecurity module for NGINX
	// By default this is disabled
	EnableModsecurity bool `json:"enable-modsecurity"`

	// EnableModsecurity enables the OWASP ModSecurity Core Rule Set (CRS)
	// By default this is disabled
	EnableOWASPCoreRules bool `json:"enable-owasp-modsecurity-crs"`

	// ClientHeaderBufferSize allows to configure a custom buffer
	// size for reading client request header
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#client_header_buffer_size
	ClientHeaderBufferSize string `json:"client-header-buffer-size"`

	// Defines a timeout for reading client request header, in seconds
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#client_header_timeout
	ClientHeaderTimeout int `json:"client-header-timeout,omitempty"`

	// Sets buffer size for reading client request body
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#client_body_buffer_size
	ClientBodyBufferSize string `json:"client-body-buffer-size,omitempty"`

	// Defines a timeout for reading client request body, in seconds
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#client_body_timeout
	ClientBodyTimeout int `json:"client-body-timeout,omitempty"`

	// DisableAccessLog disables the Access Log globally from NGINX ingress controller
	//http://nginx.org/en/docs/http/ngx_http_log_module.html
	DisableAccessLog bool `json:"disable-access-log,omitempty"`

	// DisableIpv6DNS disables IPv6 for nginx resolver
	DisableIpv6DNS bool `json:"disable-ipv6-dns"`

	// DisableIpv6 disable listening on ipv6 address
	DisableIpv6 bool `json:"disable-ipv6,omitempty"`

	// EnableUnderscoresInHeaders enables underscores in header names
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#underscores_in_headers
	// By default this is disabled
	EnableUnderscoresInHeaders bool `json:"enable-underscores-in-headers"`

	// IgnoreInvalidHeaders set if header fields with invalid names should be ignored
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#ignore_invalid_headers
	// By default this is enabled
	IgnoreInvalidHeaders bool `json:"ignore-invalid-headers"`

	// EnableVtsStatus allows the replacement of the default status page with a third party module named
	// nginx-module-vts - https://github.com/vozlt/nginx-module-vts
	// By default this is disabled
	EnableVtsStatus bool `json:"enable-vts-status,omitempty"`

	// Vts config on http level
	// Description: Sets parameters for a shared memory zone that will keep states for various keys. The cache is shared between all worker processe
	// https://github.com/vozlt/nginx-module-vts#vhost_traffic_status_zone
	// Default value is 10m
	VtsStatusZoneSize string `json:"vts-status-zone-size,omitempty"`

	// Vts config on http level
	// Description: Enables the keys by user defined variable. The key is a key string to calculate traffic.
	// The name is a group string to calculate traffic. The key and name can contain variables such as $host,
	// $server_name. The name's group belongs to filterZones if specified. The key's group belongs to serverZones
	// if not specified second argument name. The example with geoip module is as follows:
	// https://github.com/vozlt/nginx-module-vts#vhost_traffic_status_filter_by_set_key
	// Default value is $geoip_country_code country::*
	VtsDefaultFilterKey string `json:"vts-default-filter-key,omitempty"`

	// RetryNonIdempotent since 1.9.13 NGINX will not retry non-idempotent requests (POST, LOCK, PATCH)
	// in case of an error. The previous behavior can be restored using the value true
	RetryNonIdempotent bool `json:"retry-non-idempotent"`

	// http://nginx.org/en/docs/ngx_core_module.html#error_log
	// Configures logging level [debug | info | notice | warn | error | crit | alert | emerg]
	// Log levels above are listed in the order of increasing severity
	ErrorLogLevel string `json:"error-log-level,omitempty"`

	// https://nginx.org/en/docs/http/ngx_http_v2_module.html#http2_max_field_size
	// HTTP2MaxFieldSize Limits the maximum size of an HPACK-compressed request header field
	HTTP2MaxFieldSize string `json:"http2-max-field-size,omitempty"`

	// https://nginx.org/en/docs/http/ngx_http_v2_module.html#http2_max_header_size
	// HTTP2MaxHeaderSize Limits the maximum size of the entire request header list after HPACK decompression
	HTTP2MaxHeaderSize string `json:"http2-max-header-size,omitempty"`

	// Enables or disables the header HSTS in servers running SSL
	HSTS bool `json:"hsts,omitempty"`

	// Enables or disables the use of HSTS in all the subdomains of the servername
	// Default: true
	HSTSIncludeSubdomains bool `json:"hsts-include-subdomains,omitempty"`

	// HTTP Strict Transport Security (often abbreviated as HSTS) is a security feature (HTTP header)
	// that tell browsers that it should only be communicated with using HTTPS, instead of using HTTP.
	// https://developer.mozilla.org/en-US/docs/Web/Security/HTTP_strict_transport_security
	// max-age is the time, in seconds, that the browser should remember that this site is only to be
	// accessed using HTTPS.
	HSTSMaxAge string `json:"hsts-max-age,omitempty"`

	// Enables or disables the preload attribute in HSTS feature
	HSTSPreload bool `json:"hsts-preload,omitempty"`

	// Time during which a keep-alive client connection will stay open on the server side.
	// The zero value disables keep-alive client connections
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#keepalive_timeout
	KeepAlive int `json:"keep-alive,omitempty"`

	// Sets the maximum number of requests that can be served through one keep-alive connection.
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#keepalive_requests
	KeepAliveRequests int `json:"keep-alive-requests,omitempty"`

	// LargeClientHeaderBuffers Sets the maximum number and size of buffers used for reading
	// large client request header.
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#large_client_header_buffers
	// Default: 4 8k
	LargeClientHeaderBuffers string `json:"large-client-header-buffers"`

	// Enable json escaping
	// http://nginx.org/en/docs/http/ngx_http_log_module.html#log_format
	LogFormatEscapeJSON bool `json:"log-format-escape-json,omitempty"`

	// Customize upstream log_format
	// http://nginx.org/en/docs/http/ngx_http_log_module.html#log_format
	LogFormatUpstream string `json:"log-format-upstream,omitempty"`

	// Customize stream log_format
	// http://nginx.org/en/docs/http/ngx_http_log_module.html#log_format
	LogFormatStream string `json:"log-format-stream,omitempty"`

	// Maximum number of simultaneous connections that can be opened by each worker process
	// http://nginx.org/en/docs/ngx_core_module.html#worker_connections
	MaxWorkerConnections int `json:"max-worker-connections,omitempty"`

	// Sets the bucket size for the map variables hash tables.
	// Default value depends on the processor’s cache line size.
	// http://nginx.org/en/docs/http/ngx_http_map_module.html#map_hash_bucket_size
	MapHashBucketSize int `json:"map-hash-bucket-size,omitempty"`

	// If UseProxyProtocol is enabled ProxyRealIPCIDR defines the default the IP/network address
	// of your external load balancer
	ProxyRealIPCIDR []string `json:"proxy-real-ip-cidr,omitempty"`

	// Sets the name of the configmap that contains the headers to pass to the backend
	ProxySetHeaders string `json:"proxy-set-headers,omitempty"`

	// Maximum size of the server names hash tables used in server names, map directive’s values,
	// MIME types, names of request header strings, etcd.
	// http://nginx.org/en/docs/hash.html
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#server_names_hash_max_size
	ServerNameHashMaxSize int `json:"server-name-hash-max-size,omitempty"`

	// Size of the bucket for the server names hash tables
	// http://nginx.org/en/docs/hash.html
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#server_names_hash_bucket_size
	ServerNameHashBucketSize int `json:"server-name-hash-bucket-size,omitempty"`

	// Size of the bucket for the proxy headers hash tables
	// http://nginx.org/en/docs/hash.html
	// https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_headers_hash_max_size
	ProxyHeadersHashMaxSize int `json:"proxy-headers-hash-max-size,omitempty"`

	// Maximum size of the bucket for the proxy headers hash tables
	// http://nginx.org/en/docs/hash.html
	// https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_headers_hash_bucket_size
	ProxyHeadersHashBucketSize int `json:"proxy-headers-hash-bucket-size,omitempty"`

	// Enables or disables emitting nginx version in error messages and in the “Server” response header field.
	// http://nginx.org/en/docs/http/ngx_http_core_module.html#server_tokens
	// Default: true
	ShowServerTokens bool `json:"server-tokens"`

	// Enabled ciphers list to enabled. The ciphers are specified in the format understood by
	// the OpenSSL library
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_ciphers
	SSLCiphers string `json:"ssl-ciphers,omitempty"`

	// Specifies a curve for ECDHE ciphers.
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_ecdh_curve
	SSLECDHCurve string `json:"ssl-ecdh-curve,omitempty"`

	// The secret that contains Diffie-Hellman key to help with "Perfect Forward Secrecy"
	// https://wiki.openssl.org/index.php/Manual:Dhparam(1)
	// https://wiki.mozilla.org/Security/Server_Side_TLS#DHE_handshake_and_dhparam
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_dhparam
	SSLDHParam string `json:"ssl-dh-param,omitempty"`

	// SSL enabled protocols to use
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_protocols
	SSLProtocols string `json:"ssl-protocols,omitempty"`

	// Enables or disables the use of shared SSL cache among worker processes.
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_session_cache
	SSLSessionCache bool `json:"ssl-session-cache,omitempty"`

	// Size of the SSL shared cache between all worker processes.
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_session_cache
	SSLSessionCacheSize string `json:"ssl-session-cache-size,omitempty"`

	// Enables or disables session resumption through TLS session tickets.
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_session_tickets
	SSLSessionTickets bool `json:"ssl-session-tickets,omitempty"`

	// Sets the secret key used to encrypt and decrypt TLS session tickets.
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_session_tickets
	// By default, a randomly generated key is used.
	// Example: openssl rand 80 | base64 -w0
	SSLSessionTicketKey string `json:"ssl-session-ticket-key,omitempty"`

	// Time during which a client may reuse the session parameters stored in a cache.
	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_session_timeout
	SSLSessionTimeout string `json:"ssl-session-timeout,omitempty"`

	// http://nginx.org/en/docs/http/ngx_http_ssl_module.html#ssl_buffer_size
	// Sets the size of the buffer used for sending data.
	// 4k helps NGINX to improve TLS Time To First Byte (TTTFB)
	// https://www.igvita.com/2013/12/16/optimizing-nginx-tls-time-to-first-byte/
	SSLBufferSize string `json:"ssl-buffer-size,omitempty"`

	// Enables or disables the use of the PROXY protocol to receive client connection
	// (real IP address) information passed through proxy servers and load balancers
	// such as HAproxy and Amazon Elastic Load Balancer (ELB).
	// https://www.nginx.com/resources/admin-guide/proxy-protocol/
	UseProxyProtocol bool `json:"use-proxy-protocol,omitempty"`

	// Enables or disables the use of the nginx module that compresses responses using the "gzip" method
	// http://nginx.org/en/docs/http/ngx_http_gzip_module.html
	UseGzip bool `json:"use-gzip,omitempty"`

	// Enables or disables the use of the nginx geoip module that creates variables with values depending on the client IP
	// http://nginx.org/en/docs/http/ngx_http_geoip_module.html
	UseGeoIP bool `json:"use-geoip,omitempty"`

	// Enables or disables the use of the NGINX Brotli Module for compression
	// https://github.com/google/ngx_brotli
	EnableBrotli bool `json:"enable-brotli,omitempty"`

	// Brotli Compression Level that will be used
	BrotliLevel int `json:"brotli-level,omitempty"`

	// MIME Types that will be compressed on-the-fly using Brotli module
	BrotliTypes string `json:"brotli-types,omitempty"`

	// Enables or disables the HTTP/2 support in secure connections
	// http://nginx.org/en/docs/http/ngx_http_v2_module.html
	// Default: true
	UseHTTP2 bool `json:"use-http2,omitempty"`

	// MIME types in addition to "text/html" to compress. The special value “*” matches any MIME type.
	// Responses with the “text/html” type are always compressed if UseGzip is enabled
	GzipTypes string `json:"gzip-types,omitempty"`

	// Defines the number of worker processes. By default auto means number of available CPU cores
	// http://nginx.org/en/docs/ngx_core_module.html#worker_processes
	WorkerProcesses string `json:"worker-processes,omitempty"`

	// Defines a timeout for a graceful shutdown of worker processes
	// http://nginx.org/en/docs/ngx_core_module.html#worker_shutdown_timeout
	WorkerShutdownTimeout string `json:"worker-shutdown-timeout,omitempty"`

	// Defines the load balancing algorithm to use. The deault is round-robin
	LoadBalanceAlgorithm string `json:"load-balance,omitempty"`

	// Sets the bucket size for the variables hash table.
	// http://nginx.org/en/docs/http/ngx_http_map_module.html#variables_hash_bucket_size
	VariablesHashBucketSize int `json:"variables-hash-bucket-size,omitempty"`

	// Sets the maximum size of the variables hash table.
	// http://nginx.org/en/docs/http/ngx_http_map_module.html#variables_hash_max_size
	VariablesHashMaxSize int `json:"variables-hash-max-size,omitempty"`

	// Activates the cache for connections to upstream servers.
	// The connections parameter sets the maximum number of idle keepalive connections to
	// upstream servers that are preserved in the cache of each worker process. When this
	// number is exceeded, the least recently used connections are closed.
	// http://nginx.org/en/docs/http/ngx_http_upstream_module.html#keepalive
	// Default: 32
	UpstreamKeepaliveConnections int `json:"upstream-keepalive-connections,omitempty"`

	// Sets the maximum size of the variables hash table.
	// http://nginx.org/en/docs/http/ngx_http_map_module.html#variables_hash_max_size
	LimitConnZoneVariable string `json:"limit-conn-zone-variable,omitempty"`

	// Sets the timeout between two successive read or write operations on client or proxied server connections.
	// If no data is transmitted within this time, the connection is closed.
	// http://nginx.org/en/docs/stream/ngx_stream_proxy_module.html#proxy_timeout
	ProxyStreamTimeout string `json:"proxy-stream-timeout,omitempty"`

	// Sets the number of datagrams expected from the proxied server in response
	// to the client request if the UDP protocol is used.
	// http://nginx.org/en/docs/stream/ngx_stream_proxy_module.html#proxy_responses
	// Default: 1
	ProxyStreamResponses int `json:"proxy-stream-responses,omitempty"`

	// Sets the ipv4 addresses on which the server will accept requests.
	BindAddressIpv4 []string `json:"bind-address-ipv4,omitempty"`

	// Sets the ipv6 addresses on which the server will accept requests.
	BindAddressIpv6 []string `json:"bind-address-ipv6,omitempty"`

	// Sets the header field for identifying the originating IP address of a client
	// Default is X-Forwarded-For
	ForwardedForHeader string `json:"forwarded-for-header,omitempty"`

	// Append the remote address to the X-Forwarded-For header instead of replacing it
	// Default: false
	ComputeFullForwardedFor bool `json:"compute-full-forwarded-for,omitempty"`

	// EnableOpentracing enables the nginx Opentracing extension
	// https://github.com/rnburn/nginx-opentracing
	// By default this is disabled
	EnableOpentracing bool `json:"enable-opentracing"`

	// ZipkinCollectorHost specifies the host to use when uploading traces
	ZipkinCollectorHost string `json:"zipkin-collector-host"`

	// ZipkinCollectorPort specifies the port to use when uploading traces
	ZipkinCollectorPort int `json:"zipkin-collector-port"`

	// ZipkinServiceName specifies the service name to use for any traces created
	// Default: nginx
	ZipkinServiceName string `json:"zipkin-service-name"`

	// JaegerCollectorHost specifies the host to use when uploading traces
	JaegerCollectorHost string `json:"jaeger-collector-host"`

	// JaegerCollectorPort specifies the port to use when uploading traces
	JaegerCollectorPort int `json:"jaeger-collector-port"`

	// JaegerServiceName specifies the service name to use for any traces created
	// Default: nginx
	JaegerServiceName string `json:"jaeger-service-name"`

	// JaegerSamplerType specifies the sampler to be used when sampling traces.
	// The available samplers are: const, probabilistic, ratelimiting, remote
	// Default: const
	JaegerSamplerType string `json:"jaeger-sampler-type"`

	// JaegerSamplerParam specifies the argument to be passed to the sampler constructor
	// Default: 1
	JaegerSamplerParam string `json:"jaeger-sampler-param"`

	// HTTPSnippet adds custom configuration to the http section of the nginx configuration
	HTTPSnippet string `json:"http-snippet"`

	// ServerSnippet adds custom configuration to all the servers in the nginx configuration
	ServerSnippet string `json:"server-snippet"`

	// LocationSnippet adds custom configuration to all the locations in the nginx configuration
	LocationSnippet string `json:"location-snippet"`

	// HTTPRedirectCode sets the HTTP status code to be used in redirects.
	// Supported codes are 301,302,307 and 308
	// Default: 308
	HTTPRedirectCode int `json:"http-redirect-code"`

	// ReusePort instructs NGINX to create an individual listening socket for
	// each worker process (using the SO_REUSEPORT socket option), allowing a
	// kernel to distribute incoming connections between worker processes
	// Default: false
	// Reason for the default: https://trac.nginx.org/nginx/ticket/1300
	ReusePort bool `json:"reuse-port"`

	// HideHeaders sets additional header that will not be passed from the upstream
	// server to the client response
	// Default: empty
	HideHeaders []string `json:"hide-headers"`

	// LimitReqStatusCode Sets the status code to return in response to rejected requests.
	// http://nginx.org/en/docs/http/ngx_http_limit_req_module.html#limit_req_status
	// Default: 503
	LimitReqStatusCode int `json:"limit-req-status-code"`

	// EnableSyslog enables the configuration for remote logging in NGINX
	EnableSyslog bool `json:"enable-syslog"`
	// SyslogHost FQDN or IP address where the logs should be sent
	SyslogHost string `json:"syslog-host"`
	// SyslogPort port
	SyslogPort int `json:"syslog-port",omitempty`

	// NoTLSRedirectLocations is a comma-separated list of locations
	// that should not get redirected to TLS
	NoTLSRedirectLocations string `json:"no-tls-redirect-locations"`
}

// NewDefault returns the default nginx configuration
func NewDefault() Configuration {
	defIPCIDR := make([]string, 0)
	defIPCIDR = append(defIPCIDR, "0.0.0.0/0")
	defBindAddress := make([]string, 0)
	cfg := Configuration{
		AllowBackendServerHeader:   false,
		AccessLogPath:              "/var/log/nginx/access.log",
		WorkerCpuAffinity:          "",
		ErrorLogPath:               "/var/log/nginx/error.log",
		BrotliLevel:                4,
		BrotliTypes:                brotliTypes,
		ClientHeaderBufferSize:     "1k",
		ClientHeaderTimeout:        60,
		ClientBodyBufferSize:       "8k",
		ClientBodyTimeout:          60,
		EnableDynamicTLSRecords:    true,
		EnableUnderscoresInHeaders: false,
		ErrorLogLevel:              errorLevel,
		ForwardedForHeader:         "X-Forwarded-For",
		ComputeFullForwardedFor:    false,
		HTTP2MaxFieldSize:          "4k",
		HTTP2MaxHeaderSize:         "16k",
		HTTPRedirectCode:           308,
		HSTS:                       true,
		HSTSIncludeSubdomains:      true,
		HSTSMaxAge:                 hstsMaxAge,
		HSTSPreload:                false,
		IgnoreInvalidHeaders:       true,
		GzipTypes:                  gzipTypes,
		KeepAlive:                  75,
		KeepAliveRequests:          100,
		LargeClientHeaderBuffers:   "4 8k",
		LogFormatEscapeJSON:        false,
		LogFormatStream:            logFormatStream,
		LogFormatUpstream:          logFormatUpstream,
		MaxWorkerConnections:       16384,
		MapHashBucketSize:          64,
		ProxyRealIPCIDR:            defIPCIDR,
		ServerNameHashMaxSize:      1024,
		ProxyHeadersHashMaxSize:    512,
		ProxyHeadersHashBucketSize: 64,
		ProxyStreamResponses:       0,
		ShowServerTokens:           true,
		SSLBufferSize:              sslBufferSize,
		SSLCiphers:                 sslCiphers,
		SSLECDHCurve:               "auto",
		SSLProtocols:               sslProtocols,
		SSLSessionCache:            true,
		SSLSessionCacheSize:        sslSessionCacheSize,
		SSLSessionTickets:          true,
		SSLSessionTimeout:          sslSessionTimeout,
		EnableBrotli:               false,
		UseGzip:                    true,
		UseGeoIP:                   true,
		WorkerProcesses:            strconv.Itoa(runtime.NumCPU()),
		WorkerShutdownTimeout:      "10s",
		LoadBalanceAlgorithm:       defaultLoadBalancerAlgorithm,
		VtsStatusZoneSize:          "10m",
		VtsDefaultFilterKey:        "$geoip_country_code country::*",
		VariablesHashBucketSize:    128,
		VariablesHashMaxSize:       2048,
		UseHTTP2:                   true,
		ProxyStreamTimeout:         "600s",
		Backend: defaults.Backend{
			ProxyBodySize:         bodySize,
			ProxyConnectTimeout:   5,
			ProxyReadTimeout:      60,
			ProxySendTimeout:      60,
			ProxyBufferSize:       "4k",
			ProxyCookieDomain:     "off",
			ProxyCookiePath:       "off",
			ProxyNextUpstream:     "error timeout invalid_header http_502 http_503 http_504",
			ProxyRequestBuffering: "on",
			ProxyRedirectFrom:     "off",
			SSLRedirect:           true,
			CustomHTTPErrors:      []int{},
			WhitelistSourceRange:  []string{},
			SkipAccessLogURLs:     []string{},
			LimitRate:             0,
			LimitRateAfter:        0,
			ProxyBuffering:        "off",
		},
		UpstreamKeepaliveConnections: 32,
		LimitConnZoneVariable:        defaultLimitConnZoneVariable,
		BindAddressIpv4:              defBindAddress,
		BindAddressIpv6:              defBindAddress,
		ZipkinCollectorPort:          9411,
		ZipkinServiceName:            "nginx",
		JaegerCollectorPort:          6831,
		JaegerServiceName:            "nginx",
		JaegerSamplerType:            "const",
		JaegerSamplerParam:           "1",
		LimitReqStatusCode:           503,
		SyslogPort:                   514,
		NoTLSRedirectLocations:       "/.well-known/acme-challenge",
	}

	if glog.V(5) {
		cfg.ErrorLogLevel = "debug"
	}

	return cfg
}

// BuildLogFormatUpstream format the log_format upstream using
// proxy_protocol_addr as remote client address if UseProxyProtocol
// is enabled.
func (cfg Configuration) BuildLogFormatUpstream() string {
	if cfg.LogFormatUpstream == logFormatUpstream {
		return fmt.Sprintf(cfg.LogFormatUpstream, "$the_real_ip")
	}

	return cfg.LogFormatUpstream
}

// TemplateConfig contains the nginx configuration to render the file nginx.conf
type TemplateConfig struct {
	ProxySetHeaders             map[string]string
	AddHeaders                  map[string]string
	MaxOpenFiles                int
	BacklogSize                 int
	Backends                    []*ingress.Backend
	PassthroughBackends         []*ingress.SSLPassthroughBackend
	Servers                     []*ingress.Server
	TCPBackends                 []ingress.L4Service
	UDPBackends                 []ingress.L4Service
	HealthzURI                  string
	CustomErrors                bool
	Cfg                         Configuration
	IsIPV6Enabled               bool
	IsSSLPassthroughEnabled     bool
	RedirectServers             map[string]string
	ListenPorts                 *ListenPorts
	PublishService              *apiv1.Service
	DynamicConfigurationEnabled bool
}

// ListenPorts describe the ports required to run the
// NGINX Ingress controller
type ListenPorts struct {
	HTTP     int
	HTTPS    int
	Status   int
	Health   int
	Default  int
	SSLProxy int
}
