# Parallel module for Nginx [![Build Status](https://travis-ci.org/kaltura/nginx-parallel-module.svg?branch=master)](https://travis-ci.org/kaltura/nginx-parallel-module)

Reads data from an upstream-backed nginx location by running several concurrent range requests.

## Installation

Add `--add-module` when configuring nginx:

    ./configure --add-module=$PATH_TO_PARALLEL_MODULE

## Configuration

### Directives

#### parallel
* **syntax**: `parallel uri_prefix`
* **default**: `none`
* **context**: `location`

Enables the parallel module on the enclosing location, the uri_prefix is added to the request URI 
to form the URI of the subrequests

#### parallel_fiber_count
* **syntax**: `parallel_fiber_count count`
* **default**: `8`
* **context**: `http`, `server`, `location`

Sets the maximum number of concurrent requests

#### parallel_min_chunk_size
* **syntax**: `parallel_min_chunk_size size`
* **default**: `128k`
* **context**: `http`, `server`, `location`

Sets the minimum size that is requested in a single range request, only the request for the last chunk
is allowed to return less than this value. This parameter also controls the size of the initial subrequests,
that are issued before the response size is known.

#### parallel_max_chunk_size
* **syntax**: `parallel_max_chunk_size size`
* **default**: `512k`
* **context**: `http`, `server`, `location`

Sets the maximum size that can be fetched in a single request

#### parallel_max_headers_size
* **syntax**: `parallel_max_headers_size size`
* **default**: `4k`
* **context**: `http`, `server`, `location`

Sets the size that should be allocated for holding the subrequests headers

#### parallel_max_buffer_count
* **syntax**: `parallel_max_buffer_count count`
* **default**: `16`
* **context**: `http`, `server`, `location`

Sets the maximum number of buffers that the module is allowed to allocate.
Once the limit is reached, the module will until data is flushed to the client before issuing
more subrequests.

#### parallel_consistency_check_etag
* **syntax**: `parallel_consistency_check_etag on/off`
* **default**: `on`
* **context**: `http`, `server`, `location`

Enables/disables ETag consistency validation, when enabled, the server will make sure all ETag headers
returned for subrequests are consistent and fail the request otherwise.

#### parallel_consistency_check_last_modified
* **syntax**: `parallel_consistency_check_last_modified on/off`
* **default**: `on`
* **context**: `http`, `server`, `location`

Enables/disables Last-Modified consistency validation, when enabled, the server will make sure all Last-Modified headers
returned for subrequests are consistent and fail the request otherwise.

### Sample configuration

http {

	upstream backend {
		server backendhost:80;
		keepalive 32;
	}

	server {

		location /parallel {
			parallel /proxy;
		}
		
		location /proxy/parallel/ {
			proxy_pass http://backend/;
			proxy_http_version 1.1;
			proxy_set_header Connection "";
			proxy_set_header Host $http_host;
			
			internal;
		}
	}
}

## Copyright & License

All code in this project is released under the [AGPLv3 license](http://www.gnu.org/licenses/agpl-3.0.html) unless a different license for a particular library is specified in the applicable library path. 

Copyright © Kaltura Inc. All rights reserved.
