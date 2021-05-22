# Offer Cache

## Preparation

1. Create a virtualenv;
2. Install the requirements: `pip3 install -r requirements.txt`;
3. Get the [`final1.json.gz`][final1], [`final2.json.gz`][final2] files and
   save them in the `data/`;
4. If you want to import data from the RDB dump, get the
   [`routerank.rdb`][routerank-rdb] file and save it in the `data/`;

## Usage (standalone)

### Load and Export data

Start redis in a docker container:

```bash
docker run --rm --name cache -p 6379:6379 redis:latest
```

Load the data into redis:

**Note: for Windows unzip the files first because the decompression
does not work.**

```bash
python3 loader.py data/final1.json.gz data/final2.json.gz
```

You can check how many keys have been loaded:

```bash
$ docker run -it \
             --rm \
             --link cache:cache \
             -v $PWD/data:/data \
               redis redis-cli -h cache INFO Keyspace
# Keyspace
db0:keys=3050074,expires=0,avg_ttl=0
```

Export the data in RDB format:

```bash
docker run -it \
           --rm \
           --link cache:cache  \
           -v $PWD/data:/data  \
             redis redis-cli -h cache --rdb /data/routerank.rdb
sending REPLCONF capa eof
sending REPLCONF rdb-only 1
SYNC sent to master, writing 247701866 bytes to '/data/routerank.rdb'
Transfer finished with success.
```

## Import data

Note that:

* `docker run` is run without `--rm` this time, because we need to stop and
restart the container
* when copying the data the destinastion file __must__ be named `dump.rdb`

Load the `.rdb` file:

```bash
docker run --name cache -p 6379:6379 redis:latest
docker stop cache
docker cp data/routerank.rdb cache:/data/dump.rdb
```

Then you can restart the container:

```bash
$ docker start -a cache
1:C 22 May 2021 21:39:43.988 # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo
1:C 22 May 2021 21:39:43.988 # Redis version=6.2.1, bits=64, commit=00000000, modified=0, pid=1, just started
1:C 22 May 2021 21:39:43.988 # Warning: no config file specified, using the default config. In order to specify a config file use redis-server /path/to/redis.conf
1:M 22 May 2021 21:39:43.989 * monotonic clock: POSIX clock_gettime
1:M 22 May 2021 21:39:43.989 * Running mode=standalone, port=6379.
1:M 22 May 2021 21:39:43.989 # Server initialized
1:M 22 May 2021 21:39:43.990 * Loading RDB produced by version 6.2.1
1:M 22 May 2021 21:39:43.990 * RDB age 140 seconds
1:M 22 May 2021 21:39:43.990 * RDB memory usage when created 481.09 Mb
1:M 22 May 2021 21:39:46.317 * DB loaded from disk: 2.328 seconds
1:M 22 May 2021 21:39:46.317 * Ready to accept connections
```

You can check how many keys are loaded from the dump:

```bash
$ docker run -it \
             --rm \
             --link cache:cache \
             -v $PWD/data:/data \
               redis redis-cli -h cache INFO Keyspace
# Keyspace
db0:keys=3050074,expires=0,avg_ttl=0
```

this should match the number of keys as above.

When you are done you can stop and remove the container:

```bash
docker stop cache
docker rm cache
```

## Usage (with docker-compose)

**Note:** This will require a recent version of `docker-compose`.

```bash
$ docker-compose --version
docker-compose version 1.29.2, build 5becea4c
```

This docker compose will bring up two containers, one with Redis and another
with the loader script. The latter will insert the data in Redis.

```bash
$ docker-compose --profile loading up
Creating network "offer-cache_cache-network" with the default driver
Creating offer-cache_cache_1 ... done
Creating offer-cache_loader_1 ... done
Attaching to offer-cache_cache_1, offer-cache_loader_1
...
```

### Load and Export data (with docker-compose)

To export the data in RDB format issue the following command. You may need
to change the network name `offer-cache_cache-network`, use the same name
as it appears in the output of `docker-compose up`:

```bash
$ docker run -it \
             --rm \
             --network cache-network \
             --link cache:cache \
             -v "$PWD"/data:/data \
               redis redis-cli -h cache --rdb /data/routerank.rdb
sending REPLCONF capa eof
sending REPLCONF rdb-only 1
SYNC sent to master, writing 249875542 bytes to '/data/routerank.rdb'
Transfer finished with success.
```

## Import data (with docker-compose)

If you bring up a docker-compose again, it will conserve its data:

```bash
$ docker-compose up
Starting offer-cache_cache_1 ... done
Attaching to offer-cache_cache_1
cache_1   | 1:C 22 May 2021 22:06:42.051 # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo
cache_1   | 1:C 22 May 2021 22:06:42.051 # Redis version=6.2.1, bits=64, commit=00000000, modified=0, pid=1, just started
cache_1   | 1:C 22 May 2021 22:06:42.051 # Warning: no config file specified, using the default config. In order to specify a config file use redis-server /path/to/redis.conf
cache_1   | 1:M 22 May 2021 22:06:42.051 * monotonic clock: POSIX clock_gettime
cache_1   | 1:M 22 May 2021 22:06:42.052 * Running mode=standalone, port=6379.
cache_1   | 1:M 22 May 2021 22:06:42.052 # Server initialized
cache_1   | 1:M 22 May 2021 22:06:42.052 * Loading RDB produced by version 6.2.1
cache_1   | 1:M 22 May 2021 22:06:42.052 * RDB age 1759 seconds
cache_1   | 1:M 22 May 2021 22:06:42.052 * RDB memory usage when created 481.09 Mb
cache_1   | 1:M 22 May 2021 22:06:44.382 * DB loaded from disk: 2.331 seconds
cache_1   | 1:M 22 May 2021 22:06:44.383 * Ready to accept connections
```

and with Ctrl+C you can stop it again:

```bash
[Ctrl+C is pressed]
Gracefully stopping... (press Ctrl+C again to force)
Stopping offer-cache_cache_1 ... done
```

If you remove the docker-compose containers with `docker-compose down`:

```bash
$ docker-compose down
Removing offer-cache_cache_1 ... done
Removing network cache-network
```

to re-import the data again first launch docker-compose:

```bash
$ docker-compose up  
Creating network "cache-network" with the default driver
Creating offer-cache_cache_1 ... done
Attaching to offer-cache_cache_1
cache_1   | 1:C 22 May 2021 22:13:11.274 # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo
cache_1   | 1:C 22 May 2021 22:13:11.274 # Redis version=6.2.1, bits=64, commit=00000000, modified=0, pid=1, just started
cache_1   | 1:C 22 May 2021 22:13:11.274 # Warning: no config file specified, using the default config. In order to specify a config file use redis-server /path/to/redis.conf
cache_1   | 1:M 22 May 2021 22:13:11.275 * monotonic clock: POSIX clock_gettime
cache_1   | 1:M 22 May 2021 22:13:11.277 * Running mode=standalone, port=6379.
cache_1   | 1:M 22 May 2021 22:13:11.277 # Server initialized
cache_1   | 1:M 22 May 2021 22:13:11.277 * Ready to accept connections
```

stop the container:

```bash
[Ctrl+C pressed]
Gracefully stopping... (press Ctrl+C again to force)
Stopping offer-cache_cache_1 ... done
```

Import the `.rdb` file:

```bash
docker cp data/routerank.rdb offer-cache_cache_1:/data/dump.rdb
```

Restart the docker-compose file:

```bash
$ docker-compose up
Starting offer-cache_cache_1 ... done
Attaching to offer-cache_cache_1
cache_1   | 1:C 22 May 2021 22:14:54.166 # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo
cache_1   | 1:C 22 May 2021 22:14:54.166 # Redis version=6.2.1, bits=64, commit=00000000, modified=0, pid=1, just started
cache_1   | 1:C 22 May 2021 22:14:54.166 # Warning: no config file specified, using the default config. In order to specify a config file use redis-server /path/to/redis.conf
cache_1   | 1:M 22 May 2021 22:14:54.167 * monotonic clock: POSIX clock_gettime
cache_1   | 1:M 22 May 2021 22:14:54.168 * Running mode=standalone, port=6379.
cache_1   | 1:M 22 May 2021 22:14:54.168 # Server initialized
cache_1   | 1:M 22 May 2021 22:14:54.169 * Loading RDB produced by version 6.2.1
cache_1   | 1:M 22 May 2021 22:14:54.169 * RDB age 2251 seconds
cache_1   | 1:M 22 May 2021 22:14:54.169 * RDB memory usage when created 481.09 Mb
cache_1   | 1:M 22 May 2021 22:14:57.163 * DB loaded from disk: 2.994 seconds
cache_1   | 1:M 22 May 2021 22:14:57.163 * Ready to accept connections
```

and you can check as before:

```bash
$ docker run -it \
             --rm \
             --network cache-network  \
             --link offer-cache_cache_1:cache \
             -v $PWD/data:/data \
               redis redis-cli -h cache INFO Keyspace
# Keyspace
db0:keys=3050074,expires=0,avg_ttl=0
```

## Query Redis

Retrieve some request ids:

```bash
$ python3 query.py '#31:9819-#25:22859' '#30:7097-#25:16673'
* request id: #31:9819-#25:22859
    - offer id: 6990564355760000
    - offer id: 6990564287782000
    - offer id: 6990537605906000
---
* request id: #30:7097-#25:16673
    - offer id: 7000245235962000
    - offer id: 7000245253032000
    - offer id: 7000245277584000
---
```

### Help

```bash
$ python3 query.py --help
usage: query.py [-h] [-H REDIS_HOST] [--port REDIS_PORT]
                <request_id> [<request_id> ...]

positional arguments:
  <request_id>          Request ids to query.

optional arguments:
  -h, --help            show this help message and exit
  -H REDIS_HOST, --host REDIS_HOST
                        Redis hostname [default: localhost].
  --port REDIS_PORT     Redis port [default: 6379].

```

## References

* [RedisJSON: A Redis JSON Store][redislabs]
* [A presentation by Itamar Haber, Redis Labs][itamar_haber]

[final1]: http://bit.ly/R2R-final1-json-gz
[final2]: http://bit.ly/R2R-final2-json-gz
[routerank-rdb]: http://bit.ly/R2R-routerank-rdb
[redislabs]: https://redislabs.com/blog/redis-as-a-json-store/
[itamar_haber]: https://www.youtube.com/watch?v=NLRbq2FtcIk
