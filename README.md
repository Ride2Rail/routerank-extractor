# Offer Cache

## Preparation

1. Create a virtualenv;
2. Install the requirements: `pip3 install -r requirements.txt`;
3. Get the [`final1.json.gz`][final1], [`final2.json.gz`][final2] files and
   save them in the `data/`;

## Usage (standalone)

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

## Usage (with docker-compose)

This docker compose will bring up two containers, one with Redis and another
with the loader script. The latter will insert the data in Redis.

```bash
$ docker-compose up
Creating network "offer-cache_cache-network" with the default driver
Creating offer-cache_cache_1 ... done
Creating offer-cache_loader_1 ... done
Attaching to offer-cache_cache_1, offer-cache_loader_1
...
```

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

## References

* [RedisJSON: A Redis JSON Store][redislabs]
* [A presentation by Itamar Haber, Redis Labs][itamar_haber]

[final1]: http://bit.ly/R2R-final1-json-gz
[final2]: http://bit.ly/R2R-final2-json-gz
[redislabs]: https://redislabs.com/blog/redis-as-a-json-store/
[itamar_haber]: https://www.youtube.com/watch?v=NLRbq2FtcIk
