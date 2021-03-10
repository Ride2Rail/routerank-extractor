# Offer Cache

## Preparation

1. Create a virtualenv
2. Install the requirements: `pip3 install -r requirements.txt`

## Usage

Start redis in a docker container:

```bash
docker run --rm --name cache -p 6379:6379 redislabs/rejson:latest
```

Load the data into redis:

```bash
python3 loader.py data/final1.json.gz data/final2.json.gz
```

```bash
docker run -it \
           --rm \
           --link cache:cache  \
           -v $PWD/data:/data  \
             redis redis-cli -h cache --rdb /data/routerank.rdb
```

## References

* [RedisJSON: A Redis JSON Store][redislabs]
* [A presentation by Itamar Haber, Redis Labs][[itamar_haber]]

[redislabs]: https://redislabs.com/blog/redis-as-a-json-store/
[itamar_haber]: https://www.youtube.com/watch?v=NLRbq2FtcIk
