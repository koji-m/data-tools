# data-tools

## json2parquet

Read line-delimited JSON and upload Parquet file to S3 bucket. JSON record batches of size specified are processed in-memory and sent to S3 bucket without saving to file system.

```shell
# read input.json file and output s3://my-bucket/out_*.parquet
cargo run -- --schema schema.json --s3-bucket my-bucket --key-prefix out_ --batch-size 10000 input.json

# Or read input.json file from stdin and output s3://my-bucket/out_*.parquet
cat input.json | cargo run -- --schema schema.json --s3-bucket my-bucket --key-prefix out_ --batch-size 10000 -
```
