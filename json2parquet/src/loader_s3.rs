use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    types::ByteStream,
    {Client, Region}
};

pub struct Loader {
    client: Client,
    bucket: String,
    key_prefix: String,
}

impl Loader {
    pub async fn new(bucket: &str, key_prefix: &str) -> Self {
        let region_provider = RegionProviderChain::default_provider()
            .or_else(Region::new("us-east-1"));
        let config = aws_config::from_env().region(region_provider).load().await;
        Some(Client::new(&config));
        Self {
            client: Client::new(&config),
            bucket: String::from(bucket),
            key_prefix: String::from(key_prefix),
        }
    }

    pub async fn load(&self, bytes: Vec<u8>, suffix: usize) {
        let stream = ByteStream::from(bytes);
        let file = format!("{}{}.parquet", self.key_prefix, suffix);
        let resp = self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&file)
            .body(stream)
            .send()
            .await;
        match resp {
            Ok(_) => println!("Wrote s3://{}/{}", self.bucket, file),
            Err(_) => println!("Error write s3://{}/{}", self.bucket, file),
        }
    }
}