use crate::config::R2Config;
use aws_sdk_s3::Client;
use std::path::PathBuf;


pub struct R2Archiver {
    client: Client,
    r2_conf: R2Config
}


impl R2Archiver {
    pub fn new(r2_conf: &R2Config) -> Result<Self, anyhow::Error> {
        let region = aws_sdk_s3::config::Region::new("auto");

        let access_key = std::env::var("R2_API_ACCESS_KEY")?;
        let secret_key = std::env::var("R2_API_SECRET_KEY")?;
        let creds = aws_sdk_s3::config::Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "r2"
        );

        let config = aws_sdk_s3::config::Builder::new()
            .region(region)
            .endpoint_url(&r2_conf.endpoint)
            .credentials_provider(creds)
            .behavior_version_latest()
            .build(); 
        
        let client = aws_sdk_s3::Client::from_conf(config);
       
        Ok(R2Archiver{
            client: client,
            r2_conf: r2_conf.clone()
       } )
    }

    pub fn list_files(dir: &str) -> Result<impl Iterator<Item = PathBuf>, anyhow::Error> {
        let entires =  std::fs::read_dir(dir)?;
        Ok(entires.filter_map(|entry| entry.ok().map(|e| e.path())))
    }

    pub fn get_file_prefix(file_name: &str) -> Result<&str, anyhow::Error> {
        let file_type = file_name.split("_").next()
            .ok_or_else(|| anyhow::anyhow!("filename '{}' has no prefix", file_name))?;
        Ok(file_type)
    }
    
    pub fn archiver(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }




}


