use crate::config::R2Config;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use std::path::{Path, PathBuf};
use std::fs::{OpenOptions, remove_file};
use std::io::{Seek, SeekFrom, Write, Read};
use crate::logging::LogType;
use crate::logging::sys_logger::SysLogger;

const PARQUET_ARCHIVE_READY: &str = "parquet_archive/ready";
const PARQUET_ARCHIVE_FAILD: &str = "parquet_archive/failed";
const PARQUET_ARCHIVE_CORRUPT: &str= "parquet_archive/corrupt";

pub struct R2Archiver {
    client: Client,
    r2_conf: R2Config, 
    logging: SysLogger
}


impl R2Archiver {
    pub fn new(r2_conf: &R2Config, logger: SysLogger) -> Result<Self, anyhow::Error> {
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
            r2_conf: r2_conf.clone(),
            logging: logger

       } )
    }

    pub fn list_files( dir: &str) -> Result<impl Iterator<Item = PathBuf>, anyhow::Error> {
        let entires =  std::fs::read_dir(dir)?;
        Ok(entires.filter_map(|entry| entry.ok().map(|e| e.path())))
    }

    pub fn get_file_prefix(file_path: &Path) -> Result<String, anyhow::Error> {
        let file = file_path.file_name().ok_or_else(|| anyhow::anyhow!("filename '{}' has no prefix", file_path.display()))?;
        let file_str = file.to_str().ok_or_else(|| anyhow::anyhow!("File '{}' Cannot be Converted to string", file.to_string_lossy()))?;
        let prefix = file_str.split("_").next() .ok_or_else(|| anyhow::anyhow!("filename '{}' has no prefix", file_str))?.to_string();

        Ok(prefix)
    }
    
    pub fn log_failed(file_name: &str) -> Result<( u32, PathBuf), anyhow::Error>{
        let mut failed_log = PathBuf::from(PARQUET_ARCHIVE_FAILD);
        failed_log.push(file_name);
        failed_log.set_extension("attempts");
        

        let mut fail_log = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&failed_log)?;

        let mut side_car_content = String::new(); 
        fail_log.read_to_string(&mut side_car_content)?;

        let mut attempt_count: u32 = side_car_content.trim().parse().unwrap_or(0);
        
        
        attempt_count += 1;


        fail_log.seek(SeekFrom::Start(0))?;
        fail_log.set_len(0)?;
        write!(&mut fail_log, "{attempt_count}")?;

        fail_log.flush()?;

        Ok((attempt_count, failed_log))

    }

    pub fn corrupt_clean_up(&mut self, file_path: &Path, attempt_count: u32, failed_log: PathBuf) -> Result<(), anyhow::Error>{
        if attempt_count > self.r2_conf.max_upload_attempts {
            //Move file to corrupted foilder 
            let corrupt_path = PathBuf::from(PARQUET_ARCHIVE_CORRUPT).join(file_path.file_name().unwrap());
            
            std::fs::rename(file_path,corrupt_path)?; 
            self.logging.sys_log(LogType::Error, &format!("{} | Has Been Moved to Corruped as it has reached max attempts", file_path.display()));
            
            remove_file(&failed_log)?;
            self.logging.sys_log(LogType::Info, &format!("{} | Sidecar Log File has been successfully deleted", file_path.display()));
        }

        Ok(())
    }

    pub async fn r2_archiver(&self, path: &Path, folder: String) -> Result<(), anyhow::Error> {
        let body = ByteStream::from_path(&path).await?;
        let file = path.file_name().ok_or_else(|| anyhow::anyhow!("filename '{}' has no prefix", path.display()))?;
        let file_str = file.to_str().ok_or_else(|| anyhow::anyhow!("File '{}' Cannot be Converted to string", file.to_string_lossy()))?;
        let key = format!("{}/{}", folder, file_str);
        self.client.put_object()
            .bucket(self.r2_conf.bucket.clone())
            .key(key)
            .body(body)
            .send()
            .await?;
        
        Ok(())
    }

    

    pub async fn archiver(&mut self) -> Result<(), anyhow::Error> {
        //Checking failed folder first

        for file in Self::list_files(PARQUET_ARCHIVE_FAILD)? {
            let file_path: &Path  = file.as_path();
            let prefix = Self::get_file_prefix(&file_path)?;
            match self.r2_archiver(&file_path, prefix).await{
                Ok(()) => continue, 
                Err(_e) => {
                    //edit logfile here
                    let path = file_path.to_str().expect("Invalid Path");
                    let (atempts, fail_sidecar): (u32, PathBuf) = match Self::log_failed(path) {
                        Ok((atempts, fail_sidecar)) => {
                            self.corrupt_clean_up(file_path, atempts, fail_sidecar)?;
                            continue;
                        }
                        Err(_e) => {
                            continue;
                        }
                    };


                }
            }

        };

        for file in Self::list_files(PARQUET_ARCHIVE_READY)? {
            let file_path: &Path  = file.as_path();
            let prefix = Self::get_file_prefix(&file_path)?;
            match self.r2_archiver(&file_path, prefix).await {  
                Ok(()) => continue,
                Err(_e) => {
                    let failed_path = PathBuf::from(PARQUET_ARCHIVE_FAILD).join(file.file_name().unwrap());
                    std::fs::rename(file_path,failed_path)?;
                    continue; 
                } 
            };
            
        }

            Ok(())
    }

}


