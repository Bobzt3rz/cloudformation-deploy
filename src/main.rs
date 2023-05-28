use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_cloudformation::operation::create_stack::CreateStackError;
use aws_sdk_cloudformation::types::{Capability, Parameter};
use aws_sdk_cloudformation::Client as CloudformationClient;
use aws_sdk_dynamodb::error::DisplayErrorContext;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLocationConstraint, CreateBucketConfiguration, ObjectIdentifier, VersioningConfiguration,
};
use aws_sdk_s3::{Client, Error};
use aws_types::region::Region;
use serde_json::{json, Value};
use zip::ZipArchive;

const DIRECTORY: &str = "/home/palad1nz0/Downloads";
const REGION: &str = "ap-southeast-1";
const BUCKET_NAME: &str = "bob-ap-southeast-1";
const USE_DEFAULT: bool = true;

#[tokio::main]
async fn main() -> Result<(), Error> {
    print!("Enter project name: ");
    io::stdout().flush().unwrap();

    let mut project_name = String::new();
    match io::stdin().read_line(&mut project_name) {
        Ok(_) => {
            project_name = project_name.to_string().trim_matches('\n').to_string();
        }
        _ => panic!("Invalid input"),
    };

    let zip_path = get_last_modified_zip_in_directory(DIRECTORY).unwrap();
    let zip_path_str = zip_path.display().to_string();
    let folder_name_index = match zip_path_str.rfind('/') {
        Some(index) => index,
        None => panic!("Invalid path"),
    };
    let folder_name = &zip_path_str[folder_name_index + 1..zip_path_str.len() - 4];
    let file_paths_names = unzip_file(&zip_path);

    let region_provider = RegionProviderChain::first_try(Region::new(REGION));
    let region = region_provider.region().await.unwrap();

    let config = aws_config::from_env().region(region_provider).load().await;
    let s3_client = Client::new(&config);

    let list_resp = s3_client
        .list_objects_v2()
        .bucket(BUCKET_NAME)
        .prefix(&project_name)
        .send()
        .await;

    // Do something with prev_obj_keys?
    let (has_bucket, _prev_obj_keys) = match list_resp {
        Ok(val) => {
            let contents = val.contents().unwrap_or_default();
            let keys: Vec<ObjectIdentifier> = contents
                .iter()
                .filter_map(|content| match content.key() {
                    Some(val) => {
                        if val.ends_with('/') {
                            None
                        } else {
                            Some(
                                ObjectIdentifier::builder()
                                    .set_key(Some(val.to_string()))
                                    .build(),
                            )
                        }
                    }
                    _ => None,
                })
                .collect();
            (true, keys)
        }
        Err(e) => match e.into_service_error() {
            ListObjectsV2Error::NoSuchBucket(_) => (false, Vec::new()),
            e => panic!("{}", DisplayErrorContext(&e)),
        },
    };

    if !has_bucket {
        println!("No bucket, creating one");
        let constraint = BucketLocationConstraint::from(region.to_string().as_str());
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();
        s3_client
            .create_bucket()
            .bucket(BUCKET_NAME)
            .create_bucket_configuration(cfg)
            .send()
            .await
            .unwrap_or_else(|e| panic!("{}", DisplayErrorContext(&e)));

        let ver_config = VersioningConfiguration::builder()
            .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
            .build();
        s3_client
            .put_bucket_versioning()
            .bucket(BUCKET_NAME)
            .versioning_configuration(ver_config)
            .send()
            .await
            .unwrap_or_else(|e| panic!("{}", DisplayErrorContext(&e)));
    }

    let (template_str, template_url) =
        upload_files_to_s3(&project_name, file_paths_names, &s3_client).await;
    let template_json = serde_json::from_str::<Value>(&template_str).unwrap();
    let parameters = template_json
        .get("Parameters")
        .unwrap()
        .as_object()
        .unwrap();

    let mut parameter_builders: Vec<Parameter> = Vec::new();

    for (key, value) in parameters {
        if USE_DEFAULT {
            match value.get("Default") {
                Some(_) => continue,
                None => {}
            }
        }

        match key.as_str() {
            "CGDeploymentBucket" => {
                parameter_builders.push(
                    Parameter::builder()
                        .parameter_key("CGDeploymentBucket")
                        .parameter_value(BUCKET_NAME)
                        .build(),
                );
            }
            "CGDeploymentPath" => {
                parameter_builders.push(
                    Parameter::builder()
                        .parameter_key("CGDeploymentPath")
                        .parameter_value(format!("{}/{}", &project_name, folder_name))
                        .build(),
                );
            }
            _ => {
                println!(
                    "Description: {}",
                    value
                        .get("Description")
                        .unwrap_or(&json!("No description"))
                        .as_str()
                        .unwrap_or("No description")
                );
                println!(
                    "Type: {}",
                    value
                        .get("Type")
                        .unwrap_or(&json!("Unknown type"))
                        .as_str()
                        .unwrap_or("Unknown type")
                );
                print!("Enter parameter value: ");
                io::stdout().flush().unwrap();

                let mut input = String::new();
                match io::stdin().read_line(&mut input) {
                    Ok(_) => {
                        parameter_builders.push(
                            Parameter::builder()
                                .parameter_key(key)
                                .parameter_value(input.trim_matches('\n').to_string())
                                .build(),
                        );
                    }
                    _ => panic!("Invalid input"),
                }
            }
        }
    }

    let cloudformation_client = CloudformationClient::new(&config);
    // TODO: better way of handling capabilities?
    let stack_id = match cloudformation_client
        .create_stack()
        .stack_name(&project_name)
        .set_parameters(Some(parameter_builders.clone()))
        .template_url(&template_url)
        .capabilities(Capability::CapabilityNamedIam)
        .send()
        .await
    {
        Ok(res) => {
            let stack_id = res.stack_id.unwrap();
            println!("Deploying stack: {}...", stack_id);
            stack_id
        }
        Err(e) => match e.into_service_error() {
            CreateStackError::AlreadyExistsException(_) => {
                println!("Stack with name already exists, updating previous stack...");
                match cloudformation_client
                    .update_stack()
                    .stack_name(&project_name)
                    .set_parameters(Some(parameter_builders))
                    .template_url(&template_url)
                    .capabilities(Capability::CapabilityNamedIam)
                    .send()
                    .await
                {
                    Ok(res) => {
                        let stack_id = res.stack_id().unwrap();
                        println!("Updating stack: {}...", stack_id);
                        stack_id.to_string()
                    }
                    Err(e) => panic!("{}", DisplayErrorContext(&e)),
                }
            }
            e => panic!("{}", DisplayErrorContext(&e)),
        },
    };
    // poll status of stack
    let mut in_progress = true;
    while in_progress {
        match cloudformation_client
            .describe_stacks()
            .stack_name(&stack_id)
            .send()
            .await
        {
            Ok(res) => {
                let stacks = res.stacks.as_ref().unwrap();
                let status = match stacks.get(0) {
                    Some(s) => s.stack_status.as_ref().unwrap().as_str(),
                    None => "Unknown status",
                };
                match status {
                    s if s.ends_with("PROGRESS") => {
                        println!("{}", s);
                        thread::sleep(Duration::from_secs(2));
                    }
                    s => {
                        println!("{}", s);
                        in_progress = false;
                    }
                }
            }
            Err(e) => panic!("{}", DisplayErrorContext(&e)),
        }
    }

    Ok(())
}

fn get_last_modified_zip_in_directory(directory: &str) -> Option<PathBuf> {
    let result = fs::read_dir(directory)
        .expect("Failed to read directory")
        .filter_map(|entry| {
            if entry.is_err() {
                return None;
            };
            let checked_entry = entry.unwrap();
            let path = checked_entry.path();
            if !path.is_file() || path.extension().unwrap_or_default() != "zip" {
                return None;
            }
            match checked_entry.metadata() {
                Err(_) => None,
                Ok(metadata) => match metadata.modified() {
                    Ok(time) => Some((path, time)),
                    Err(_) => None,
                },
            }
        })
        .fold(None, |highest, (path, time)| match highest {
            None => Some((path, time)),
            Some((highest_path, highest_time)) => {
                if time > highest_time {
                    Some((path, time))
                } else {
                    Some((highest_path, highest_time))
                }
            }
        });

    match result {
        None => None,
        Some((path, _)) => Some(path),
    }
}

fn unzip_file(zip_path: &PathBuf) -> Vec<(PathBuf, String)> {
    let file = File::open(zip_path).unwrap();
    let mut archive = ZipArchive::new(file).unwrap();

    let mut file_paths_names: Vec<(PathBuf, String)> = Vec::new();

    for i in 0..archive.len() {
        let mut file = archive.by_index(i).unwrap();
        let (outpath, name) = match file.enclosed_name() {
            Some(name) => (Path::new(DIRECTORY).join(name), name.display().to_string()),
            None => continue,
        };

        if (*file.name()).ends_with('/') {
            println!("File {} extracted to \"{}\"", i, outpath.display());
            fs::create_dir_all(&outpath).unwrap();
        } else {
            println!(
                "File {} extracted to \"{}\" ({} bytes)",
                i,
                outpath.display(),
                file.size()
            );
            if let Some(p) = outpath.parent() {
                if !p.exists() {
                    fs::create_dir_all(p).unwrap();
                }
            }
            let mut outfile = fs::File::create(&outpath).unwrap();
            io::copy(&mut file, &mut outfile).unwrap();
        }

        // Get and Set permissions
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            if let Some(mode) = file.unix_mode() {
                fs::set_permissions(&outpath, fs::Permissions::from_mode(mode)).unwrap();
            }
        }
        file_paths_names.push((outpath, name));
    }
    file_paths_names
}

async fn upload_files_to_s3(
    project_name: &String,
    file_paths_names: Vec<(PathBuf, String)>,
    client: &aws_sdk_s3::Client,
) -> (String, String) {
    let mut json_str = String::new();
    let mut template_url = String::new();
    for (path, name) in file_paths_names {
        let mut file = File::open(&path).unwrap();
        let mut file_contents = Vec::new();
        file.read_to_end(&mut file_contents).unwrap();

        let ends_in_json = name.ends_with(".json");
        if ends_in_json {
            json_str = std::str::from_utf8(&file_contents).unwrap().to_string();
            let region_string = if REGION == "us-east-1" {
                String::new()
            } else {
                format!(".{}", REGION)
            };
            template_url = format!(
                "https://{}.s3{}.amazonaws.com/{}/{}",
                BUCKET_NAME, region_string, project_name, name
            );
        }

        client
            .put_object()
            .key(format!("{}/{}", &project_name, name))
            .bucket(BUCKET_NAME)
            .body(ByteStream::from(file_contents))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{}", DisplayErrorContext(&e)));
        println!("Uploaded file: {}", name);
    }
    (json_str, template_url)
}
