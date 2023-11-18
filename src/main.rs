use std::{path::Path, sync::mpsc, time::Duration, env};

use aws_sdk_s3 as s3;
use s3::primitives::ByteStream;

use notify::{RecursiveMode, Watcher};

use notify_debouncer_full::new_debouncer;

#[tokio::main]
async fn main() {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    // Read command line arguments
    let args: Vec<String> = env::args().collect();

    // Check if the correct number of arguments is provided
    if args.len() != 4 {
        eprintln!("Usage: {} <bucket_name> <local_directory> <s3_directory>", args[0]);
        std::process::exit(1);
    }

    // Extract values from command line arguments
    let bucket_name = &args[1];
    let local_directory = &args[2];
    let s3_directory = &args[3];

    // Rest of your code
    println!("Bucket Name: {}", bucket_name);
    println!("Local Directory: {}", local_directory);
    println!("S3 Directory: {}", s3_directory);

    let (tx, rx) = mpsc::channel();

    // Create a new debounced file watcher with a timeout of 2 seconds.
    // The tickrate will be selected automatically, as well as the underlying watch implementation.
    let mut debouncer = match new_debouncer(Duration::from_secs(2), None, tx) {
        Ok(debouncer) => {
            println!("Created Debounced file watcher with a timeout of 2 seconds");
            debouncer
        }
        Err(error) => {
            println!(
                "Failed to create debounced file watcher with a timeout of 2 seconds: {:?}",
                error
            );
            std::process::exit(1);
        }
    };

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    match debouncer
        .watcher()
        .watch(Path::new(local_directory), RecursiveMode::Recursive)
    {
        Ok(_) => println!("Watching files in {} diretory for changes", local_directory),
        Err(error) => {
            println!(
                "Failed to Watching files in {} diretory for changes: {:?}",
                local_directory, error
            );
            std::process::exit(1);
        }
    };

    // Initialize the file id cache for the same path. This will allow the debouncer to stitch together move events,
    // even if the underlying watch implementation doesn't support it.
    // Without the cache and with some watch implementations,
    // you may receive `move from` and `move to` events instead of one `move both` event.
    debouncer
        .cache()
        .add_root(Path::new(local_directory), RecursiveMode::Recursive);

    for debounced_events in rx {
        match debounced_events {
            Ok(events) => {
                for debounced_event in events {
                    if let notify::EventKind::Modify(_) = debounced_event.event.kind {
                        for path in debounced_event.event.paths {
                            let path = match path.to_str() {
                                Some(path) => path,
                                None => continue,
                            };

                            let mut key = String::from(s3_directory);
                            key.push_str(path);

                            let body = match ByteStream::from_path(Path::new(path)).await {
                                Ok(body) => body,
                                Err(error) => {
                                    println!(
                                        "Failed to get byte stream form the path {}: {:?}",
                                        path, error
                                    );
                                    continue;
                                }
                            };

                            match client
                                .put_object()
                                .bucket(bucket_name)
                                .key(key)
                                .body(body)
                                .send()
                                .await
                            {
                                Ok(_) => println!("The file {} has been successfully backed up",path),
                                Err(error) => {
                                    println!("The backup process for the file {} encountered an error: {:?}",path,error);
                                    continue;
                                },
                            };
                        }
                    }
                }
            }
            Err(errors) => {
                for error in errors{
                    println!("Debounced event error{:?}", error);
                }
            }
        }
    }
}
