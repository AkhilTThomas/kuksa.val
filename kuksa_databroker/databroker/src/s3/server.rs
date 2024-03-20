/********************************************************************************
* Copyright (c) 2023 Contributors to the Eclipse Foundation
*
* See the NOTICE file(s) distributed with this work for additional
* information regarding copyright ownership.
*
* This program and the accompanying materials are made available under the
* terms of the Apache License 2.0 which is available at
* http://www.apache.org/licenses/LICENSE-2.0
*
* SPDX-License-Identifier: Apache-2.0
********************************************************************************/

use crate::{
    authorization::Authorization,
    broker::{self, Entry},
    permissions::{self, Permissions},
};
use databroker_proto::kuksa::val::v1::{self as proto, Datapoint};

use aws_sdk_s3::{config::Credentials, primitives::ByteStream, Client};
use std::time::Duration;
use tokio::time::interval;
use tracing::debug;
use std::collections::{HashMap, HashSet};
use crate::types::{ChangeType, DataType, DataValue, EntryType};


fn get_credentials() -> Credentials {
    Credentials::new(
        // "TbPQujnhdhpO54pDt9lj",
        "minioadmin1",
        // "MgsmIoPBrcOtGZyqYqod9K6HVONpumTSVpZ4wklA",
        "minioadmin1",
        None,
        None,
        "",
    )
}

/*
Subscribe to databroker points
Read the points of interset from a file, if possible add support for wildcards
buffer data for 10s, buffer size must be configurable
upload the data into s3 bucket periodically
*/
pub async fn serve(
    broker: broker::DataBroker,
    authorization: Authorization,
) -> Result<(), Box<dyn std::error::Error>> {
    let shared_config = aws_config::from_env()
        .region("us-east-1")
        .credentials_provider(get_credentials())
        // .endpoint_url("http://192.168.0.111:9000")
        .endpoint_url("http://172.18.0.2:9000")
        .load()
        .await;
    let client = Client::new(&shared_config);

    println!("Successfully entered server");

    // Replace these with your S3 bucket name and object key
    let bucket_name = "kuksa";
    let object_key = "kuksa";

    let broker = broker.authorized_access(&&permissions::ALLOW_ALL);

    // Example data to be written
    let data = "Hello, S3!";



    // Initialize the Tokio interval with the desired duration
    let mut interval = interval(Duration::from_secs(10)); // Change the duration as needed

    loop {
        // Await the next interval tick
        interval.tick().await;

        // let datapoints = broker.map_entries(|entry| entry.datapoint().map(|dp| dp.clone())).await;
        // for dp in datapoints {
        //     println!("{:?}",dp);
        // }

        let mut dbHashMap : HashMap<String, DataValue> = HashMap::new();

        let metadatas = broker.map_entries(|entry| entry.metadata().clone()).await;
        for metadata in metadatas {
            // println!("{:?}",entry);
            match broker.get_entry_by_path(metadata.path.as_str()).await {
                    Ok(entry) => {
                        let dp = entry.datapoint.value.to_owned();
                        // println!("{:?}",dp);
                        dbHashMap.insert(metadata.path.as_str().to_string(), dp);
                    }
                    Err(err) => {
                        eprintln!("Error reading data from database: {:?}", err);
                    }
            }
        }

        let serialized = serde_json::to_string(&dbHashMap).unwrap();

        let body = ByteStream::from(serialized.as_bytes().to_vec());
                // Send request to S3
                match client
                    .put_object()
                    .bucket(bucket_name)
                    .key(object_key)
                    .body(body)
                    .send()
                    .await
                {
                    Ok(_) => {
                        println!("Successfully wrote data to S3");
                    }
                    Err(err) => {
                        eprintln!("Error writing data to S3: {:?}", err);
                    }
                }

        // Get datapoints
        // match broker.get_entry_by_path("Vehicle.Speed").await {
        //     Ok(entry) => {
        //         let dp = entry.datapoint.value.to_owned();
        //         println!("{:?}",dp);

        //         let body = ByteStream::from(data.as_bytes().to_vec());
        //         // Send request to S3
        //         match client
        //             .put_object()
        //             .bucket(bucket_name)
        //             .key(object_key)
        //             .body(body)
        //             .send()
        //             .await
        //         {
        //             Ok(_) => {
        //                 println!("Successfully wrote data to S3");
        //             }
        //             Err(err) => {
        //                 eprintln!("Error writing data to S3: {:?}", err);
        //             }
        //         }
        //     }
        //     Err(_) => {
        //         // TODO: This should probably generate an error
        //         debug!("notify: could not find entry with id ")
        //     }
        // }
    }
}
