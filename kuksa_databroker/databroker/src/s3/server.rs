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
    broker,
    permissions::{self, Permissions},
};
use databroker_proto::kuksa::val::v1 as proto;

use aws_sdk_s3::{config::Credentials, primitives::ByteStream, Client};
use std::time::Duration;
use tokio::time::interval;

struct DataResponse {
    data: Data,
}

fn get_credentials() -> Credentials {
    Credentials::new(
        "TbPQujnhdhpO54pDt9lj",
        "MgsmIoPBrcOtGZyqYqod9K6HVONpumTSVpZ4wklA",
        None,
        None,
        "",
    )
}

pub async fn serve(
    broker: broker::DataBroker,
    authorization: Authorization,
) -> Result<(), Box<dyn std::error::Error>> {
    let shared_config = aws_config::from_env()
        .region("us-east-1")
        .credentials_provider(get_credentials())
        .endpoint_url("http://192.168.0.111:9000")
        .load()
        .await;
    let client = Client::new(&shared_config);

    // Replace these with your S3 bucket name and object key
    let bucket_name = "databroker-bucket";
    let object_key = "kuksa";

    let broker = broker.authorized_access(&&permissions::ALLOW_ALL);

    // Example data to be written
    let data = "Hello, S3!";

    // Initialize the Tokio interval with the desired duration
    let mut interval = interval(Duration::from_secs(10)); // Change the duration as needed

    loop {
        // Await the next interval tick
        interval.tick().await;

        // Get datapoints
        match broker.get_datapoint_by_path("Vehicle.Speed").await {
            Ok(datapoint) => {
                let dp = DataPoint::from(datapoint);
                let response = DataResponse {
                    data: Data::Object(DataObject {
                        path: request.path,
                        dp,
                    }),
                };
                let bytes = serde_json::to_string(&response);

                let body = Some(bytes.into());
                // Send request to S3
                match client
                    .put_object()
                    .bucket(bucket_name)
                    .key(object_key)
                    .body(body.unwrap())
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
            }
        }
    }
}
