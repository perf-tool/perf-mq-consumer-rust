// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use futures::TryStreamExt;
use pulsar::{Consumer, Pulsar, TokioExecutor, message::proto::command_subscribe::SubType, Error};
use serde::{Deserialize, Serialize};
use serde_env::from_env;

#[derive(Debug, Serialize, Deserialize)]
struct PulsarConfig {
    #[serde(default = "default_localhost")]
    pulsar_host: String,
    #[serde(default = "default_6650")]
    pulsar_port: i32,
    pulsar_topic: String,
    pulsar_subscription_name: String,
}

fn default_localhost() -> String {
    "localhost".to_string()
}

fn default_6650() -> i32 {
    6650
}

pub async fn start() {
    let pulsar_config: PulsarConfig = from_env().expect("deserialize from env");
    let addr = format!("pulsar://{}:{}", pulsar_config.pulsar_host, pulsar_config.pulsar_port);
    let result = Pulsar::builder(addr, TokioExecutor).build().await;
    match result {
        Ok(pulsar) => {
            let result: Result<Consumer<String, TokioExecutor>, Error>  = pulsar
                .consumer()
                .with_topic(pulsar_config.pulsar_topic)
                .with_subscription_type(SubType::Failover)
                .with_subscription(pulsar_config.pulsar_subscription_name)
                .build()
                .await;
            match result {
                Ok(mut consumer) => {
                    while let Some(msg) = consumer.try_next().await.unwrap() {
                        consumer.ack(&msg).await.unwrap();
                    }
                }
                Err(e) => {
                    println!("create consumer failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!("connect failed: {}", e);
        }
    }
}
