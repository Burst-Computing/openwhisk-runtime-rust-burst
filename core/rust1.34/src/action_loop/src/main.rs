/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use actions::main as actionMain;

use burst_communication_middleware::{create_actors, Config};
use serde_derive::Deserialize;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    env,
    fs::File,
    io::{stderr, stdin, stdout, BufRead, Write},
    os::unix::io::FromRawFd,
    thread,
};

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Input {
    value: Vec<Value>,
    invoker_id: String,
    transaction_id: String,
    burst_info: HashMap<String, Vec<u32>>,
    middleware: Middleware,
    #[serde(flatten)]
    environment: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Middleware {
    backend_info: BackendInfo,
    chunk_size: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct BackendInfo {
    uri: Option<String>,
    #[serde(flatten)]
    backend: Backend,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "backend_type")]
enum Backend {
    /// Use S3 as backend
    S3 {
        /// S3 bucket name
        bucket: Option<String>,
        /// S3 region
        region: Option<String>,
        /// S3 access key id
        access_key_id: Option<String>,
        /// S3 secret access key
        secret_access_key: Option<String>,
        /// S3 session token
        session_token: Option<String>,
    },
    /// Use Redis Streams as backend
    RedisStream,
    /// Use Redis Lists as backend
    RedisList,
    /// Use RabbitMQ as backend
    Rabbitmq,
    /// Use burst message relay as backend
    MessageRelay,
}

fn main() {
    let mut fd3 = unsafe { File::from_raw_fd(3) };
    let stdin = stdin();
    for line in stdin.lock().lines() {
        let buffer: String = line.expect("Error reading line");
        println!("buffer: {}", buffer);
        let parsed_input: Result<Input, serde_json::Error> = serde_json::from_str(&buffer);
        match parsed_input {
            Ok(input) => {
                println!("input: {:?}", input);
                for (key, val) in input.environment {
                    if let Some(string_value) = val.as_str() {
                        env::set_var(format!("__OW_{}", key.to_uppercase()), string_value);
                    } else {
                        env::set_var(format!("__OW_{}", key.to_uppercase()), val.to_string());
                    };
                }

                // Initialize middleware
                let group_ranges: HashMap<String, HashSet<u32>> = input
                    .burst_info
                    .iter()
                    .map(|(k, v)| (k.clone(), v.iter().map(|x| *x).collect::<HashSet<u32>>()))
                    .collect();

                let burst_size = input.burst_info.values().map(|x| x.len()).sum::<usize>();

                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        log_error(
                            &mut fd3,
                            format!("Error creating tokio runtime: {}", e).into(),
                        );
                        panic!("Error creating tokio runtime: {}", e);
                    }
                };

                let mut actors = match create_actors(
                    Config {
                        backend: input.middleware.backend_info.backend.into(),
                        server: input.middleware.backend_info.uri,
                        burst_id: input.transaction_id,
                        burst_size: burst_size as u32,
                        group_ranges,
                        group_id: input.invoker_id,
                        chunking: input.middleware.chunk_size.is_some(),
                        chunk_size: input.middleware.chunk_size.unwrap_or(0),
                        tokio_broadcast_channel_size: None,
                    },
                    &runtime,
                ) {
                    Ok(actors) => actors,
                    Err(e) => {
                        log_error(&mut fd3, format!("Error creating actors: {}", e).into());
                        panic!("Error creating actors: {}", e);
                    }
                };

                // Create threads
                let mut handlers = Vec::new();
                for (id, input) in input.value.into_iter().enumerate() {
                    let actor = actors
                        .remove(&(id as u32))
                        .expect(format!("Error getting actor for id: {}", id).as_str());
                    handlers.push(thread::spawn(move || {
                        println!("worker_id: {}", id);
                        println!("input: {:?}", input);
                        actionMain(input, actor)
                    }));
                }

                // new burst output have got the following format:
                // [result1, result2, ..., resultN]
                let mut results = Vec::new();
                for handle in handlers {
                    match handle.join().expect("Error joining thread") {
                        Ok(result) => results.push(result),
                        Err(error) => log_serde_error(&mut fd3, error),
                    }
                }

                let output = Value::Array(results);

                writeln!(fd3, "{}\n", output).expect("Error writing on fd3");

                stdout().flush().expect("Error flushing stdout");
                stderr().flush().expect("Error flushing stderr");
            }
            Err(error) => log_serde_error(&mut fd3, error),
        }
    }
}

impl From<Backend> for burst_communication_middleware::Backend {
    fn from(backend: Backend) -> Self {
        match backend {
            Backend::S3 {
                bucket,
                region,
                access_key_id,
                secret_access_key,
                session_token,
            } => burst_communication_middleware::Backend::S3 {
                bucket,
                region,
                access_key_id,
                secret_access_key,
                session_token,
            },
            Backend::RedisStream => burst_communication_middleware::Backend::RedisStream,
            Backend::RedisList => burst_communication_middleware::Backend::RedisList,
            Backend::Rabbitmq => burst_communication_middleware::Backend::Rabbitmq,
            Backend::MessageRelay => burst_communication_middleware::Backend::MessageRelay,
        }
    }
}

fn log_serde_error(fd3: &mut File, error: serde_json::Error) {
    writeln!(fd3, "{{\"error\":\"{}\"}}\n", error).expect("Error writing on fd3");
    eprintln!("error: {}", error);
}

fn log_error(fd3: &mut File, error: Box<dyn std::error::Error>) {
    writeln!(fd3, "{{\"error\":\"{}\"}}\n", error).expect("Error writing on fd3");
    eprintln!("error: {}", error);
}

mod test {
    #[test]
    fn test_deserialize_rabbitmq() {
        let rabbitmq = serde_json::json!({
            "value": [{"name": "Pedro G.", "param2": "value2"}, {"name": "Marc S.", "param2": "value2"}],
            "invoker_id": "invoker0",
            "transaction_id": "uuid...",
            "middleware": {
                "backend_info": {
                    "uri": "amqp://user:xxxxxx@xxxx:port",
                    "backend_type": "Rabbitmq"
                },
            },
            "burst_info": {
                "invoker0": [0, 1]
            },
            "environment": {
                "api_host": "https://apihost.com",
                "api_key": "apikey"
            }
        });

        let parsed_input: Result<crate::Input, serde_json::Error> =
            serde_json::from_value(rabbitmq);
        println!("{:?}", parsed_input);
        assert!(parsed_input.is_ok())
    }

    #[test]
    fn test_deserialize_s3() {
        let s3 = serde_json::json!({
            "value": [{"name": "Pedro G.", "param2": "value2"}, {"name": "Marc S.", "param2": "value2"}],
            "invoker_id": "invoker0",
            "transaction_id": "uuid...",
            "middleware": {
                "backend_info": {
                    "bucket": "example-bucket",
                    "region": "us-east-1",
                    "access_key_id": "ACCESS_KEY_ID",
                    "secret_access_key": "SECRET_ACCESS_KEY",
                    "session_token": "SESSION_TOKEN",
                    "backend_type": "S3"
                },
            },
            "burst_info": {
                "invoker0": [0, 1]
            },
            "environment": {
                "api_host": "https://apihost.com",
                "api_key": "apikey"
            }
        });

        let parsed_input: Result<crate::Input, serde_json::Error> = serde_json::from_value(s3);
        println!("{:?}", parsed_input);
        assert!(parsed_input.is_ok())
    }
}
