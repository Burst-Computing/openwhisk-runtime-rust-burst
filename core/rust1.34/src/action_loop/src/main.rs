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

use burst_communication_middleware::{
    BurstMessageRelayImpl, BurstMessageRelayOptions, BurstMiddleware, BurstOptions,
    MiddlewareActorHandle, RabbitMQMImpl, RabbitMQOptions, RedisListImpl, RedisListOptions,
    RedisStreamImpl, RedisStreamOptions, TokioChannelImpl, TokioChannelOptions,
};
use serde_derive::Deserialize;
use serde_json::{Error, Value};
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
    rabbitmq: RabbitMQ,
    #[serde(flatten)]
    environment: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RabbitMQ {
    uri: String,
}

#[derive(Debug)]
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

#[derive(Debug)]
struct Arguments {
    backend: Backend,
    server: Option<String>,
    burst_id: String,
    burst_size: u32,
    group_ranges: HashMap<String, HashSet<u32>>,
    group_id: String,
    chunking: bool,
    chunk_size: usize,
    tokio_broadcast_channel_size: Option<usize>,
}

const MB: usize = 1024 * 1024;
const DEFAULT_CHUNK_SIZE: usize = 1 * MB;

// new burst input have got the following format:
// {
//    "value": [{"name": "Pedro G.", "param2": "value2"}, ..., {"name": "Marc S.", "param2": "value2"}],
//    "burst_info: {invoker0: [0, 3], invoker1: [4, 13], ..., invokerN: 180, 199]},
//    "invoker_id": "invoker0",
//    "transaction_id": "uuid...",
//    "rabbitmq": {
//       "uri": "amqp://user:passwd@host:port",
//    }
// }
fn main() {
    let mut fd3 = unsafe { File::from_raw_fd(3) };
    let stdin = stdin();
    for line in stdin.lock().lines() {
        let buffer: String = line.expect("Error reading line");
        println!("buffer: {}", buffer);
        let parsed_input: Result<Input, Error> = serde_json::from_str(&buffer);
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

                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("Error creating tokio runtime");

                let mut actors = create_actors(
                    Arguments {
                        backend: Backend::Rabbitmq,
                        server: Some(input.rabbitmq.uri),
                        burst_id: input.transaction_id,
                        burst_size: burst_size as u32,
                        group_ranges,
                        group_id: input.invoker_id,
                        chunking: true,
                        chunk_size: DEFAULT_CHUNK_SIZE,
                        tokio_broadcast_channel_size: None,
                    },
                    &runtime,
                );

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
                        Err(error) => log_error(&mut fd3, error),
                    }
                }

                let output = Value::Array(results);

                writeln!(fd3, "{}\n", output).expect("Error writing on fd3");

                stdout().flush().expect("Error flushing stdout");
                stderr().flush().expect("Error flushing stderr");
            }
            Err(error) => log_error(&mut fd3, error),
        }
    }
}

fn create_actors(
    args: Arguments,
    tokio_runtime: &tokio::runtime::Runtime,
) -> HashMap<u32, MiddlewareActorHandle> {
    let burst_options = BurstOptions::new(
        args.burst_id.to_string(),
        args.burst_size,
        args.group_ranges,
        args.group_id.to_string(),
        args.chunking,
        args.chunk_size,
    );

    let mut channel_options = TokioChannelOptions::new();
    if let Some(size) = args.tokio_broadcast_channel_size {
        channel_options.broadcast_channel_size(size);
    }

    let proxies = tokio_runtime.block_on(async move {
        match &args.backend {
            Backend::S3 {
                bucket,
                region,
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                let mut options = burst_communication_middleware::S3Options::default();
                if let Some(bucket) = bucket {
                    options.bucket(bucket.to_string());
                }
                if let Some(region) = region {
                    options.region(region.to_string());
                }
                if let Some(access_key_id) = access_key_id {
                    options.access_key_id(access_key_id.to_string());
                }
                if let Some(secret_access_key) = secret_access_key {
                    options.secret_access_key(secret_access_key.to_string());
                }
                options.session_token(session_token.clone());
                options.endpoint(args.server.clone());

                BurstMiddleware::create_proxies::<
                    TokioChannelImpl,
                    burst_communication_middleware::S3Impl,
                    _,
                    _,
                >(burst_options, channel_options, options)
                .await
            }
            Backend::RedisStream => {
                let mut options = RedisStreamOptions::default();
                if let Some(server) = &args.server {
                    options.redis_uri(server.to_string());
                }

                BurstMiddleware::create_proxies::<TokioChannelImpl, RedisStreamImpl, _, _>(
                    burst_options,
                    channel_options,
                    options,
                )
                .await
            }
            Backend::RedisList => {
                let mut options = RedisListOptions::default();
                if let Some(server) = &args.server {
                    options.redis_uri(server.to_string());
                }
                BurstMiddleware::create_proxies::<TokioChannelImpl, RedisListImpl, _, _>(
                    burst_options,
                    channel_options,
                    options,
                )
                .await
            }
            Backend::Rabbitmq => {
                let mut options = RabbitMQOptions::default()
                    .durable_queues(true)
                    .ack(true)
                    .build();
                if let Some(server) = &args.server {
                    options.rabbitmq_uri(server.to_string());
                }
                BurstMiddleware::create_proxies::<TokioChannelImpl, RabbitMQMImpl, _, _>(
                    burst_options,
                    channel_options,
                    options,
                )
                .await
            }
            Backend::MessageRelay => {
                let mut options = BurstMessageRelayOptions::default();
                if let Some(server) = &args.server {
                    options.server_uri(server.to_string());
                }
                BurstMiddleware::create_proxies::<TokioChannelImpl, BurstMessageRelayImpl, _, _>(
                    burst_options,
                    channel_options,
                    options,
                )
                .await
            }
        }
    });

    match proxies {
        Ok(proxies) => proxies
            .into_iter()
            .map(|(id, proxy)| {
                let actor = MiddlewareActorHandle::new(proxy, tokio_runtime);
                (id, actor)
            })
            .collect::<HashMap<u32, MiddlewareActorHandle>>(),
        Err(error) => panic!("Error creating proxies: {}", error),
    }
}

fn log_error(fd3: &mut File, error: Error) {
    writeln!(fd3, "{{\"error\":\"{}\"}}\n", error).expect("Error writing on fd3");
    eprintln!("error: {}", error);
}
