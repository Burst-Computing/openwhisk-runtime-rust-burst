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

use burst_communication_middleware::{Middleware, MiddlewareArguments};
use serde_derive::Deserialize;
use serde_json::{Error, Value};
use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{stderr, stdin, stdout, BufRead, Write},
    os::unix::io::FromRawFd,
    thread::JoinHandle,
};

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Input {
    value: Value,
    invoker_id: Option<String>,
    transaction_id: Option<String>,
    burst_info: Option<HashMap<String, Vec<u32>>>,
    rabbitmq: Option<RabbitMQ>,
    #[serde(flatten)]
    environment: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct RabbitMQ {
    uri: String,
}

fn log_error(fd3: &mut File, error: Error) {
    writeln!(fd3, "{{\"error\":\"{}\"}}\n", error).expect("Error writing on fd3");
    eprintln!("error: {}", error);
}

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
#[tokio::main]
async fn main() {
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
                // if burst, create inputs for each function
                let burst = input.value.is_array();
                let mut inputs: Vec<Value> = Vec::new();
                let mut handlers: Vec<JoinHandle<Result<Value, Error>>> = Vec::new();
                if burst {
                    // each function will receive the next format:
                    // Value class: {"name": "Pedro G.", "param2": "value2"}
                    for value in input.value.as_array().unwrap() {
                        inputs.push(value.clone());
                    }
                } else {
                    inputs.push(input.value);
                }

                // Prepare middleware for burst communication
                if let Some(burst_info) = input.burst_info {
                    // Get global and local ranges
                    let upper_limit = *burst_info.values().flatten().max().unwrap();
                    let lower_limit = *burst_info.values().flatten().min().unwrap();
                    let global_range = lower_limit..upper_limit + 1;
                    let local_range = burst_info.get(&input.invoker_id.unwrap()).unwrap();
                    let local_range = local_range[0]..local_range[1] + 1;

                    println!("global_range: {:?}", global_range);
                    println!("local_range: {:?}", local_range);

                    let burst_id = input.transaction_id.clone().unwrap();
                    // Initialize middleware
                    let mw = Middleware::init_global(
                        MiddlewareArguments::new(
                            input.rabbitmq.unwrap().uri,
                            global_range,
                            local_range.clone(),
                        )
                        .exchange_name(burst_id.clone())
                        .queue_prefix(format!("{}-queue", burst_id))
                        .build(),
                    )
                    .await
                    .expect("Error initializing middleware");

                    // Create threads
                    for (index, input) in inputs.into_iter().enumerate() {
                        let mut mw = mw.clone();
                        let start = local_range.start;
                        handlers.push(std::thread::spawn(move || {
                            tokio::runtime::Builder::new_multi_thread()
                                .enable_all()
                                .build()
                                .unwrap()
                                .block_on(async move {
                                    mw.init_local(start + index as u32)
                                        .await
                                        .expect("Error initializing local middleware");
                                    println!("id: {}", start + index as u32);
                                    println!("input: {:?}", input);
                                    actionMain(input, Some(mw)).await
                                })
                        }));
                    }
                // If not burst, create threads without middleware
                } else {
                    for input in inputs {
                        handlers.push(std::thread::spawn(move || {
                            tokio::runtime::Builder::new_multi_thread()
                                .enable_all()
                                .build()
                                .unwrap()
                                .block_on(actionMain(input, None))
                        }));
                    }
                }

                // new burst output have got the following format:
                // [result1, result2, ..., resultN]
                let mut results: Vec<Value> = Vec::new();
                for handle in handlers {
                    match handle.join().unwrap() {
                        Ok(result) => results.push(result),
                        Err(error) => log_error(&mut fd3, error),
                    }
                }
                let output;
                if burst {
                    output = Value::Array(results);
                } else {
                    output = results[0].clone();
                }
                writeln!(fd3, "{}\n", output).expect("Error writing on fd3");
                stdout().flush().expect("Error flushing stdout");
                stderr().flush().expect("Error flushing stderr");
            }
            Err(error) => log_error(&mut fd3, error),
        }
    }
}
