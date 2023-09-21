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

use serde_derive::Deserialize;
use serde_json::{Error, Value};
use std::{
    collections::HashMap,
    env,
    fs::File,
    thread::JoinHandle,
    io::{stderr, stdin, stdout, BufRead, Write},
    os::unix::io::FromRawFd,
};

#[derive(Debug, Clone, PartialEq, Deserialize)]
struct Input {
    value: Value,
    #[serde(flatten)]
    environment: HashMap<String, Value>,
}

fn log_error(fd3: &mut File, error: Error) {
    writeln!(fd3, "{{\"error\":\"{}\"}}\n", error).expect("Error writing on fd3");
    eprintln!("error: {}", error);
}


// new burst input have got the following format:
// {
//    "value": [{"name": "Pedro G.", "param2": "value2"}, ..., {"name": "Marc S.", "param2": "value2"}],
//    "burst_info: {invoker0: [0, 3], invoker1: [4, 13], ..., invokerN: 180, 199]}
// }
fn main() {
    let mut fd3 = unsafe { File::from_raw_fd(3) };
    let stdin = stdin();
    for line in stdin.lock().lines() {
        let buffer: String = line.expect("Error reading line");
        let parsed_input: Result<Input, Error> = serde_json::from_str(&buffer);
        match parsed_input {
            Ok(input) => {
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
                for input in inputs {
                    handlers.push(std::thread::spawn(move || { return actionMain(input); }));
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
            Err(error) => log_error(&mut fd3, error)
        }
    }
}

