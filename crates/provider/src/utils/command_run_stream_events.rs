use serde_json::Value;

use crate::tura_llm::{normalize_response_content, ProviderStreamEvent, ProviderStreamEventSink};
use crate::utils::normalize_command_run_tool_input;

pub fn emit_command_run_stream_events_from_content(
    content: &Value,
    stream_events: Option<&ProviderStreamEventSink>,
) {
    let Some(sink) = stream_events else {
        return;
    };
    for (tool_call_id, commands) in command_run_commands_from_content(content) {
        for (command_index, command) in commands.into_iter().enumerate() {
            sink(ProviderStreamEvent::CommandRunCommandReady {
                tool_call_id: tool_call_id.clone(),
                command_index,
                command,
            });
        }
    }
}

fn command_run_commands_from_content(content: &Value) -> Vec<(String, Vec<Value>)> {
    let normalized;
    let tool_calls = if let Some(tool_calls) = content.get("tool_calls").and_then(Value::as_array) {
        tool_calls
    } else {
        normalized = normalize_response_content(content);
        let Some(tool_calls) = normalized.get("tool_calls").and_then(Value::as_array) else {
            return Vec::new();
        };
        tool_calls
    };
    if tool_calls.is_empty() {
        return Vec::new();
    }
    tool_calls
        .iter()
        .enumerate()
        .filter_map(|(index, call)| {
            let function = call.get("function")?;
            let name = function.get("name").and_then(Value::as_str)?;
            if name != "command_run" {
                return None;
            }
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(ToString::to_string)
                .unwrap_or_else(|| format!("call_command_run_{index}"));
            let commands = command_run_commands_from_arguments(function.get("arguments")?)?;
            Some((id, commands))
        })
        .collect()
}

fn command_run_commands_from_arguments(arguments: &Value) -> Option<Vec<Value>> {
    normalize_command_run_tool_input("command_run", arguments.clone())
        .get("commands")?
        .as_array()
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::emit_command_run_stream_events_from_content;
    use crate::tura_llm::{ProviderStreamEvent, ProviderStreamEventSink};
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    #[test]
    fn complete_provider_arguments_inherit_top_level_timeout_into_each_command() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let sink: ProviderStreamEventSink = Arc::new(move |event| {
            captured.lock().expect("event lock").push(event);
        });

        emit_command_run_stream_events_from_content(
            &json!({
                "tool_calls": [{
                    "id": "call_timeout",
                    "function": {
                        "name": "command_run",
                        "arguments": {
                            "timeout_ms": 25_000,
                            "commands": [
                                {"command_type": "shell_command", "command_line": "echo inherited"},
                                {"command_type": "shell_command", "command_line": "echo override", "timeout_ms": 30_000}
                            ]
                        }
                    }
                }]
            }),
            Some(&sink),
        );

        let events = events.lock().expect("event lock");
        let commands = events
            .iter()
            .map(|event| match event {
                ProviderStreamEvent::CommandRunCommandReady { command, .. } => command,
                other => panic!("unexpected provider event: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(commands[0]["timeout_ms"], 25_000);
        assert_eq!(commands[1]["timeout_ms"], 30_000);
    }
}
