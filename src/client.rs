use crate::protocol::{MAX_FRAME_BYTES, Request, Response, VERSION, read_frame};
use crate::{AppError, Result};
use serde_json::Value;
use std::path::{Path, PathBuf};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::UnixStream,
};

/// A stable client identity; each request may reconnect without replaying work.
#[derive(Clone, Debug)]
pub struct Client {
    socket: PathBuf,
    client_id: String,
    server_id: String,
}

impl Client {
    pub async fn connect(socket: impl AsRef<Path>) -> Result<Self> {
        let mut client = Self {
            socket: socket.as_ref().into(),
            client_id: uuid::Uuid::new_v4().to_string(),
            server_id: String::new(),
        };
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.request(
                "system.hello",
                serde_json::json!({}),
                uuid::Uuid::new_v4().to_string(),
            ),
        )
        .await
        .map_err(|_| {
            AppError::new(
                "handshake_timeout",
                "server did not complete its handshake within two seconds",
            )
        })??;
        client.server_id = response.server_id.clone();
        let hello = response.into_result()?;
        if hello["protocol_version"] != VERSION || hello["server_id"] != client.server_id {
            return Err(AppError::new(
                "protocol_error",
                "server handshake identity/version mismatch",
            ));
        }
        if hello["app_version"] != env!("CARGO_PKG_VERSION")
            || hello["core_version"] != ontography::VERSION
            || hello["app_build"] != crate::APP_BUILD
            || hello["core_build"] != crate::CORE_BUILD
        {
            return Err(AppError::new("incompatible_server", "the existing server uses a different app/core build; use its matching client or explicitly stop it before upgrading")
                .details(serde_json::json!({"server_id":client.server_id,"expected_app":env!("CARGO_PKG_VERSION"),"actual_app":hello["app_version"],"expected_core":ontography::VERSION,"actual_core":hello["core_version"],"expected_app_build":crate::APP_BUILD,"actual_app_build":hello["app_build"],"expected_core_build":crate::CORE_BUILD,"actual_core_build":hello["core_build"]})));
        }
        Ok(client)
    }

    pub fn server_id(&self) -> &str {
        &self.server_id
    }
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub async fn call(&self, operation: &str, args: Value) -> Result<Value> {
        let request_id = uuid::Uuid::new_v4().to_string();
        self.request(operation, args, request_id)
            .await?
            .into_result()
    }

    pub async fn request(
        &self,
        operation: &str,
        args: Value,
        request_id: String,
    ) -> Result<Response> {
        let request = Request {
            version: VERSION,
            client_id: self.client_id.clone(),
            request_id: request_id.clone(),
            operation: operation.into(),
            expected_server_id: if self.server_id.is_empty() {
                None
            } else {
                Some(self.server_id.clone())
            },
            args,
        };
        request.validate()?;
        // Validate/encode before opening the connection: these failures cannot dispatch work.
        let mut frame = serde_json::to_vec(&request)?;
        if frame.len() + 1 > MAX_FRAME_BYTES {
            return Err(AppError::new(
                "request_too_large",
                "use a bounded request or import content from a file",
            ));
        }
        frame.push(b'\n');
        let mut stream = UnixStream::connect(&self.socket).await?;
        let uncertain = |cause: AppError| {
            if operation == "system.hello" {
                return cause;
            }
            AppError::new("unknown_outcome", format!("{operation} may have committed: {}. Inspect operation.get with client_id={} and request_id={} on server {} before retrying",cause.message,self.client_id,request_id,self.server_id))
                .details(serde_json::json!({"client_id":self.client_id,"request_id":request_id,"server_id":self.server_id,"operation":operation,"cause":cause}))
        };
        stream
            .write_all(&frame)
            .await
            .map_err(|error| uncertain(error.into()))?;
        stream
            .flush()
            .await
            .map_err(|error| uncertain(error.into()))?;
        let bytes = read_frame(&mut BufReader::new(stream))
            .await
            .map_err(&uncertain)?
            .ok_or_else(|| {
                uncertain(AppError::new(
                    "disconnected",
                    "server disconnected before returning a result",
                ))
            })?;
        let response: Response = serde_json::from_slice(&bytes)
            .map_err(|error| uncertain(AppError::new("protocol_error", error.to_string())))?;
        if response.version != VERSION || response.request_id != request_id {
            return Err(uncertain(AppError::new(
                "protocol_error",
                "response identity/version mismatch",
            )));
        }
        if !self.server_id.is_empty() && response.server_id != self.server_id {
            return Err(AppError::new(
                "server_restarted",
                "connect again and refresh run/resource state",
            ));
        }
        Ok(response)
    }
}
