use crate::environment::Environment;
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
    app_session_id: Option<String>,
    environment: Option<Environment>,
}

impl Client {
    pub async fn connect(socket: impl AsRef<Path>) -> Result<Self> {
        let (client, hello) = Self::handshake(socket).await?;
        if hello["app_version"] != env!("CARGO_PKG_VERSION")
            || hello["core_version"] != ontography::VERSION
            || hello["app_build"] != crate::APP_BUILD
            || hello["core_build"] != crate::CORE_BUILD
        {
            // Servers before idle reporting never say they are idle.
            let idle = hello["idle"] == true;
            let message = if idle {
                "the running server is an idle one from another build"
            } else {
                "the running server is from another build and may have sessions running; run `ontography server stop` with the same --data-dir, which suspends them, then retry"
            };
            return Err(AppError::new("incompatible_server", message)
                .details(serde_json::json!({"server_id":client.server_id,"idle":idle,"expected_app":env!("CARGO_PKG_VERSION"),"actual_app":hello["app_version"],"expected_core":ontography::VERSION,"actual_core":hello["core_version"],"expected_app_build":crate::APP_BUILD,"actual_app_build":hello["app_build"],"expected_core_build":crate::CORE_BUILD,"actual_core_build":hello["core_build"]})));
        }
        Ok(client)
    }

    /// Explicit administrative shutdown needs compatible framing and server
    /// identity, but no app/core data compatibility. Never expose this connection
    /// to callers for arbitrary operations or use it during automatic startup.
    pub async fn stop_server(socket: impl AsRef<Path>) -> Result<Value> {
        let (client, _) = Self::handshake(socket).await?;
        client.call("server.stop", serde_json::json!({})).await
    }

    async fn handshake(socket: impl AsRef<Path>) -> Result<(Self, Value)> {
        let mut client = Self {
            socket: socket.as_ref().into(),
            client_id: uuid::Uuid::new_v4().to_string(),
            server_id: String::new(),
            app_session_id: None,
            environment: None,
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
        if client.server_id.is_empty()
            || hello["protocol_version"] != VERSION
            || hello["server_id"] != client.server_id
        {
            return Err(AppError::new(
                "protocol_error",
                "server handshake identity/version mismatch",
            ));
        }
        Ok((client, hello))
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

    pub fn for_session(&self, session_id: &str) -> Self {
        Self {
            app_session_id: Some(session_id.into()),
            ..self.clone()
        }
    }

    /// A client whose requests carry `environment`: the programs of a session
    /// it activates start with it.
    pub fn with_environment(&self, environment: Environment) -> Self {
        Self {
            environment: Some(environment),
            ..self.clone()
        }
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
            app_session_id: self.app_session_id.clone(),
            expected_server_id: if self.server_id.is_empty() {
                None
            } else {
                Some(self.server_id.clone())
            },
            args,
            environment: self
                .environment
                .as_ref()
                .map(|environment| environment.vars().clone()),
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
