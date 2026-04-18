//! Log events published on the event stream. akka.net: `Event/Logging.cs`.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Error,
    Warning,
    Info,
    Debug,
}

#[derive(Debug, Clone)]
pub struct LogEvent {
    pub level: LogLevel,
    pub source: String,
    pub message: String,
}

impl LogEvent {
    pub fn new(level: LogLevel, source: impl Into<String>, message: impl Into<String>) -> Self {
        Self { level, source: source.into(), message: message.into() }
    }

    pub fn emit(&self) {
        match self.level {
            LogLevel::Error => tracing::error!(source = %self.source, "{}", self.message),
            LogLevel::Warning => tracing::warn!(source = %self.source, "{}", self.message),
            LogLevel::Info => tracing::info!(source = %self.source, "{}", self.message),
            LogLevel::Debug => tracing::debug!(source = %self.source, "{}", self.message),
        }
    }
}
