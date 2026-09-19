use std::fmt;

/// The kind of cluster-control identifier that failed validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifierKind {
    Instance,
    Partition,
    Resource,
}

impl fmt::Display for IdentifierKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Instance => "instance",
            Self::Partition => "partition",
            Self::Resource => "resource",
        })
    }
}

/// An invalid cluster-control identifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentifierError {
    Empty(IdentifierKind),
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty(kind) => write!(formatter, "{kind} identifier must not be empty"),
        }
    }
}

impl std::error::Error for IdentifierError {}

fn validate(kind: IdentifierKind, value: String) -> Result<String, IdentifierError> {
    if value.is_empty() {
        Err(IdentifierError::Empty(kind))
    } else {
        Ok(value)
    }
}

/// Stable identity of a participant in the cluster.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstanceId(String);

impl InstanceId {
    /// Construct an instance identity from its exact value.
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        Ok(Self(validate(IdentifierKind::Instance, value.into())?))
    }

    /// Return the exact instance name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for InstanceId {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for InstanceId {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for InstanceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable identity of a partition in a resource.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionId(String);

impl PartitionId {
    /// Construct a partition identity from its exact value.
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        Ok(Self(validate(IdentifierKind::Partition, value.into())?))
    }

    /// Return the exact partition name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for PartitionId {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for PartitionId {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable identity of a resource in the cluster.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceId(String);

impl ResourceId {
    /// Construct a resource identity from its exact value.
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        Ok(Self(validate(IdentifierKind::Resource, value.into())?))
    }

    /// Return the exact resource name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ResourceId {
    type Error = IdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ResourceId {
    type Error = IdentifierError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl fmt::Display for ResourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Opaque identity for one participant incarnation.
///
/// Session IDs are allocated by the participant/session model. The underlying
/// representation is intentionally not exposed as a ZooKeeper session ID or
/// any other backend coordinate.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SessionId(u64);

impl SessionId {
    pub(crate) fn from_sequence(sequence: u64) -> Self {
        Self(sequence)
    }

    pub(crate) const fn sequence(self) -> u64 {
        self.0
    }

    /// Return the opaque, backend-independent wire representation.
    ///
    /// This is intentionally a logical session token.  It is not an etcd
    /// lease id (or a ZooKeeper session id).
    pub const fn wire_value(self) -> u64 {
        self.0
    }

    /// Reconstruct a session token received from a coordination adapter.
    pub const fn from_wire_value(value: u64) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{IdentifierError, IdentifierKind, InstanceId, PartitionId, ResourceId, SessionId};

    #[test]
    fn identifiers_are_opaque_orderable_values() {
        let first = InstanceId::new("node-a").expect("valid identifier");
        let second = InstanceId::new("node-b").expect("valid identifier");
        assert!(first < second);
        assert_eq!(first.as_str(), "node-a");
        assert_eq!(
            PartitionId::try_from("partition-0").unwrap().as_str(),
            "partition-0"
        );
        assert_eq!(
            ResourceId::try_from("resource").unwrap().as_str(),
            "resource"
        );
    }

    #[test]
    fn empty_identifiers_are_rejected() {
        assert_eq!(
            InstanceId::new(""),
            Err(IdentifierError::Empty(IdentifierKind::Instance))
        );
        assert_eq!(
            PartitionId::new(""),
            Err(IdentifierError::Empty(IdentifierKind::Partition))
        );
        assert_eq!(
            ResourceId::new(""),
            Err(IdentifierError::Empty(IdentifierKind::Resource))
        );
        assert_eq!(IdentifierKind::Instance.to_string(), "instance");
        assert_eq!(IdentifierKind::Partition.to_string(), "partition");
        assert_eq!(IdentifierKind::Resource.to_string(), "resource");
        assert_eq!(
            IdentifierError::Empty(IdentifierKind::Resource).to_string(),
            "resource identifier must not be empty"
        );
        assert_eq!(
            InstanceId::try_from(String::from("node-a"))
                .unwrap()
                .to_string(),
            "node-a"
        );
        assert_eq!(
            PartitionId::try_from(String::from("p0"))
                .unwrap()
                .to_string(),
            "p0"
        );
        assert_eq!(
            ResourceId::try_from(String::from("r")).unwrap().to_string(),
            "r"
        );
    }

    #[test]
    fn session_ids_round_trip_through_the_wire_value() {
        let session = SessionId::from_wire_value(u64::MAX);
        assert_eq!(session.wire_value(), u64::MAX);
        assert_eq!(SessionId::from_wire_value(session.wire_value()), session);
        assert!(SessionId::from_wire_value(1) < session);
    }
}
