use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::{Arc, RwLock};

use crate::kraft::{self, Partition, RecordBatch, RecordValue, Topic};
use tracing::warn;

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub topic: Topic,
    pub partitions: HashMap<i32, Partition>,
}

#[derive(Debug, Clone, Default)]
pub struct ClusterMetadata {
    pub topics: HashMap<[u8; 16], TopicMetadata>,
}

pub type SharedClusterMetadata = Arc<RwLock<ClusterMetadata>>;

impl ClusterMetadata {
    pub fn from_record_batches(batches: &[RecordBatch]) -> Self {
        let mut topics: HashMap<[u8; 16], TopicMetadata> = HashMap::new();

        for batch in batches {
            for record in &batch.records {
                match &record.value {
                    RecordValue::Topic(topic) => {
                        let entry = topics
                            .entry(topic.topic_id)
                            .or_insert_with(|| TopicMetadata {
                                topic: topic.clone(),
                                partitions: HashMap::new(),
                            });
                        entry.topic = topic.clone();
                    }
                    RecordValue::Partition(partition) => {
                        let entry =
                            topics
                                .entry(partition.topic_id)
                                .or_insert_with(|| TopicMetadata {
                                    topic: Topic {
                                        version: 0,
                                        name: String::new(),
                                        topic_id: partition.topic_id,
                                        tag_buffer: vec![],
                                    },
                                    partitions: HashMap::new(),
                                });
                        entry
                            .partitions
                            .insert(partition.partition_id, partition.clone());
                    }
                    _ => {}
                }
            }
        }

        Self { topics }
    }

    pub fn load() -> anyhow::Result<Self> {
        match kraft::load_metadata_image() {
            Ok(batches) => Ok(Self::from_record_batches(&batches)),
            Err(err) if is_metadata_file_missing(&err) => {
                warn!(error = ?err, "metadata log file not found, starting with empty cluster metadata");
                Ok(Self::default())
            }
            Err(err) => Err(err),
        }
    }

    pub fn load_shared() -> anyhow::Result<SharedClusterMetadata> {
        let metadata = Self::load()?;
        Ok(Arc::new(RwLock::new(metadata)))
    }
}

fn is_metadata_file_missing(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| io_err.kind() == ErrorKind::NotFound)
    })
}
