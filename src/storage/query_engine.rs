// TODO: get the cluster metadata
// 1. get all the topics from it.
// 2. for each topic, read the log files from the disk
// 3. initialize the cursors to those files (all the partitions)
// 4. an api that can fetch the the messages given the topic and partition

use anyhow::Context;
use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};

use crate::storage::SharedClusterMetadata;

pub struct QueryEngine {
    topic_partition_cursors: HashMap<String, BufReader<File>>,
}

impl QueryEngine {
    pub fn init(cluster_metadata: SharedClusterMetadata) -> Result<Self> {
        let cluster_metadata = cluster_metadata
            .read()
            .expect("failed to read cluster metadata");
        let topic_partition_cursors = cluster_metadata
            .topics
            .values()
            .flat_map(|topic| {
                let topic_name = topic.topic.name.clone();
                topic.partitions.values().map(move |partition| {
                    let partition_id = partition.partition_id;
                    let key = format!("{topic_name}-{partition_id}");
                    let partition_file = File::open(format!(
                        "/tmp/kraft-combined-logs/{topic_name}-{partition_id}/00000000000000000000.log"
                    ))
                    .context("failed to open partition file")?;
                    Ok((key, BufReader::new(partition_file)))
                })
            })
            .collect::<Result<HashMap<String, BufReader<File>>>>()?;
        Ok(Self {
            topic_partition_cursors,
        })
    }

    /// Reads the raw RecordBatch bytes from disk for the given topic/partition
    /// starting at `fetch_offset`. Returns the full batch bytes (header + body)
    /// ready to be written verbatim into the Fetch response records field.
    /// Returns None if no batch with that base_offset is found.
    pub fn fetch_messages(
        &mut self,
        topic_name: &str,
        partition_id: i32,
        fetch_offset: i64,
    ) -> Result<Option<Vec<u8>>> {
        let key = format!("{topic_name}-{partition_id}");
        let cursor = self
            .topic_partition_cursors
            .get_mut(&key)
            .context("partition file not found in the topic partition cursors")?;

        // always scan from the beginning
        cursor.seek(SeekFrom::Start(0))?;

        let mut header = [0u8; 12]; // base_offset (8) + batch_length (4)
        loop {
            match cursor.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e.into()),
            }

            let base_offset = i64::from_be_bytes(header[0..8].try_into().unwrap());
            let batch_length = i32::from_be_bytes(header[8..12].try_into().unwrap());

            if batch_length < 0 {
                return Ok(None);
            }
            let batch_length = batch_length as usize;

            if base_offset == fetch_offset {
                // read the rest of the batch body
                let mut body = vec![0u8; batch_length];
                cursor.read_exact(&mut body)?;
                // return header + body as one contiguous buffer
                let mut full = Vec::with_capacity(12 + batch_length);
                full.extend_from_slice(&header);
                full.extend_from_slice(&body);
                return Ok(Some(full));
            } else {
                // skip this batch
                cursor.seek(SeekFrom::Current(batch_length as i64))?;
            }
        }
    }
}
