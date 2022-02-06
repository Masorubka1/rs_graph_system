use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

pub struct Cons {
    pub cons: Consumer,
}

impl Cons {
    pub fn new(topic: &str) -> Cons {
        let consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
            .with_topic_partitions(topic.to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_group("my-group".to_owned())
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()
            .unwrap();
        Cons { cons: consumer }
    }
}
