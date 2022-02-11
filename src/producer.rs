use kafka::producer::{Producer, Record, RequiredAcks};
use std::time::Duration;

pub struct Prod {
    buf: String,
    prod: Producer,
}

impl Prod {
    pub fn new() -> Prod {
        let var = Producer::from_hosts(vec!["localhost:9092".to_owned()])
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();
        let buf = String::with_capacity(10);
        Prod { buf, prod: var }
    }
    pub fn writ(&mut self, topic: &str, data: &str) {
        self.buf = data.to_string();
        self.prod.send(&Record::from_value(topic, data.as_bytes()));
        self.buf.clear();
    }
}
