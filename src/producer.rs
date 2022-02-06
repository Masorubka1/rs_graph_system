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
        let len_buf = 10;
        self.buf = data.to_string();
        self.prod.send(&Record::from_value(topic, data.as_bytes()));
        self.buf.clear();
        /*for i in 0..((data.len() + len_buf) / len_buf) {
            self.buf = data[i * self.buf.len()..self.buf.len()].to_string();
            self.prod.send(&Record::from_value(topic, data.as_bytes()));
            self.buf.clear();
        }*/
    }
}
